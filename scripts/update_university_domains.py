"""Standalone script to enrich Firestore university docs with email domains.

Usage examples:
    python update_university_domains.py
    python update_university_domains.py --force --limit 100
    python update_university_domains.py --dry-run

Environment variables:
    OPENAI_API_KEY  Required (OpenAI is the sole data source for domains).
"""

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

# Add parent directory to path so we can import config module
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from firebase_admin import firestore  # type: ignore
except ModuleNotFoundError:
    print(
        "Missing required package. Install dependencies with:\n"
        "    pip install -r requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)

# Initialise Firebase app (expects config/firebaseConfig.py to call initialize_app)
try:
    from config.firebaseConfig import *  # noqa: F401,F403 - side effects only
except ModuleNotFoundError as exc:
    print(
        "Firebase configuration not found. Ensure config/firebaseConfig.py exists "
        "and correctly initialises firebase_admin.",
        file=sys.stderr,
    )
    raise exc

try:
    import openai
    OpenAI = getattr(openai, 'OpenAI', None)
    OpenAIAPIError = getattr(openai, 'APIError', Exception)
    if OpenAI is None:
        # Fallback for older openai versions
        openai.api_key = os.getenv("OPENAI_API_KEY")
        OpenAI = openai  # type: ignore[assignment]
except ModuleNotFoundError:
    OpenAI = None  # type: ignore[assignment]
    OpenAIAPIError = Exception  # type: ignore[assignment]

DOMAIN_RE = re.compile(r"^(?!-)(?:[a-z0-9-]{1,63}\.)+[a-z]{2,63}$")
DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
LOGGER = logging.getLogger("update_university_domains")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def extract_domain_from_url(raw_url: Optional[str]) -> Optional[str]:
    if not raw_url:
        return None
    parsed = urlparse(raw_url.strip())
    hostname = parsed.netloc or parsed.path
    hostname = hostname.split("/")[0].split(":")[0]
    if hostname.lower().startswith("www."):
        hostname = hostname[4:]
    hostname = hostname.lower().strip()
    if DOMAIN_RE.match(hostname):
        return hostname
    return None


def normalise_domain(raw_domain: str) -> Optional[str]:
    candidate = raw_domain.strip().lower()
    if candidate.startswith("http://") or candidate.startswith("https://"):
        return extract_domain_from_url(candidate)
    if candidate.startswith("www."):
        candidate = candidate[4:]
    candidate = candidate.strip()
    if "@" in candidate:
        candidate = candidate.split("@", 1)[-1]
    candidate = candidate.strip("./ ")
    if DOMAIN_RE.match(candidate):
        return candidate
    return None


def build_openai_client() -> Optional[OpenAI]:  # type: ignore[valid-type]
    if OpenAI is None:
        LOGGER.warning("openai package not installed; OpenAI lookups will be skipped.")
        return None

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        LOGGER.warning("OPENAI_API_KEY not set; OpenAI lookups will be skipped.")
        return None

    # For newer openai versions, return a client instance
    if hasattr(OpenAI, '__call__'):
        try:
            return OpenAI(api_key=api_key)
        except TypeError:
            # Fallback: older versions use module-level api_key
            return OpenAI  # type: ignore[return-value]
    return OpenAI  # type: ignore[return-value]


def suggest_domain_with_openai(client: OpenAI, university_name: str, model: str, max_retries: int = 3) -> Optional[str]:
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            # Try newer API first (v1.0+)
            if hasattr(client, 'chat') and hasattr(client.chat, 'completions'):
                response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You answer with the single primary email domain used by the specified university. "
                                "Respond with domain only, no extra text."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                "What is the primary email domain used for institutional email addresses at "
                                f"{university_name}?"
                            ),
                        },
                    ],
                    max_tokens=25,
                )
                text = response.choices[0].message.content.strip()
            else:
                # Fallback for older API
                response = client.ChatCompletion.create(
                    model=model,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You answer with the single primary email domain used by the specified university. "
                                "Respond with domain only, no extra text."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                "What is the primary email domain used for institutional email addresses at "
                                f"{university_name}?"
                            ),
                        },
                    ],
                    max_tokens=25,
                )
                text = response['choices'][0]['message']['content'].strip()

            domain = normalise_domain(text) if text else None
            if domain:
                return domain
            LOGGER.warning("OpenAI returned un-parseable response for '%s': %s", university_name, text)
            return None
        except OpenAIAPIError as exc:  # type: ignore[arg-type]
            LOGGER.warning(
                "OpenAI API error for '%s' (attempt %s/%s): %s",
                university_name,
                attempt,
                max_retries,
                exc,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Unexpected error from OpenAI for '%s': %s", university_name, exc)

        time.sleep(delay)
        delay *= 2

    return None


def update_document(
    doc_ref: firestore.DocumentReference,  # type: ignore[name-defined]
    data: dict,
    *,
    force: bool,
    dry_run: bool,
    openai_client: Optional[OpenAI],  # type: ignore[valid-type]
    model: str,
    sleep: float,
) -> str:
    doc_id = doc_ref.id
    existing_domain = data.get("university_domain")
    name = data.get("university_name") or data.get("name")

    if existing_domain and not force:
        LOGGER.debug("Skipping %s: domain already present (%s)", doc_id, existing_domain)
        return "skipped_existing"

    if openai_client is None:
        LOGGER.warning("OpenAI client unavailable; skipping %s", doc_id)
        return "openai_unavailable"

    if not name:
        LOGGER.warning("Missing university_name for document %s", doc_id)
        return "missing_name"

    domain = suggest_domain_with_openai(openai_client, name, model)

    if not domain:
        LOGGER.warning("Could not determine domain for %s (%s)", doc_id, name)
        return "missing"

    update_payload = {
        "university_domain": domain,
        "university_domain_source": "openai",
        "university_domain_updated_at": firestore.SERVER_TIMESTAMP,
    }

    if dry_run:
        LOGGER.info("[Dry Run] Would update %s with domain %s (source=openai)", doc_id, domain)
    else:
        doc_ref.update(update_payload)
        LOGGER.info("Updated %s with domain %s (source=openai)", doc_id, domain)

    if sleep:
        time.sleep(sleep)

    return "openai"


def process_universities(
    collection: str,
    *,
    limit: Optional[int],
    force: bool,
    dry_run: bool,
    sleep: float,
    model: str,
) -> None:
    db = firestore.client()
    openai_client = build_openai_client()

    docs_iter = db.collection(collection).order_by("university_name").stream()

    processed = 0
    updated = 0
    skipped = 0
    missing = 0
    processed_to_update = 0  # Count only docs that need updating

    for doc in docs_iter:
        data = doc.to_dict() or {}
        existing_domain = data.get("university_domain")
        
        # Skip if already has domain and not forcing
        if existing_domain and not force:
            skipped += 1
            continue
        
        # Only count towards limit if this doc needs updating
        processed_to_update += 1
        
        status = update_document(
            doc.reference,
            data,
            force=force,
            dry_run=dry_run,
            openai_client=openai_client,
            model=model,
            sleep=sleep,
        )
        processed += 1
        if status in {"official_website", "openai", "updated"}:
            updated += 1
        elif status == "skipped_existing":
            skipped += 1
        else:
            missing += 1

        if limit and processed_to_update >= limit:
            LOGGER.info("Reached limit of %s documents to update. Stopping.", limit)
            break

    LOGGER.info(
        "Finished processing. processed=%s updated=%s skipped=%s missing=%s",
        processed,
        updated,
        skipped,
        missing,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate the university_domain field for Firestore university documents.",
    )
    parser.add_argument(
        "--collection",
        default="universities",
        help="Firestore collection to target (default: universities)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of documents to process.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing university_domain values.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write changes back to Firestore.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep between document updates (useful to avoid rate limits).",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_OPENAI_MODEL,
        help="OpenAI model to use when inferring domains (default: gpt-4o-mini).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(verbose=args.verbose)

    LOGGER.info(
        "Starting domain enrichment (collection=%s, limit=%s, force=%s, dry_run=%s)",
        args.collection,
        args.limit,
        args.force,
        args.dry_run,
    )

    process_universities(
        args.collection,
        limit=args.limit,
        force=args.force,
        dry_run=args.dry_run,
        sleep=args.sleep,
        model=args.model,
    )


if __name__ == "__main__":
    main()
