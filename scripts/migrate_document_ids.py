"""Migrate Firestore documents by renaming their IDs to match the universityId field.

This script reads all documents from a collection, extracts the universityId field,
and migrates each document to use that ID instead of the current document ID.

Usage examples:
    python migrate_document_ids.py
    python migrate_document_ids.py --dry-run
    python migrate_document_ids.py --limit 50
    python migrate_document_ids.py --collection universities

Environment variables:
    None required (uses existing Firebase config).
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional

try:
    from firebase_admin import firestore  # type: ignore
except ModuleNotFoundError:
    print(
        "Missing required package. Install dependencies with:\n"
        "    pip install -r requirements.txt",
        file=sys.stderr,
    )
    sys.exit(1)

# Add parent directory to path so we can import config module
sys.path.insert(0, str(Path(__file__).parent.parent))

# Initialise Firebase app
try:
    from config.firebaseConfig import *  # noqa: F401,F403 - side effects only
except ModuleNotFoundError as exc:
    print(
        "Firebase configuration not found. Ensure config/firebaseConfig.py exists "
        "and correctly initialises firebase_admin.",
        file=sys.stderr,
    )
    raise exc

LOGGER = logging.getLogger("migrate_document_ids")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def migrate_document(
    doc_ref: firestore.DocumentReference,  # type: ignore[name-defined]
    data: dict,
    *,
    dry_run: bool,
    sleep: float,
    max_retries: int = 3,
) -> str:
    """Migrate a single document to use universityId as its ID.
    
    Returns status: 'migrated', 'missing_id', 'already_correct', or 'error'.
    """
    old_id = doc_ref.id
    university_id = data.get("universityId")
    
    if not university_id:
        LOGGER.warning("Document %s has no universityId field. Skipping.", old_id)
        return "missing_id"
    
    if old_id == university_id:
        LOGGER.debug("Document %s already has correct ID. Skipping.", old_id)
        return "already_correct"
    
    if dry_run:
        LOGGER.info("[Dry Run] Would migrate %s -> %s", old_id, university_id)
        return "migrated"
    
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            db = firestore.client()
            # Extract collection name from the document reference path
            # _path is a tuple like ('universities', 'doc_id')
            path_parts = doc_ref._path
            if isinstance(path_parts, tuple):
                collection_name = path_parts[0]
            else:
                collection_name = path_parts.split("/")[-2]
            
            # Create new document with universityId as ID
            new_doc_ref = db.collection(collection_name).document(university_id)
            new_doc_ref.set(data)
            
            # Delete old document
            doc_ref.delete()
            
            LOGGER.info("Migrated %s -> %s", old_id, university_id)
            
            if sleep:
                time.sleep(sleep)
            
            return "migrated"
        
        except Exception as exc:  # noqa: BLE001
            if attempt < max_retries:
                LOGGER.warning(
                    "Error migrating %s (attempt %s/%s): %s. Retrying in %.1fs...",
                    old_id,
                    attempt,
                    max_retries,
                    exc,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
            else:
                LOGGER.exception("Error migrating document %s after %s attempts: %s", old_id, max_retries, exc)
                return "error"


def migrate_documents(
    collection: str,
    *,
    limit: Optional[int],
    dry_run: bool,
    sleep: float,
) -> None:
    """Migrate all documents in a collection to use universityId as their ID.
    
    Processes documents in batches to avoid Firestore query timeouts.
    """
    db = firestore.client()
    
    total_processed = 0
    total_migrated = 0
    total_already_correct = 0
    total_missing_id = 0
    total_errors = 0
    
    batch_size = 50
    last_doc = None
    
    while True:
        LOGGER.info("Fetching batch of %s documents (starting after %s)...", batch_size, last_doc.id if last_doc else "beginning")
        
        # Build query for this batch
        query = db.collection(collection).order_by("__name__")
        if last_doc:
            query = query.start_after(last_doc)
        
        docs_batch = list(query.limit(batch_size).stream())
        
        if not docs_batch:
            LOGGER.info("No more documents to process.")
            break
        
        LOGGER.info("Processing batch of %s documents...", len(docs_batch))
        
        for doc in docs_batch:
            data = doc.to_dict() or {}
            status = migrate_document(
                doc.reference,
                data,
                dry_run=dry_run,
                sleep=sleep,
                max_retries=3,
            )
            
            total_processed += 1
            if status == "migrated":
                total_migrated += 1
            elif status == "already_correct":
                total_already_correct += 1
            elif status == "missing_id":
                total_missing_id += 1
            else:
                total_errors += 1
        
        last_doc = docs_batch[-1]
        
        # Small pause between batches to avoid rate limits
        time.sleep(2.0)
    
    LOGGER.info(
        "Finished migration. processed=%s migrated=%s already_correct=%s missing_id=%s errors=%s",
        total_processed,
        total_migrated,
        total_already_correct,
        total_missing_id,
        total_errors,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate Firestore documents to use universityId as their document ID.",
    )
    parser.add_argument(
        "--collection",
        default="universities",
        help="Firestore collection to target (default: universities)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write changes back to Firestore.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Seconds to sleep between document migrations (default: 1.0, respects Firebase rate limits).",
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
        "Starting document ID migration (collection=%s, dry_run=%s, sleep=%.1fs)",
        args.collection,
        args.dry_run,
        args.sleep,
    )
    LOGGER.info("Processing all documents with retry logic (max 3 attempts per document)...")
    
    migrate_documents(
        args.collection,
        limit=None,
        dry_run=args.dry_run,
        sleep=args.sleep,
    )


if __name__ == "__main__":
    main()
