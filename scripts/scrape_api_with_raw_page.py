import requests
import json
import time
import re
import asyncio
from contextlib import suppress

# --- Playwright / scraping deps ---
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# --------------------------
# Helpers for scraping page
# --------------------------

def _slug_from_name(name: str) -> str:
    return re.sub(r"\s+", "%20", (name or "").strip())

async def _accept_cookies(page):
    cookie_selectors = [
        "button:has-text('Accept all cookies')",
        "button:has-text('Allow all cookies')",
        "button:has-text('Accept all')",
        "#onetrust-accept-btn-handler",
        "[data-accept-type='all']",
        "button:has-text('Accept')"
    ]
    for sel in cookie_selectors:
        with suppress(Exception):
            print(f"🔎 Trying cookie selector: {sel}")
            await page.click(sel, timeout=2500)
            await page.wait_for_timeout(1000)
            return True

    with suppress(Exception):
        res = await page.evaluate("""
            () => {
                try {
                    if (window.Cookiebot && Cookiebot.submitCustomConsent) {
                        Cookiebot.submitCustomConsent(true, true, true);
                        return 'cookiebot-submit';
                    }
                } catch (e) {}
                return null;
            }
        """)
        if res:
            print("✅ Cookiebot consent accepted")
            await page.wait_for_timeout(1000)
            return True

    print("⚠️ No cookie banner found / accepted")
    return False

def _find_h2_ci(soup: BeautifulSoup, text: str):
    want = (text or "").strip().lower()
    for h2 in soup.find_all("h2"):
        if (h2.get_text(strip=True) or "").strip().lower() == want:
            return h2
    return None

def _extract_section_paragraphs(soup: BeautifulSoup, h2_text: str):
    h2 = _find_h2_ci(soup, h2_text)
    if not h2:
        return None
    prose_div = h2.find_next("div", class_=lambda c: c and "prose" in c)
    if not prose_div:
        prose_div = h2.find_next("div")
    if not prose_div:
        return None
    ps = prose_div.find_all("p")
    if not ps:
        return None
    return "\n".join(p.get_text(" ", strip=True) for p in ps)

def _extract_course_locations(soup: BeautifulSoup):
    locations = []
    h2 = _find_h2_ci(soup, "Course locations")
    if not h2:
        return locations
    grid = h2.find_next("div", class_=lambda c: c and "content-grid" in c)
    if not grid:
        grid = h2.find_next("div")
    if not grid:
        return locations
    for art in grid.find_all("article"):
        title = None
        addr = None
        h2_tag = art.find("h2")
        if h2_tag:
            title = h2_tag.get_text(strip=True)
        addr_div = art.find("div")
        if addr_div:
            addr = addr_div.get_text(" ", strip=True)
        if title or addr:
            locations.append({"title": title, "address": addr})
    return locations

def _extract_video_src(soup: BeautifulSoup):
    iframe = (
        soup.select_one("iframe.video-player") or
        soup.select_one("iframe[data-cookieconsent='marketing']") or
        soup.find("iframe")
    )
    if iframe:
        return iframe.get("src")
    return None

async def scrape_uni_page(provider_id: str, provider_name: str) -> dict:
    slug_name = _slug_from_name(provider_name)
    url = f"https://www.ucas.com/explore/unis/{provider_id}/{slug_name}?studyYear=current"
    print(f"🌍 Scraping UCAS page for {provider_name} ({provider_id})")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded")
        await _accept_cookies(page)

        # Try explicitly waiting for video iframe
        with suppress(Exception):
            await page.wait_for_selector("iframe.video-player, iframe[data-cookieconsent='marketing']", timeout=5000)
            print(f"🎥 Video iframe appeared for {provider_name}")

        await page.wait_for_timeout(2000)
        html = await page.content()
        await browser.close()

    soup = BeautifulSoup(html, "html.parser")

    about_text = _extract_section_paragraphs(soup, "About us")
    diff_text = _extract_section_paragraphs(soup, "What makes us different")
    locations = _extract_course_locations(soup)
    video_url = _extract_video_src(soup)

    print(f"📹 Extracted video URL: {video_url} for {provider_name}")

    return {
        "aboutUs": about_text,
        "whatMakesUsDifferent": diff_text,
        "courseLocations": locations,
        "video": video_url
    }

# --------------------------
# Main unchanged
# --------------------------
def main():
    with open("../data/provider_ids.json", "r", encoding="utf-8") as f:
        ids_data = json.load(f)
    provider_ids = ids_data.get("ids", [])

    base_url = (
        "https://services.ucas.com/search/api/v2/providers/search/{}/"
        "?fields=provider("
        "id,name,logoUrl,websiteUrl,institutionCode,"
        "address(line4,country(mappedCaption)),"
        "aliasName,aliases,providerSort,"
        "courses(id,academicYearId,applicationCode,courseTitle,"
        "routingData(destination(caption)),"
        "options(id,outcomeQualification(caption),duration,durationRange(min,max),studyMode,startDate,location(name))))"
    )

    payload = {
        "searchTerm": "",
        "filters": {
            "academicYearId": "2025",
            "destinations": ["Undergraduate", "Postgraduate"],
            "providers": [],
            "schemes": [],
            "ucasTeacherTrainingProvider": False,
            "degreeApprenticeship": False,
            "studyTypes": [],
            "subjects": [],
            "qualifications": [],
            "attendanceTypes": [],
            "acceleratedDegrees": False,
            "entryPoint": None,
            "regions": [],
            "vacancy": "",
            "startDates": [],
            "higherTechnicalQualifications": False
        },
        "options": {
            "viewType": "provider",
            "paging": {"loadFrom": 1, "pageSize": 600}
        }
    }

    headers = {"Content-Type": "application/json"}
    all_providers_data = {}

    for pid in provider_ids:
        url = base_url.format(pid)
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
        except Exception as e:
            print(f"❌ Network error for {pid}: {e}")
            time.sleep(0.5)
            continue

        if response.status_code == 200:
            data = response.json()
            provider = data.get("provider", {})
            if provider:
                provider["backgroundUrl"] = f"https://www.ucas.com/provider-images/files/styles/tiles/{pid}.jpg"
                all_providers_data[pid] = provider
            print(f"✅ API fetched for provider {pid}")
        else:
            print(f"❌ API failed for {pid}: {response.status_code}")
        time.sleep(0.5)

    async def scrape_all():
        sem = asyncio.Semaphore(3)
        async def scrape_one(pid, prov):
            name = prov.get("name") or prov.get("aliasName") or ""
            if not name:
                return pid, None
            async with sem:
                try:
                    data = await scrape_uni_page(pid, name)
                    return pid, data
                except Exception as e:
                    print(f"⚠️ Scrape failed for {pid}: {e}")
                    return pid, None
        tasks = [scrape_one(pid, prov) for pid, prov in all_providers_data.items()]
        results = await asyncio.gather(*tasks)
        return {pid: scraped for pid, scraped in results if scraped is not None}

    try:
        scraped_map = asyncio.run(scrape_all())
    except RuntimeError:
        import nest_asyncio
        nest_asyncio.apply()
        loop = asyncio.get_event_loop()
        scraped_map = loop.run_until_complete(scrape_all())

    for pid, scraped in scraped_map.items():
        prov = all_providers_data.get(pid)
        if not prov:
            continue
        prov["aboutUs"] = scraped.get("aboutUs")
        prov["whatMakesUsDifferent"] = scraped.get("whatMakesUsDifferent")
        prov["courseLocations"] = scraped.get("courseLocations", [])
        prov["video"] = scraped.get("video")

    with open("../data/providers_with_courses.json", "w", encoding="utf-8") as f:
        json.dump(all_providers_data, f, indent=4, ensure_ascii=False)

    print(f"🎉 Finished! Data for {len(all_providers_data)} providers saved in providers_with_courses.json")

if __name__ == "__main__":
    main()
