#!/usr/bin/env python3
"""
Build countries.json from booking_hierarchy.json

Rules
- Do NOT scrape regions. Read region URLs from booking_hierarchy.json only.
- On each region page, resolve the country from breadcrumb at: nav > ol > li:nth-of-type(3)
  • If that li has a link, that href is the country URL
  • If it is plain text, the current page URL is the country URL
- Save countries.json with:
  {
    "countries": [
      {
        "name": "...",
        "url": "...",
        "cities": [
          { "name": "...", "url": "...", "about": "...", "image": "..." }
        ],
        "popular_regions": [
          { "name": "...", "url": "...", "image": "..." }
        ]
      }
    ]
  }

Performance
- Thread heavy by default. No artificial sleeps or rate limits.
- HTTP adapter pool scaled to concurrency.
"""

import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

BASE_URL = "https://www.booking.com"
REGIONS_SOURCE = "booking_hierarchy.json"
COUNTRIES_OUT = "countries.json"

# ---------- HTTP ----------

def build_session(pool: int) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    # No retries or backoff to avoid blocking
    adapter = HTTPAdapter(pool_connections=max(256, pool * 4), pool_maxsize=max(256, pool * 4))
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def get_html(s: requests.Session, url: str, timeout: float = 25.0) -> str:
    r = s.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text

# ---------- IO ----------

def load_regions(path: str = REGIONS_SOURCE) -> List[Dict]:
    if not os.path.exists(path):
        print(f"Missing {path}", file=sys.stderr)
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    regions: List[Dict] = []
    if isinstance(data, dict) and isinstance(data.get("regions"), list):
        it = data["regions"]
    elif isinstance(data, list):
        it = data
    else:
        it = []

    for r in it:
        if not isinstance(r, dict):
            continue
        name = r.get("name") or r.get("region_name")
        url = r.get("url")
        if name and url:
            regions.append({"name": str(name), "url": str(url)})

    if not regions:
        print("No regions in booking_hierarchy.json", file=sys.stderr)
        sys.exit(1)
    return regions


def save_countries(data: Dict, path: str = COUNTRIES_OUT) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# ---------- Parsing helpers ----------

def _text_of(el) -> str:
    return el.get_text(" ", strip=True) if el else ""


def _img_src(img_el) -> str:
    if not img_el:
        return ""
    for attr in ("src", "data-src", "data-lazy", "data-lazy-src", "data-original"):
        v = img_el.get(attr)
        if v:
            return v.strip()
    return ""

# ---------- Parsers ----------

def parse_country_from_breadcrumb(html: str, page_url: str) -> Tuple[str, str]:
    """
    Use nav > ol > li:nth-of-type(3) exactly.
    If it has a link use that href, else use page_url.
    Returns (name, url)
    """
    soup = BeautifulSoup(html, "lxml")
    li = soup.select_one("nav ol li:nth-of-type(3)")
    if li:
        a = li.find("a", href=True)
        if a:
            name = a.get_text(strip=True)
            href = a["href"].strip()
            return name, urljoin(BASE_URL, href)
        else:
            return li.get_text(" ", strip=True), page_url
    return "", page_url


def parse_cities_from_country(html: str) -> List[Dict[str, str]]:
    """
    Extract city cards from a country page.

    Source patterns:
      - Prefer container [data-test-id="top-cities"]
      - Fallback to aria-label containing either:
          "Top destinations for" or "Check out these popular cities in"

    Card fields:
      - name  = .bui-card__content h3.bui-card__title
      - about = .bui-card__content h4.bui-card__subtitle
      - image = .bui-card__image-container img  (src or data-* variants)
      - url   = first anchor href inside the card

    Returns a list of { name, url, about, image } with dedupe by (url, name).
    """
    soup = BeautifulSoup(html, "lxml")

    # Flexible label match for fallback
    label_re = re.compile(
        r"(?:top\s*destinations\s*for|check\s*out\s*these\s*popular\s*cities\s*in)",
        re.I,
    )

    # Prefer Booking's stable test id, else fall back to aria label
    container = soup.select_one('[data-test-id="top-cities"]')
    blocks = [container] if container else soup.find_all(attrs={"aria-label": label_re})

    results: List[Dict[str, str]] = []

    for block in blocks:
        if not block:
            continue

        # Always end up at an anchor for stable href extraction
        anchors = block.select('.bui-carousel__item a[href], a.bui-card[href], .bui-card a[href]')
        if not anchors:
            anchors = block.select('a[href]')

        for a in anchors:
            href = a.get("href", "").strip()
            url = urljoin(BASE_URL, href) if href else ""

            # Image: prefer the card image, fall back to any img under anchor
            img_el = a.select_one(".bui-card__image-container img") or a.find("img")
            image = _img_src(img_el)

            # Content: semantic headings inside the card
            content = a.select_one(".bui-card__content")
            name_el = content.select_one("h3.bui-card__title") if content else None
            about_el = content.select_one("h4.bui-card__subtitle") if content else None

            name = _text_of(name_el) or _text_of(content)
            about = _text_of(about_el)

            if not name:
                continue

            results.append({
                "name": name,
                "url": url,
                "about": about,
                "image": image,
            })

    # Dedupe by (url, name) preserving order
    seen = set()
    unique: List[Dict[str, str]] = []
    for item in results:
        key = (item.get("url", ""), item.get("name", ""))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)

    return unique



def parse_popular_regions(html: str) -> List[Dict[str, str]]:
    """
    Find section with heading text containing "Hotels in the most popular regions in"
    Then parse cards similar to city cards. Save: name, image, url.
    """
    soup = BeautifulSoup(html, "lxml")
    title_re = re.compile(r"Hotels\s+in\s+the\s+most\s+popular\s+regions\s+in\b", re.I)

    regions: List[Dict[str, str]] = []

    # Look for headings h2 or h3 containing the title
    for h in soup.find_all(["h2", "h3"]):
        heading_txt = h.get_text(" ", strip=True) or ""
        if not title_re.search(heading_txt):
            continue

        # Walk forward from the heading to the actual carousel or grid container
        # Prefer the test id when present
        container = soup.select_one('[data-test-id="top-regions"]')

        # Fallback — find the heading then walk forward to the real carousel container
        if not container:
            title_re = re.compile(r"Hotels\s+in\s+the\s+most\s+popular\s+regions\s+in", re.I)
            heading = None
            for h in soup.find_all(["h2", "h3"]):
                if title_re.search(_text_of(h)):
                    heading = h
                    break
            if heading:
                cursor = heading
                for _ in range(8):  # walk a few sibling containers
                    cursor = cursor.find_next(["div", "section"])
                    if not cursor:
                        break
                    if cursor.select_one(".bui-card, .bui-carousel__item, .bui-card__content, a[href]"):
                        container = cursor
                        break

        regions: List[Dict[str, str]] = []
        if container:
            anchors = container.select('.bui-carousel__item a[href], a.bui-card[href], .bui-card a[href]')
            if not anchors:
                anchors = container.select('a[href]')

            for a in anchors:
                url = urljoin(BASE_URL, a["href"])
                img_el = a.select_one(".bui-card__image-container img") or a.find("img")
                image = _img_src(img_el)

                content = a.select_one(".bui-card__content")
                name_el = content.select_one("h3.bui-card__title") if content else None
                name = _text_of(name_el) or _text_of(content)

                # Regions must have a name and a link to be useful
                if name and url:
                    regions.append({"name": name, "url": url, "image": image})

        # Dedupe by (name, url)
        seen = set()
        uniq = []
        for r in regions:
            key = (r.get("name", ""), r.get("url", ""))
            if key in seen:
                continue
            seen.add(key)
            uniq.append(r)
        return uniq


# ---------- Workers ----------

def resolve_country_worker(s: requests.Session, region: Dict) -> Optional[Tuple[str, str]]:
    try:
        html = get_html(s, region["url"])
        name, url = parse_country_from_breadcrumb(html, region["url"])
        if not name:
            return None
        # normalise absolute URL
        if url and urlparse(url).netloc == "":
            url = urljoin(BASE_URL, url)
        return name.strip(), url
    except Exception as e:
        print(f"[WARN] Region failed {region.get('name')} {region.get('url')}: {e}", file=sys.stderr)
        return None


def scrape_country_worker(s: requests.Session, country: Dict[str, str]) -> Tuple[str, Dict]:
    html = get_html(s, country["url"])
    cities = parse_cities_from_country(html)
    popular_regions = parse_popular_regions(html)
    return country["url"], {"cities": cities, "popular_regions": popular_regions}

# ---------- Main ----------

def main(concurrency: int = None) -> None:
    # Default to very high concurrency for network bound scraping
    if concurrency is None:
        try:
            cpu = os.cpu_count() or 8
        except Exception:
            cpu = 8
        concurrency = max(128, cpu * 64)  # very high by default

    regions = load_regions(REGIONS_SOURCE)
    session = build_session(concurrency)

    # Stage 1 — resolve unique countries fast
    pairs: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(resolve_country_worker, session, r) for r in regions]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                pairs.append(res)

    # Dedupe by URL, then name as fallback
    seen = set()
    countries = []
    for name, url in pairs:
        key = url or name
        if key in seen:
            continue
        seen.add(key)
        countries.append({"name": name, "url": url})

    # Prepare output structure
    countries_full = [{"name": c["name"], "url": c["url"], "cities": [], "popular_regions": []} for c in countries]
    # Save countries list immediately
    save_countries({"countries": countries_full}, COUNTRIES_OUT)
    print(f"[stage1] Saved {len(countries_full)} countries to {COUNTRIES_OUT}")

    # Stage 2 — scrape each country in parallel for cities and popular regions
    url_to_idx = {c["url"]: i for i, c in enumerate(countries_full)}
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(scrape_country_worker, session, c) for c in countries]
        for fut in as_completed(futures):
            try:
                url, payload = fut.result()
                idx = url_to_idx.get(url)
                if idx is not None:
                    countries_full[idx]["cities"] = payload.get("cities", [])
                    countries_full[idx]["popular_regions"] = payload.get("popular_regions", [])
            except Exception as e:
                print(f"[WARN] Country scrape failed: {e}", file=sys.stderr)

    save_countries({"countries": countries_full}, COUNTRIES_OUT)
    print(f"[stage2] Attached cities and popular regions. Saved to {COUNTRIES_OUT}")


if __name__ == "__main__":
    # Allow: python scrape_Countries.py 512   to force a specific thread count
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        main(int(sys.argv[1]))
    else:
        main()
