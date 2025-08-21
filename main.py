#!/usr/bin/env python3
"""
Booking.com threaded scraper
Hierarchy: Region -> Country -> HotelListing
Outputs booking_hierarchy.json

Usage ideas while testing:
  python main.py --max-regions 3 --max-countries 2 --max-pages 1

After validation, remove or increase the limits.
"""

import json
import time
import random
import re
import sys
import argparse
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed


BASE = "https://www.booking.com"
REGIONS_URL = f"{BASE}/region.html"

# ---------- HTTP session with retries and backoff ----------

def make_session() -> requests.Session:
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
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    })
    retry = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def fetch_html(session: requests.Session, url: str, *, sleep_range=(0.6, 1.6)) -> str:
    # small jitter to look less robotic
    time.sleep(random.uniform(*sleep_range))
    r = session.get(url, timeout=25)
    # Some anti bot walls return 200 with empty or script heavy content.
    # We keep a simple sanity check on content length.
    if r.status_code != 200 or len(r.text) < 1000:
        r.raise_for_status()
    return r.text

# ---------- Models ----------

@dataclass
class Hotel:
    name: str
    url: str

@dataclass
class Country:
    name: str
    url: str
    listings: List[Hotel]

@dataclass
class Region:
    name: str
    url: str
    countries: List[Country]

# ---------- Parsing helpers ----------

def absolutize(href: str) -> str:
    return urljoin(BASE, href)

def dedupe_preserve_order(items: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    seen = set()
    out = []
    for k in items:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out

# ---------- Step 1: Regions ----------

def parse_regions(html: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    pairs: List[Tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(strip=True)
        if not text or not href:
            continue
        # Region links usually start with /region
        # Examples: /region/nl/dutch-coast.html or /region/es-tenerife.html
        if href.startswith("/region"):
            pairs.append((text, absolutize(href)))
    return dedupe_preserve_order(pairs)

# ---------- Step 2: From region page, find the country anchor ----------

def extract_country_from_region_page(html: str) -> Optional[Tuple[str, str]]:
    """
    On most region pages there is a breadcrumb or a link to the country like /country/nl.html
    We try a few robust patterns.
    """
    soup = BeautifulSoup(html, "lxml")

    # Look for explicit country links
    for a in soup.select('a[href^="/country/"]'):
        name = a.get_text(strip=True)
        href = absolutize(a["href"])
        if name and href:
            return name, href

    # Fallback: infer country code from canonical like /region/nl/slug
    can = soup.find("link", rel="canonical")
    if can and can.get("href"):
        m = re.search(r"/region/([a-z]{2})/", can["href"])
        if m:
            cc = m.group(1)
            # try to find a visible country name nearby
            # many pages show "Netherlands" in breadcrumbs
            crumb = soup.select_one('nav[aria-label*="breadcrumb"], ol.breadcrumb, ol[aria-label*="breadcrumb"]')
            if crumb:
                # pick the last anchor that is not the current region
                anchors = [a for a in crumb.find_all("a", href=True)]
                if anchors:
                    name = anchors[-1].get_text(strip=True)
                    if name:
                        return name, f"{BASE}/country/{cc}.html"
            # last resort, use code as name
            return cc.upper(), f"{BASE}/country/{cc}.html"

    return None

# ---------- Step 3: From a country page, discover the country wide search URL ----------

def find_country_search_url(html: str, fallback_country_name: Optional[str]) -> Optional[str]:
    """
    Look for a link to searchresults for the country.
    Good patterns contain dest_type=country or &ss=<CountryName>.
    """
    soup = BeautifulSoup(html, "lxml")

    # Prefer an explicit dest link
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "searchresults" in href and ("dest_type=country" in href or "dest_id=" in href):
            return absolutize(href)

    # Try any searchresults with country name
    if fallback_country_name:
        cand = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "searchresults" in href and "ss=" in href:
                cand = absolutize(href)
                break
        if cand:
            # Ensure ss matches country name
            return cand

    # Construct a generic search query as last resort
    if fallback_country_name:
        q = {
            "ss": fallback_country_name,
            "ssne": fallback_country_name,
            "ssne_untouched": fallback_country_name,
            "lang": "en-us",
        }
        return f"{BASE}/searchresults.html?{urlencode(q)}"

    return None

# ---------- Step 4: Parse hotel listings and paginate ----------

def parse_hotels_from_search(html: str) -> List[Hotel]:
    soup = BeautifulSoup(html, "lxml")
    hotels: List[Hotel] = []

    # Newer markup uses data-testid=title-link on the property card
    for a in soup.select('a[data-testid="title-link"][href]'):
        name = a.get_text(strip=True)
        href = absolutize(a["href"])
        if name and href and "/hotel/" in href:
            hotels.append(Hotel(name=name, url=href))

    # Fallback generic anchors that look like hotel pages
    if not hotels:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/hotel/"):
                text = a.get_text(strip=True)
                if text:
                    hotels.append(Hotel(name=text, url=absolutize(href)))

    # Deduplicate by URL
    seen = set()
    out = []
    for h in hotels:
        if h.url not in seen:
            seen.add(h.url)
            out.append(h)
    return out

def find_next_page(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")

    # rel=next when available
    ln = soup.find("link", rel="next")
    if ln and ln.get("href"):
        return absolutize(ln["href"])

    # Pagination control with aria label
    for a in soup.select('a[aria-label*="Next"][href], a[rel="next"][href]'):
        return absolutize(a["href"])

    # Heuristic: increment the offset parameter if present
    parsed = urlparse(current_url)
    qs = dict(parse_qsl(parsed.query))
    if "offset" in qs:
        try:
            nxt = int(qs["offset"]) + int(qs.get("rows", "25"))
        except ValueError:
            nxt = int(qs["offset"]) + 25
        qs["offset"] = str(nxt)
        new_q = urlencode(qs)
        return urlunparse(parsed._replace(query=new_q))

    return None

def scrape_country_listings(session: requests.Session, search_url: str, max_pages: int) -> List[Hotel]:
    listings: List[Hotel] = []
    url = search_url
    pages_done = 0
    while url and pages_done < max_pages:
        html = fetch_html(session, url)
        page_hotels = parse_hotels_from_search(html)
        listings.extend(page_hotels)
        pages_done += 1
        next_url = find_next_page(html, url)
        # stop if nothing new showed up
        if not next_url or next_url == url:
            break
        url = next_url
    # final dedupe
    unique = []
    seen = set()
    for h in listings:
        if h.url not in seen:
            seen.add(h.url)
            unique.append(h)
    return unique

# ---------- Orchestration per region ----------

def process_region(session: requests.Session, region_pair: Tuple[str, str],
                   max_countries: int, max_pages: int) -> Region:
    region_name, region_url = region_pair
    try:
        r_html = fetch_html(session, region_url)
    except Exception as e:
        print(f"[region error] {region_name} {region_url} -> {e}", file=sys.stderr)
        return Region(name=region_name, url=region_url, countries=[])

    country_info = extract_country_from_region_page(r_html)
    countries: List[Country] = []

    candidates: List[Tuple[str, str]] = []
    if country_info:
        candidates.append(country_info)

    # Some region pages mention multiple countries. Collect any extra visible ones.
    soup = BeautifulSoup(r_html, "lxml")
    for a in soup.select('a[href^="/country/"]'):
        nm = a.get_text(strip=True)
        hr = absolutize(a["href"])
        if nm and hr and (nm, hr) not in candidates:
            candidates.append((nm, hr))

    # Deduplicate and respect max_countries
    candidates = dedupe_preserve_order(candidates)[:max_countries] if max_countries > 0 else dedupe_preserve_order(candidates)

    for country_name, country_url in candidates:
        try:
            c_html = fetch_html(session, country_url)
        except Exception as e:
            print(f"[country error] {country_name} {country_url} -> {e}", file=sys.stderr)
            countries.append(Country(name=country_name, url=country_url, listings=[]))
            continue

        search_url = find_country_search_url(c_html, country_name)
        listings: List[Hotel] = []
        if search_url:
            try:
                listings = scrape_country_listings(session, search_url, max_pages=max_pages)
            except Exception as e:
                print(f"[listings error] {country_name} search={search_url} -> {e}", file=sys.stderr)
        else:
            print(f"[no search link] {country_name} from {country_url}", file=sys.stderr)

        countries.append(Country(
            name=country_name,
            url=country_url,
            listings=listings
        ))

    return Region(name=region_name, url=region_url, countries=countries)

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default="booking_hierarchy.json")
    ap.add_argument("--threads", type=int, default=6)
    ap.add_argument("--max-regions", type=int, default=5, help="0 means all")
    ap.add_argument("--max-countries", type=int, default=1, help="countries per region. 0 means all discovered")
    ap.add_argument("--max-pages", type=int, default=1, help="listing pages per country")
    args = ap.parse_args()

    session = make_session()
    print("Fetching regions...")
    regions_html = fetch_html(session, REGIONS_URL)
    region_pairs = parse_regions(regions_html)

    if args.max_regions and args.max_regions > 0:
        region_pairs = region_pairs[:args.max_regions]

    print(f"Found {len(region_pairs)} region links to process")

    results: List[Region] = []

    with ThreadPoolExecutor(max_workers=max(1, args.threads)) as ex:
        futures = {
            ex.submit(process_region, session, rp, args.max_countries, args.max_pages): rp
            for rp in region_pairs
        }
        for fut in as_completed(futures):
            rp = futures[fut]
            try:
                region_obj = fut.result()
                results.append(region_obj)
                total_c = sum(len(c.listings) for c in region_obj.countries)
                print(f"[done] {region_obj.name} | countries={len(region_obj.countries)} | listings={total_c}")
            except Exception as e:
                print(f"[thread error] {rp} -> {e}", file=sys.stderr)

    # Sort regions by name for stable output
    results.sort(key=lambda r: r.name.lower())

    # Convert to plain dict for JSON
    def region_to_dict(r: Region) -> Dict:
        return {
            "name": r.name,
            "url": r.url,
            "countries": [
                {
                    "name": c.name,
                    "url": c.url,
                    "listings": [asdict(h) for h in c.listings],
                }
                for c in r.countries
            ],
        }

    out = [region_to_dict(r) for r in results]

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    # Quick summary
    total_regions = len(results)
    total_countries = sum(len(r.countries) for r in results)
    total_listings = sum(len(h.listings) for r in results for h in r.countries)
    print(f"Saved {args.output} | regions={total_regions} countries={total_countries} listings={total_listings}")

if __name__ == "__main__":
    main()
