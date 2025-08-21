#!/usr/bin/env python3
"""
Booking.com Locations Scraper — Countries → Regions → Cities
No Booking API. Pure HTTP + BeautifulSoup.
Python 3.9+

Usage
  pip install requests beautifulsoup4
  python main.py --sleep 0.6 --out booking_locations.json
  python main.py --countries nl pk za --out sample.json
"""

from __future__ import annotations
import argparse
import json
import os
import random
import re
import time
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- replace make_session() and fetch() with this ---

import importlib

BASE = "https://www.booking.com"

UA_POOL = [
    # a few modern realistic desktop UAs
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]

def _accept_encoding_header() -> str:
    # Only advertise br if the decoder exists, or servers may send br and Requests
    # won’t decode it. Either install `brotli` / `brotlicffi` or drop br.
    has_brotli = importlib.util.find_spec("brotli") or importlib.util.find_spec("brotlicffi")
    return "gzip, deflate, br" if has_brotli else "gzip, deflate"

def make_session() -> requests.Session:
    s = requests.Session()

    retry = Retry(
        total=8,
        connect=4,
        read=4,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=64)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    # Default headers as a real browser would send
    s.headers.update({
        "User-Agent": UA_POOL[0],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": _accept_encoding_header(),
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        # Modern client hints and fetch metadata
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Ch-Ua": '"Chromium";v="131", "Not=A?Brand";v="24", "Google Chrome";v="131"',
    })

    # you can also pin a realistic viewport if you want:
    # s.headers["Viewport-Width"] = "1920"

    # Attach a timeout tuple to use in .get
    s.timeout = (10, 30)
    return s

SESSION = make_session()
_BOOTSTRAPPED = False

def _bootstrap_cookies():
    """
    Hit the homepage and a harmless HTML page to collect cookies and CSRF-ish
    state before we go to /destination.html. Do it once per process.
    """
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    # landing page
    SESSION.get(f"{BASE}/", timeout=SESSION.timeout)
    # popular landing that sets some additional cookies
    SESSION.get(f"{BASE}/index.en-gb.html", timeout=SESSION.timeout)
    _BOOTSTRAPPED = True

def jitter(base: float) -> float:
    return base * (0.85 + random.random() * 0.3)

def fetch(url: str, retries: int = 2, referer: Optional[str] = None) -> str:
    """
    GET with extra manual retry layer that rotates UA and adds jitter
    on top of urllib3 Retry. Also sets a realistic Referer.
    """
    _bootstrap_cookies()

    last_exc = None
    for attempt in range(retries + 1):
        try:
            if attempt:
                time.sleep(jitter(0.6 + 0.4 * attempt))
                # rotate UA on retry
                ua = random.choice(UA_POOL)
                SESSION.headers["User-Agent"] = ua
                # rotate a slightly different client hint alongside UA
                SESSION.headers["Sec-Ch-Ua"] = '"Chromium";v="131", "Not=A?Brand";v="24", "Google Chrome";v="131"'

            # set a sane referer chain
            if referer:
                SESSION.headers["Referer"] = referer
            else:
                # derive a generic referer on first hop
                SESSION.headers["Referer"] = f"{BASE}/index.en-gb.html"

            r = SESSION.get(url, timeout=SESSION.timeout)
            sc = r.status_code

            if sc == 200:
                return r.text

            if sc in (301, 302, 303, 307, 308):
                # let requests follow redirects but keep headers stable
                # requests already auto-follows — just loop back to interpret final code
                pass

            if sc in (403, 429, 500, 502, 503, 504):
                # soft failures — retry with new UA and a backoff
                last_exc = requests.HTTPError(f"{sc} for {url}")
                continue

            r.raise_for_status()

        except requests.RequestException as e:
            last_exc = e
            continue

    raise RuntimeError(f"Failed to fetch after retries: {url} — {last_exc}")


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=8,
        connect=4,
        read=4,
        backoff_factor=1.2,                 # exponential backoff
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=64)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": UA_POOL[0],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    s.timeout = (10, 30)  # (connect, read)
    return s

SESSION = make_session()

def jitter(base: float) -> float:
    return base * (0.85 + random.random() * 0.3)

def fetch(url: str, retries: int = 2) -> str:
    """
    GET with an extra manual retry layer that rotates UA and adds jitter
    on top of urllib3 Retry for stubborn 502s.
    """
    last_exc = None
    for attempt in range(retries + 1):
        try:
            # small jitter to avoid thundering herd patterns
            if attempt:
                time.sleep(jitter(0.5 * attempt))
                # rotate UA on retry
                SESSION.headers["User-Agent"] = random.choice(UA_POOL)
            r = SESSION.get(url)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 500, 502, 503, 504):
                last_exc = requests.HTTPError(f"{r.status_code} for {url}")
                continue
            r.raise_for_status()
        except requests.RequestException as e:
            last_exc = e
            continue
    raise RuntimeError(f"Failed to fetch after retries: {url} — {last_exc}")

def dedupe(seq: List[str]) -> List[str]:
    seen = set(); out=[]
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def find_next_section_texts(header: Tag) -> List[str]:
    """Collect anchor texts under the block that follows `header` until next h2/h3."""
    texts: List[str] = []
    for el in header.find_all_next():
        if el.name in ("h2", "h3") and el is not header:
            break
        if el.name == "a":
            t = el.get_text(" ", strip=True)
            if t:
                texts.append(t)
    return texts

# ---------- Country discovery with redundancy ----------

def _countries_from_destination() -> List[Tuple[str, str]]:
    html = fetch(f"{BASE}/destination.html", referer=f"{BASE}/index.en-gb.html")
    print(html)
    exit()
    soup = BeautifulSoup(html, "html.parser")
    out: List[Tuple[str,str]] = []
    for a in soup.find_all("a", href=True):
        m = re.match(r"^/(destination/country|country)/([a-z]{2})\.html$", a["href"])
        if m:
            cc = m.group(2)
            name = a.get_text(strip=True)
            if name and cc:
                out.append((name, cc))
    # de‑dup preferring first seen
    seen=set(); res=[]
    for name, cc in out:
        if cc not in seen:
            seen.add(cc); res.append((name, cc))
    return res

def _country_codes_from_region_index() -> List[str]:
    """Scrape /region.html and collect all /region/<cc>/<slug>.html codes."""
    html = fetch(f"{BASE}/region.html")
    soup = BeautifulSoup(html, "html.parser")
    codes=set()
    for a in soup.find_all("a", href=True):
        m = re.match(r"^/region/([a-z]{2})/([a-z0-9\-]+)\.html$", a["href"])
        if m:
            codes.add(m.group(1))
    return sorted(codes)

def _country_codes_from_city_index() -> List[str]:
    """Scrape /city.html and collect all /city/<cc>/<slug>.html codes."""
    html = fetch(f"{BASE}/city.html")
    soup = BeautifulSoup(html, "html.parser")
    codes=set()
    for a in soup.find_all("a", href=True):
        m = re.match(r"^/city/([a-z]{2})/([a-z0-9\-]+)\.html$", a["href"])
        if m:
            codes.add(m.group(1))
    return sorted(codes)

def list_countries() -> List[Tuple[str, str]]:
    """
    Robust country discovery:
      1) Try /destination.html for names
      2) If that fails or returns few, union codes from /region.html and /city.html
         and synthesize name placeholders “CC”.
    """
    try:
        dest = _countries_from_destination()
    except Exception:
        dest = []

    codes = set(cc for _, cc in dest)
    # add codes from region and city indexes
    try:
        for cc in _country_codes_from_region_index():
            codes.add(cc)
    except Exception:
        pass
    try:
        for cc in _country_codes_from_city_index():
            codes.add(cc)
    except Exception:
        pass

    if not codes and not dest:
        raise RuntimeError("Could not discover any countries from multiple sources.")

    # build final list with names when we have them
    name_map = {cc: name for name, cc in dest}
    out: List[Tuple[str,str]] = []
    for cc in sorted(codes):
        out.append((name_map.get(cc, cc.upper()), cc))
    return out

# ---------- Country cities ----------

def list_country_cities(cc: str) -> List[str]:
    """
    Parse cities from /destination/country/<cc>.html
    Looks for a heading like “Cities in <Country>” then collects anchor texts.
    """
    url = f"{BASE}/destination/country/{cc}.html"
    html = fetch(url, referer=f"{BASE}/destination.html")
    soup = BeautifulSoup(html, "html.parser")

    hdr = None
    for tag in soup.find_all(["h2", "h3"]):
        txt = tag.get_text(" ", strip=True)
        if re.search(r"^Cities in\s+", txt, re.I) or re.search(r"^Cities\b", txt, re.I):
            hdr = tag
            break

    cities: List[str] = []
    if hdr:
        for t in find_next_section_texts(hdr):
            if re.search(r"\b(hotels?|map|See all|Filter)\b", t, re.I):
                continue
            if len(t) <= 1:
                continue
            if re.fullmatch(r"[A-Za-z]$", t):
                continue
            cities.append(t)

    return dedupe(cities)

# ---------- Regions ----------

def list_regions_from_country_page(cc: str) -> List[str]:
    """
    Read regions directly from /country/<cc>.html under
    “Hotels in the most popular regions …” which shows “<Region> <number> hotels”.
    """
    url = f"{BASE}/country/{cc}.html"
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")

    hdr = None
    for tag in soup.find_all(["h2", "h3"]):
        if "most popular regions" in tag.get_text(" ", strip=True).lower():
            hdr = tag
            break

    regions: List[str] = []
    if hdr:
        for t in find_next_section_texts(hdr):
            m = re.match(r"^(.*?)\s+\d+\s+hotels?$", t, re.I)
            if m:
                regions.append(m.group(1).strip())

    return dedupe(regions)

def list_regions_from_index(cc: str) -> List[Tuple[str, str]]:
    """
    Fallback — parse /region.html for all anchors matching /region/<cc>/<slug>.html
    Returns list of (region_name, region_slug)
    """
    html = fetch(f"{BASE}/region.html")
    soup = BeautifulSoup(html, "html.parser")

    regs: List[Tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        m = re.match(rf"^/region/{cc}/([a-z0-9\-]+)\.html$", a["href"])
        if m:
            name = a.get_text(strip=True)
            slug = m.group(1)
            if name and slug:
                regs.append((name, slug))

    # de‑dup
    seen = set(); out=[]
    for name, slug in regs:
        if slug not in seen:
            seen.add(slug); out.append((name, slug))
    return out

def list_region_cities(cc: str, region_slug: str) -> List[str]:
    """
    Given a region page /region/<cc>/<slug>.html, read the “popular cities in …” block.
    """
    url = f"{BASE}/region/{cc}/{region_slug}.html"
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")

    hdr = None
    for tag in soup.find_all(["h2", "h3"]):
        if "popular cities in" in tag.get_text(" ", strip=True).lower():
            hdr = tag
            break

    cities: List[str] = []
    if hdr:
        for t in find_next_section_texts(hdr):
            if re.search(r"\b(hotels?|map)\b", t, re.I):
                continue
            if len(t) > 1:
                cities.append(t)

    return dedupe(cities)

# ---------- Orchestration ----------

def scrape(countries: Optional[List[str]], sleep: float) -> Dict[str, Dict]:
    """
    Walk the hierarchy: Countries → Regions → Cities
    """
    result: Dict[str, Dict] = {}

    all_countries = list_countries()
    cc_map = {cc: name for name, cc in all_countries}
    to_do = countries or list(cc_map.keys())

    for cc in to_do:
        name = cc_map.get(cc, cc.upper())
        entry: Dict[str, object] = {"name": name, "code": cc}

        # country cities
        try:
            entry["cities"] = list_country_cities(cc)
        except Exception as e:
            entry["cities_error"] = str(e)
        time.sleep(jitter(sleep))

        # regions
        reg_names = []
        try:
            reg_names = list_regions_from_country_page(cc)
        except Exception:
            reg_names = []

        if not reg_names:
            try:
                regs = list_regions_from_index(cc)
                reg_entries = []
                for rname, rslug in regs:
                    time.sleep(jitter(sleep))
                    try:
                        rcities = list_region_cities(cc, rslug)
                    except Exception:
                        rcities = []
                    reg_entries.append({"name": rname, "slug": rslug, "cities": rcities})
                entry["regions"] = reg_entries
            except Exception as e:
                entry["regions_error"] = str(e)
        else:
            entry["regions"] = [{"name": r, "slug": None, "cities": []} for r in reg_names]

        result[cc] = entry
        time.sleep(jitter(sleep))

    return result

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--countries", nargs="*", help="Limit to country codes like nl pk za")
    ap.add_argument("--sleep", type=float, default=0.6, help="Delay between requests")
    ap.add_argument("--out", default="booking_locations.json", help="Output JSON file")
    args = ap.parse_args()

    data = scrape(args.countries, args.sleep)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Wrote {args.out}")

if __name__ == "__main__":
    _countries_from_destination()
