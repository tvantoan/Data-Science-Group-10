#!/usr/bin/env python3
"""
Filename:
bonbanh_crawler.py

- Two modes:
  1) category mode: given a category URL (e.g. https://bonbanh.com/oto or https://bonbanh.com/oto/lexus)
     and max pages to find listing links and crawl each listing. (RECOMMENDED))
  2) idrange mode: given start_id and end_id, try to access each /<slug>-<id> (many 404). (NOT RECOMMENDED, cannot customize cars input)

Results saved as JSON/CSV; option to download images.

Requires: requests, beautifulsoup4, lxml, tqdm
pip install requests beautifulsoup4 lxml tqdm

Sample script:
- Category python bonbanh_crawler.py category https://bonbanh.com/oto --pages 3 --max-listings 100 --out oto_sample.json
Replace with your desired category URL, pages, max listings, and output file.
- ID range python bonbanh_crawler.py idrange 6433596 6433600 "https://bonbanh.com/xe-{}" --out idrange_sample.json

"""

import argparse
import csv
import json
import logging
import os
import random
import re
import time
import urllib.robotparser
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# --- Config ---
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)
SLEEP_MIN = 1.0
SLEEP_MAX = 2.5
HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
}

YEAR_NOW = current_year = (
    time.localtime().tm_year
)  # filter out cars older than this year


# Sleep random time between requests to not overload server and get blocked
# If you want faster crawling, reduce SLEEP_MIN/SLEEP_MAX but be polite.
def polite_sleep():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


# Check robots.txt for allowed paths
def check_robots(base_url, path="/"):
    """
    Try to parse robots.txt; return True if allowed or unknown.
    """
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        allowed = rp.can_fetch(DEFAULT_USER_AGENT, path)
        return allowed, robots_url
    except Exception as e:
        # If can't read robots, assume allowed but warn
        logging.warning("Could not query robots.txt (%s). Assuming allowed.", e)
        return True, robots_url


# Fetch URL with session and raise for status
def fetch(url, session, timeout=18):
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r


# Extract listing links from category page
def extract_listing_links_from_category(html, base_url):
    """
    Find all links of the form /xe-...-<id> in the category page.
    Return a complete list of absolute URLs (unique).
    """
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    patt = re.compile(r"(?:/)?xe-[\w\-_]+-\d+", re.I)
    # Filter links that match the pattern like /xe-<slug>-<id>
    urls = set()

    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        href = href.strip()
        # skip non-URL anchors
        if (
            href.startswith("#")
            or href.lower().startswith("javascript:")
            or href.lower().startswith("mailto:")
        ):
            continue

        path = urlparse(href).path or href

        if patt.search(path):
            full = urljoin(base_url, href)
            urls.add(full.split("?")[0])
    return sorted(urls)


# Parse key:value pairs from 'Technical Specifications' section
def parse_key_values_from_section(text):
    """
    Based on the text of the 'Technical Specifications' section, extract key:value pairs heuristically.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    kv = {}
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.endswith(":"):
            key = line.rstrip(":").strip()
            val = ""
            # take the next non-empty line as value if reasonable
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                # if next_line has a colon, it's the next key -> value is empty
                if ":" not in next_line or re.search(r"\d", next_line):
                    val = next_line
                    i += 1
            kv[key] = val
        elif ":" in line:
            parts = line.split(":", 1)
            kv[parts[0].strip()] = parts[1].strip()
        else:
            # line without ':', may be a value based on previous key (skip)
            pass
        i += 1
    return kv


PHONE_RE = re.compile(r"((?:\+84|84|0)[\s.-]?\d{2,3}[\s.-]?\d{3}[\s.-]?\d{3,4})")


# Parse a listing page to extract details


def parse_listing_page(html, url, download_images=False, img_dir="images"):
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n")

    # Title + tách giá
    title_tag = soup.find(["h1", "h2"])
    raw_title = title_tag.get_text(separator=" ", strip=True) if title_tag else ""
    raw_title = re.sub(r"[\n\t]+", " ", raw_title).strip()

    # Tìm giá trong title
    mprice = re.search(r"(-\s*[\d\s\.,]*\s*(Tỷ|Triệu|VNĐ|đ|VND))", raw_title, re.I)
    price = mprice.group(0).lstrip("-").strip() if mprice else None

    # Loại bỏ giá khỏi title
    title = re.sub(
        r"(-\s*[\d\s\.,]*\s*(Tỷ|Triệu|VNĐ|đ|VND))", "", raw_title, flags=re.I
    ).strip()

    # Listing id
    m = re.search(r"Mã tin\s*[:：]?\s*(\d+)", text)
    if m:
        listing_id = m.group(1)
    else:
        m2 = re.search(r"-([0-9]{5,10})$", url)
        listing_id = m2.group(1) if m2 else None

    # Date
    mdate = re.search(r"Đăng ngày\s*([\d/]{6,12})", text)
    posted_date = mdate.group(1) if mdate else None

    # Spec section
    spec_text = ""
    if "Thông số kỹ thuật" in text:
        start = text.find("Thông số kỹ thuật")
        end_candidates = []
        for marker in ["Thông tin mô tả", "Liên hệ người bán", "Liên hệ"]:
            idx = text.find(marker, start + 1)
            if idx != -1:
                end_candidates.append(idx)
        end = min(end_candidates) if end_candidates else None
        spec_text = text[start : (end if end else start + 2000)]
    specs = parse_key_values_from_section(spec_text) if spec_text else {}

    # Description
    desc = ""
    if "Thông tin mô tả" in text:
        s = text.find("Thông tin mô tả")
        endc = text.find("Liên hệ người bán", s + 1)
        end = endc if endc != -1 else s + 800
        desc = text[s:end].replace("Thông tin mô tả", "").strip()
        desc = re.sub(r"[\n\t]+", " ", desc).strip()

    # Images
    imgs = []
    for img in soup.find_all("img"):
        src = img.get("data-src") or img.get("src") or ""
        if not src or len(src) < 10:
            continue
        if re.search(
            r"avatar|icon|logo|loading|spinner|pixel|approved2|bct|reg_i", src, re.I
        ):
            continue
        full_src = urljoin(url, src)
        imgs.append(full_src)
    imgs = list(dict.fromkeys(imgs))  # unique, preserve order

    # Download images nếu bật
    if download_images:
        os.makedirs(img_dir, exist_ok=True)
        for i, src in enumerate(imgs, 1):
            ext = os.path.splitext(urlparse(src).path)[1]
            if not ext or len(ext) > 5:
                ext = ".jpg"
            fname = f"{listing_id}_{i}{ext}"
            fpath = os.path.join(img_dir, fname)
            try:
                r = requests.get(src, headers=HEADERS, timeout=15)
                if r.status_code == 200:
                    with open(fpath, "wb") as f:
                        f.write(r.content)
            except Exception as e:
                logging.warning("Could not download image %s: %s", src, e)

    # Return structure, bỏ contact
    return {
        "url": url,
        "id": listing_id,
        "title": title,
        "price": price,
        "posted_date": posted_date,
        "specs": specs,
        "description": desc,
        "images": imgs,
    }


# --- Crawler flows ---
def crawl_category(
    session,
    base_category_url,
    max_pages=5,
    max_listings=None,
    download_images=False,
    out_json="results.json",
):
    base_root = (
        f"{urlparse(base_category_url).scheme}://{urlparse(base_category_url).netloc}"
    )
    allowed, robots_url = check_robots(
        base_category_url, urlparse(base_category_url).path
    )
    if not allowed:
        print(
            f"[WARN] robots.txt ({robots_url}) may block this path. Stopping for ethical reasons."
        )
        return
    found_links = []
    for page in range(1, max_pages + 1):
        # bonbanh uses pagination like /page%2C{n} in some pages
        if page == 1:
            page_url = base_category_url
        else:
            # try two common forms
            page_url = base_category_url.rstrip("/") + f"/page%2C{page}"
        try:
            r = fetch(page_url, session)
        except Exception as e1:
            # try an alternative pattern ?page=
            try:
                page_url2 = base_category_url + (
                    ("&" if "?" in base_category_url else "?") + f"page={page}"
                )
                r = fetch(page_url2, session)
            except Exception as e2:
                logging.warning(
                    "Could not load page %s (%s / %s). Stopping page loop.",
                    page_url,
                    e1,
                    e2,
                )
                break
        links = extract_listing_links_from_category(r.text, base_root)
        new = [u for u in links if u not in found_links]
        if not new:
            # no new links -> may be finished
            break
        found_links.extend(new)
        print(f"[INFO] Page {page}: found {len(new)} links, total {len(found_links)}")
        if max_listings and len(found_links) >= max_listings:
            found_links = found_links[:max_listings]
            break
        polite_sleep()
    # Crawl each listing
    results = []
    for url in tqdm(found_links, desc="Crawl listings"):
        try:
            r = fetch(url, session)
            data = parse_listing_page(r.text, url)
            year = int(data["specs"].get("Năm sản xuất", 0))
            if year < YEAR_NOW - 3:  # filter out car older than 3 years
                continue
            results.append(data)
        except Exception as e:
            logging.warning("Lỗi khi crawl %s: %s", url, e)
        polite_sleep()
    # Save JSON
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"[DONE] Saved {len(results)} listings to {out_json}")
    return results


# Crawl by ID range
def crawl_id_range(
    session,
    id_start,
    id_end,
    url_template,
    download_images=False,
    out_json="results_idrange.json",
):
    """
    url_template: e.g. "https://bonbanh.com/xe-bmw-4_series-{}" or better "https://bonbanh.com/xe-some-slug-{}"
    If slug unknown, user can use pattern: "https://bonbanh.com/xe-{}" but many URLs will 404.
    """
    base_root = f"{urlparse(url_template).scheme}://{urlparse(url_template).netloc}"
    allowed, robots_url = check_robots(base_root, "/")
    if not allowed:
        print(f"[WARN] robots.txt ({robots_url}) may block. Stopping.")
        return
    results = []
    for idn in tqdm(range(id_start, id_end + 1), desc="ID range"):
        # try to construct url
        url = url_template.format(idn)
        try:
            r = session.get(url, timeout=12)
            if r.status_code == 404:
                polite_sleep()
                continue
            r.raise_for_status()
            data = parse_listing_page(r.text, url)
            results.append(data)
        except requests.HTTPError as he:
            logging.debug("HTTPError %s for %s", he, url)
        except Exception as e:
            logging.warning("Error fetching %s: %s", url, e)
        polite_sleep()
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"[DONE] Saved {len(results)} listings to {out_json}")
    return results


# category mode sample: python bonbanh_crawler.py category https://bonbanh.com/oto --pages 3 --max-listings 100 --out oto_sample.json
# id range mode sample: python bonbanh_crawler.py idrange 6433596 6448777 "https://bonbanh.com/xe-{}" --out idrange_sample.json
def main():
    parser = argparse.ArgumentParser(description="Crawler bonbanh.com (simple, polite)")
    sub = parser.add_subparsers(dest="mode", required=True, help="category or idrange")
    p_cat = sub.add_parser("category", help="crawl from category URL")
    p_cat.add_argument(
        "category_url",
        help="e.g. https://bonbanh.com/oto or https://bonbanh.com/oto/lexus",
    )
    p_cat.add_argument("--pages", type=int, default=3, help="max pages (default 3)")
    p_cat.add_argument(
        "--max-listings", type=int, default=200, help="limit total listings"
    )
    p_cat.add_argument("--out", default="bonbanh_category.json")
    p_id = sub.add_parser("idrange", help="crawl by id range")
    p_id.add_argument("start_id", type=int)
    p_id.add_argument("end_id", type=int)
    p_id.add_argument(
        "url_template",
        help="URL template containing {} for id, e.g. 'https://bonbanh.com/xe-bmw--{}' or 'https://bonbanh.com/xe-{}'",
    )
    p_id.add_argument("--out", default="bonbanh_idrange.json")

    args = parser.parse_args()
    session = requests.Session()
    session.headers.update(HEADERS)

    if args.mode == "category":
        crawl_category(
            session,
            args.category_url,
            max_pages=args.pages,
            max_listings=args.max_listings,
            out_json=args.out,
        )
    elif args.mode == "idrange":
        crawl_id_range(
            session, args.start_id, args.end_id, args.url_template, out_json=args.out
        )


main()
