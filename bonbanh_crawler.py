#!/usr/bin/env python3
"""
Filename: bonbanh_crawler.py

Modes:
  1) category: crawl listing URLs from a category and parse details
  2) idrange:  crawl listing details from an ID range

Requires: requests, beautifulsoup4, lxml, tqdm

python bonbanh_crawler.py category https://bonbanh.com/oto-tu-nam-2022-cu-da-qua-su-dung --pages 30 --max-listings 500 --out oto_sample.json
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

# tốc độ hợp lý: ~0.5s/request
SLEEP_MIN = 0.3
SLEEP_MAX = 0.8

USER_AGENTS = [
    DEFAULT_USER_AGENT,
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) "
    "Gecko/20100101 Firefox/120.0",
]

YEAR_NOW = time.localtime().tm_year  # filter cars older than this year


def polite_sleep():
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


def rotate_headers(session):
    ua = random.choice(USER_AGENTS)
    session.headers.update(
        {
            "User-Agent": ua,
            "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
        }
    )


def check_robots(base_url, path="/", session=None):
    """
    Check robots.txt, return (allowed, robots_url, reason)
    """
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        ua = (
            session.headers.get("User-Agent", DEFAULT_USER_AGENT)
            if session
            else DEFAULT_USER_AGENT
        )
        allowed = rp.can_fetch(ua, path)
        reason = None if allowed else f"Blocked by robots.txt for UA={ua}"
        return allowed, robots_url, reason
    except Exception as e:
        logging.warning("Could not read robots.txt (%s). Assuming allowed.", e)
        return True, robots_url, None


def fetch_with_retry(session, url, timeout=18, max_retries=6, backoff_base=2.0):
    last_exc = None
    for attempt in range(1, max_retries + 1):
        rotate_headers(session)
        try:
            r = session.get(url, timeout=timeout)
        except requests.RequestException as e:
            last_exc = e
            wait = min(backoff_base**attempt, 60)
            logging.warning(
                "RequestException on %s (attempt %d): %s — retrying in %.1fs",
                url,
                attempt,
                e,
                wait,
            )
            time.sleep(wait)
            continue

        status = r.status_code
        if status == 200:
            return r
        if status == 404:
            return r
        if status == 429:
            wait = min(backoff_base**attempt, 120)
            logging.warning("429 on %s (attempt %d). Backoff %.1fs", url, attempt, wait)
            time.sleep(wait)
            last_exc = Exception("429 Too Many Requests")
            continue
        if status in (403, 451):
            wait = min((backoff_base**attempt) * 2, 300)
            logging.warning(
                "%s on %s (attempt %d). Possible block. Backoff %.1fs",
                status,
                url,
                attempt,
                wait,
            )
            time.sleep(wait)
            last_exc = Exception(f"{status} Forbidden")
            continue
        if 500 <= status < 600:
            wait = min(backoff_base**attempt, 120)
            logging.warning(
                "Server error %s on %s (attempt %d). Retry in %.1fs",
                status,
                url,
                attempt,
                wait,
            )
            time.sleep(wait)
            last_exc = Exception(f"Server error {status}")
            continue
        return r

    raise Exception(
        f"Failed to fetch {url} after {max_retries} retries. Last error: {last_exc}"
    )


def extract_listing_links_from_category(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    patt = re.compile(r"(?:/)?xe-[\w\-_]+-\d+", re.I)
    urls = set()
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        href = href.strip()
        if href.startswith("#") or href.lower().startswith(("javascript:", "mailto:")):
            continue
        path = urlparse(href).path or href
        if patt.search(path):
            full = urljoin(base_url, href)
            urls.add(full.split("?")[0])
    return sorted(urls)


def parse_key_values_from_section(text):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    kv = {}
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.endswith(":"):
            key = line.rstrip(":").strip()
            val = ""
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                if ":" not in next_line or re.search(r"\d", next_line):
                    val = next_line
                    i += 1
            kv[key] = val
        elif ":" in line:
            parts = line.split(":", 1)
            kv[parts[0].strip()] = parts[1].strip()
        i += 1
    return kv


def save_to_csv(results, out_csv="results.csv"):
    if not results:
        return
    keys = list(results[0].keys())
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in results:
            row_copy = row.copy()
            if isinstance(row_copy.get("images"), list):
                row_copy["images"] = ", ".join(row_copy["images"])
            writer.writerow(row_copy)
    print(f"[DONE] Saved {len(results)} listings to {out_csv}")


def parse_listing_page(html, url, download_images=False, img_dir="images"):
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n")
    title_tag = soup.find(["h1", "h2"])
    raw_title = title_tag.get_text(" ", strip=True) if title_tag else ""
    raw_title = re.sub(r"[\n\t]+", " ", raw_title).strip()
    parts = [p.strip() for p in raw_title.split("-", 1)]
    title = parts[0] if parts else raw_title
    title = " ".join(title.split())
    price = parts[1] if len(parts) > 1 else None
    m = re.search(r"Mã tin\s*[:：]?\s*(\d+)", text)
    listing_id = m.group(1) if m else re.search(r"-([0-9]{5,10})$", url).group(1)
    mdate = re.search(r"Đăng ngày\s*([\d/]{6,12})", text)
    posted_date = mdate.group(1) if mdate else None
    spec_text = ""
    if "Thông số kỹ thuật" in text:
        start = text.find("Thông số kỹ thuật")
        end_candidates = [
            text.find(marker, start + 1)
            for marker in ["Thông tin mô tả", "Liên hệ người bán", "Liên hệ"]
            if text.find(marker, start + 1) != -1
        ]
        end = min(end_candidates) if end_candidates else None
        spec_text = text[start : (end if end else start + 2000)]
    specs = parse_key_values_from_section(spec_text) if spec_text else {}
    imgs = []
    for img in soup.find_all("img"):
        src = img.get("data-src") or img.get("src") or ""
        if re.match(r"https://s\.bonbanh\.com/uploads/users/.+/m_\d+\.\d+\.jpg", src):
            imgs.append(src)
    imgs = list(dict.fromkeys(imgs))
    if download_images:
        os.makedirs(img_dir, exist_ok=True)
        for i, src in enumerate(imgs, 1):
            ext = os.path.splitext(urlparse(src).path)[1] or ".jpg"
            fname = f"{listing_id}_{i}{ext}"
            fpath = os.path.join(img_dir, fname)
            try:
                r = fetch_with_retry(requests.Session(), src)
                if r.status_code == 200:
                    with open(fpath, "wb") as f:
                        f.write(r.content)
            except Exception as e:
                logging.warning("Could not download image %s: %s", src, e)
    return {
        "url": url,
        "id": listing_id,
        "title": title,
        "price": price,
        "posted_date": posted_date,
        "specs": specs,
        "images": imgs,
    }


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
    allowed, robots_url, reason = check_robots(
        base_category_url, urlparse(base_category_url).path, session
    )
    if not allowed:
        print(
            f"[WARN] robots.txt ({robots_url}) may block this path: {reason}. Stopping."
        )
        return
    found_links = []
    for page in range(1, max_pages + 1):
        if page == 1:
            page_url = base_category_url
        else:
            page_url = base_category_url.rstrip("/") + f"/page,{page}"
        try:
            r = fetch_with_retry(session, page_url)
        except Exception as e1:
            try:
                page_url2 = base_category_url + (
                    ("&" if "?" in base_category_url else "?") + f"page={page}"
                )
                r = fetch_with_retry(session, page_url2)
            except Exception as e2:
                logging.warning(
                    "Could not load page %s (%s / %s). Stopping.", page_url, e1, e2
                )
                break
        links = extract_listing_links_from_category(r.text, base_root)
        new = [u for u in links if u not in found_links]
        if not new:
            break
        found_links.extend(new)
        print(f"[INFO] Page {page}: {len(new)} new links, total {len(found_links)}")
        if max_listings and len(found_links) >= max_listings:
            found_links = found_links[:max_listings]
            break
        polite_sleep()
    results = []
    for url in tqdm(found_links, desc="Crawl listings"):
        try:
            r = fetch_with_retry(session, url)
            data = parse_listing_page(r.text, url)
            year = int(data["specs"].get("Năm sản xuất", 0))
            if year < YEAR_NOW - 3:
                continue
            results.append(data)
        except Exception as e:
            logging.warning("Error crawling %s: %s", url, e)
        polite_sleep()
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    save_to_csv(results, out_csv=out_json.replace(".json", ".csv"))
    print(f"[DONE] Saved {len(results)} listings to {out_json}")
    return results


def crawl_id_range(
    session,
    id_start,
    id_end,
    url_template,
    download_images=False,
    out_json="results_idrange.json",
):
    base_root = f"{urlparse(url_template).scheme}://{urlparse(url_template).netloc}"
    allowed, robots_url, reason = check_robots(base_root, "/", session)
    if not allowed:
        print(f"[WARN] robots.txt ({robots_url}) may block: {reason}. Stopping.")
        return
    results = []
    for idn in tqdm(range(id_start, id_end + 1), desc="ID range"):
        url = url_template.format(idn)
        try:
            r = fetch_with_retry(session, url, timeout=12)
            if r.status_code == 404:
                polite_sleep()
                continue
            data = parse_listing_page(r.text, url)
            results.append(data)
        except Exception as e:
            logging.warning("Error fetching %s: %s", url, e)
        polite_sleep()
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"[DONE] Saved {len(results)} listings to {out_json}")
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Crawler bonbanh.com (with retry & UA rotation)"
    )
    sub = parser.add_subparsers(dest="mode", required=True, help="category or idrange")
    p_cat = sub.add_parser("category", help="crawl from category URL")
    p_cat.add_argument("category_url")
    p_cat.add_argument("--pages", type=int, default=3, help="max pages (default 3)")
    p_cat.add_argument("--max-listings", type=int, default=500)
    p_cat.add_argument("--out", default="bonbanh_category.json")
    p_id = sub.add_parser("idrange", help="crawl by id range")
    p_id.add_argument("start_id", type=int)
    p_id.add_argument("end_id", type=int)
    p_id.add_argument("url_template")
    p_id.add_argument("--out", default="bonbanh_idrange.json")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update(
        {"User-Agent": DEFAULT_USER_AGENT, "Accept-Language": "vi,en-US;q=0.9,en;q=0.8"}
    )

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


if __name__ == "__main__":
    main()
