#!/usr/bin/env python3
"""
crawler.py
Crawl danh mục / tin rao oto.com.vn (tên xe, giá, thông số).
Sử dụng Playwright (sync) + BeautifulSoup để parse.

Requires: playwright, beautifulsoup4, lxml, tqdm
Install Playwright browsers: playwright install
Example usage:
python crawler.py category https://oto.com.vn/mua-ban-xe/f02255 --pages 3 --max-listings 100 --out oto_sample.json // crawl 3 pages of Ford cars, max 100 listings, year of manufacture >= 2022
"""

import argparse
import csv
import json
import logging
import random
import re
import time
import urllib.robotparser
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from tqdm import tqdm

# -------- CONFIG / DEFAULTS --------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
]

DEFAULT_SLEEP_MIN = 0.25
DEFAULT_SLEEP_MAX = 0.6
DEFAULT_CONTEXT_POOL = 3
NAVIGATE_TIMEOUT = 20000  # ms

PRICE_RE = re.compile(
    r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?\s*(?:tỷ|tỉ|triệu|tri|vnđ|vnd|đ))",
    re.I,
)
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
KM_RE = re.compile(r"(\d[\d\.,]*\s*(?:km|km\b|nghìn km))", re.I)

# pattern to match *detail* pages like:
# /mua-ban-xe-ford-territory-hai-phong/gia-tot-...-aidxc23364843
DETAIL_HREF_RE = re.compile(r"^/mua-ban-xe-[^/]+/.+", re.I)
DETAIL_FULLURL_RE = re.compile(r"^https?://[^/]+/mua-ban-xe-[^/]+/.+", re.I)


# -------- helpers --------
def polite_sleep(min_s=DEFAULT_SLEEP_MIN, max_s=DEFAULT_SLEEP_MAX):
    time.sleep(random.uniform(min_s, max_s))


def check_robots(base_url, path="/"):
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
        allowed = rp.can_fetch("*", path)
        return allowed, robots_url
    except Exception as e:
        logging.warning(
            "Could not read robots.txt (%s). Assuming allowed (%s).", robots_url, e
        )
        return True, robots_url


def extract_price_from_text(text):
    if not text:
        return None
    m = PRICE_RE.search(text)
    return m.group(1).strip() if m else None


def normalize_model_name(raw_title: str) -> str:
    if not raw_title:
        return ""
    raw_title = re.sub(r"\s+", " ", raw_title).strip()
    tokens = [t.strip() for t in re.split(r"\s*-\s*", raw_title) if t.strip()]

    def is_noise(tok):
        if YEAR_RE.search(tok):
            return True
        if "km" in tok.lower():
            return True
        if PRICE_RE.search(tok):
            return True
        return False

    meaningful = [t for t in tokens if not is_noise(t)]
    if meaningful:
        meaningful.sort(key=lambda s: (-len(s), s))
        return meaningful[0]
    return re.sub(r"^(19|20)\d{2}\s*[-:\s]*", "", raw_title).strip()


def parse_specs_from_soup(soup, page_text):
    specs = {}
    rows = soup.select("ul.list-info > li")
    for li in rows:
        label_el = li.select_one("label.label")
        if label_el:
            key = label_el.get_text(strip=True).rstrip(":")
            val = (
                li.get_text(" ", strip=True)
                .replace(label_el.get_text(strip=True), "")
                .strip()
            )
            if key and val:
                specs[key] = val

    return specs


def parse_listing_page(html: str, url: str):
    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text("\n")

    title_tag = soup.find(["h1", "h2"]) or soup.find("title")
    raw_title = title_tag.get_text(" ", strip=True) if title_tag else ""
    model_name = normalize_model_name(raw_title)

    top_text = page_text[:4000] if page_text else ""
    price = extract_price_from_text(top_text)

    m = re.search(r"Mã tin\s*[:：]?\s*(\d+)", page_text)
    if m:
        listing_id = m.group(1)
    else:
        m2 = re.search(r"aidxc(\d+)", url)
        listing_id = m2.group(1) if m2 else None

    specs = parse_specs_from_soup(soup, page_text)
    imgs = []
    for img in soup.find_all("img"):
        src = img.get("data-src") or img.get("src") or ""
        if re.match(
            r"^https:\/\/img1\.oto\.com\.vn\/crop\/640x480\/[A-Za-z0-9\/\-\_]+\.webp$",
            src,
        ):
            imgs.append(src)
    imgs = list(dict.fromkeys(imgs))
    return {
        "url": url,
        "id": listing_id,
        "raw_title": raw_title,
        "title": model_name,
        "price": price,
        "specs": specs,
        "images": imgs,
    }


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


def crawl_detail_links(
    found_links, contexts, out_json, sleep_min, sleep_max, max_listings
):
    results = []
    for idx, link in enumerate(
        tqdm(found_links[:max_listings], desc="Crawling listings")
    ):
        ctx = contexts[idx % len(contexts)]
        page = ctx.new_page()
        try:
            page.goto(link, wait_until="networkidle", timeout=NAVIGATE_TIMEOUT)
            body_text = page.inner_text("body")
            if "Nhập mã xác nhận" in body_text:
                logging.warning("Captcha on %s", link)
                continue
            html = page.content()
            data = parse_listing_page(html, link)
            results.append(data)
        except Exception as e:
            logging.warning("Error crawling %s: %s", link, e)
        finally:
            page.close()
        polite_sleep(sleep_min, sleep_max)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    save_to_csv(results, out_csv=out_json.replace(".json", ".csv"))
    logging.info("Saved %d listings to %s", len(results), out_json)
    return results


MFDATE_RE = re.compile(r"^(mfdate=)(.*)$", re.I)


# -------- CRAWLER (sync Playwright) --------
# --- thay thế crawl_category_playwright (chính) ---
def crawl_category_playwright(
    category_url,
    pages=3,
    max_listings=200,
    out_json="results.json",
    headless=True,
    pool_size=DEFAULT_CONTEXT_POOL,
    sleep_min=DEFAULT_SLEEP_MIN,
    sleep_max=DEFAULT_SLEEP_MAX,
    proxies=None,
    check_robots_flag=True,
):
    allowed, robots_url = check_robots(category_url)
    if check_robots_flag and not allowed:
        logging.warning(
            "robots.txt (%s) may block crawling %s — exiting.", robots_url, category_url
        )
        return []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless, args=["--no-sandbox"])
        contexts = []
        proxies_list = proxies.split(",") if proxies else [None] * pool_size
        for i in range(pool_size):
            ua = random.choice(USER_AGENTS)
            proxy_config = None
            if proxies:
                proxy_url = proxies_list[i % len(proxies_list)].strip()
                if proxy_url:
                    proxy_config = {"server": proxy_url}
            ctx = browser.new_context(
                user_agent=ua,
                locale="vi-VN",
                timezone_id="Asia/Bangkok",
                extra_http_headers={"Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8"},
                proxy=proxy_config,
            )
            contexts.append(ctx)

        found_links = []
        seen = set()
        base_root = f"{urlparse(category_url).scheme}://{urlparse(category_url).netloc}"

        for page_no in range(1, pages + 1):
            if page_no == 1:
                page_url = category_url
            else:
                # oto uses /p{n}
                page_url = category_url.rstrip("/") + f"/p{page_no}"

            ctx = contexts[(page_no - 1) % len(contexts)]
            page = ctx.new_page()
            try:
                page.goto(page_url, wait_until="networkidle", timeout=NAVIGATE_TIMEOUT)
            except PlaywrightTimeoutError:
                page.close()
                page = ctx.new_page()
                alt = category_url + (
                    ("&" if "?" in category_url else "?") + f"page={page_no}"
                )
                try:
                    page.goto(alt, wait_until="networkidle", timeout=NAVIGATE_TIMEOUT)
                    page_url = alt
                except Exception as e_alt:
                    logging.warning(
                        "Could not load page %s (%s). Skipping.", page_url, e_alt
                    )
                    page.close()
                    continue
            except Exception as e:
                logging.warning("Error loading %s: %s", page_url, e)
                page.close()
                continue

            # get body text safely
            try:
                body_text = page.inner_text("body")
            except Exception:
                body_text = page.content()

            if "Hiện không có tin rao phù hợp" in (body_text or ""):
                logging.info("No listings on page %s", page_url)
                page.close()
                break

            # collect anchors (ElementHandle list)
            anchors = page.query_selector_all("a[href*='/mua-ban-xe-']")
            hrefs = []
            for a in anchors:
                try:
                    h = a.get_attribute("href") or ""
                except Exception:
                    h = ""
                if not h:
                    continue
                hrefs.append(h.split("?")[0])  # keep path part, remove query

            total_anchors = len(anchors)
            unique_hrefs = len(set(hrefs))
            logging.info(
                "length anchors with /mua-ban-xe-: %d (unique hrefs %d)",
                total_anchors,
                unique_hrefs,
            )

            # iterate unique hrefs (preserve order): use seen_hrefs to keep first-seen order
            new_links = []
            for href in dict.fromkeys(hrefs):  # ordered unique hrefs
                full = urljoin(base_root, href)
                # filter detail pages (only accept detail pattern)
                if not (DETAIL_HREF_RE.match(href) or DETAIL_FULLURL_RE.match(full)):
                    continue
                canonical = full.split("?")[0].rstrip("/")
                # skip duplicates gracefully
                if canonical in seen:
                    continue
                seen.add(canonical)
                found_links.append(canonical)
                new_links.append(canonical)
                logging.debug("found detail candidate: %s", canonical)

                # if we've reached the max_listings requested, stop collecting
                if max_listings and len(found_links) >= max_listings:
                    break

            logging.info(
                "Page %d (%s): found %d new links (total %d)",
                page_no,
                page_url,
                len(new_links),
                len(found_links),
            )
            if new_links:
                logging.debug("Sample new links: %s", new_links[:6])

            page.close()
            polite_sleep(sleep_min, sleep_max)

            if max_listings and len(found_links) >= max_listings:
                found_links = found_links[:max_listings]
                break

        # Crawl each listing (rotate contexts) via helper
        results = crawl_detail_links(
            found_links, contexts, out_json, sleep_min, sleep_max, max_listings
        )

        # cleanup contexts + browser
        for c in contexts:
            try:
                c.close()
            except Exception:
                pass
        browser.close()

        return results


# -------- CLI --------
def main():
    parser = argparse.ArgumentParser(description="Crawl oto.com.vn (Playwright)")
    sub = parser.add_subparsers(dest="mode", required=True)
    p_cat = sub.add_parser("category")
    p_cat.add_argument("category_url")
    p_cat.add_argument("--pages", type=int, default=3)
    p_cat.add_argument("--max-listings", type=int, default=200)
    p_cat.add_argument("--out", default="oto_results.json")
    p_cat.add_argument("--headless", action="store_true", default=True)
    p_cat.add_argument("--pool-size", type=int, default=DEFAULT_CONTEXT_POOL)
    p_cat.add_argument("--sleep-min", type=float, default=DEFAULT_SLEEP_MIN)
    p_cat.add_argument("--sleep-max", type=float, default=DEFAULT_SLEEP_MAX)
    p_cat.add_argument(
        "--proxies",
        type=str,
        default=None,
        help="comma-separated proxy urls (http://ip:port,...)",
    )
    p_cat.add_argument(
        "--no-robots", action="store_true", help="Don't check robots.txt"
    )

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )

    if args.mode == "category":
        crawl_category_playwright(
            args.category_url,
            pages=args.pages,
            max_listings=args.max_listings,
            out_json=args.out,
            headless=args.headless,
            pool_size=max(1, args.pool_size),
            sleep_min=args.sleep_min,
            sleep_max=args.sleep_max,
            proxies=args.proxies,
            check_robots_flag=(not args.no_robots),
        )


if __name__ == "__main__":
    main()
