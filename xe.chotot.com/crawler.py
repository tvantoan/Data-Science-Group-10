#!/usr/bin/env python3
"""
chotot_crawler.py

Crawl danh mục / tin rao trên xe.chotot.com (ví dụ:
https://xe.chotot.com/mua-ban-oto-cu-ha-noi-sdca1?mfdate=2022-%2A )

Features:
- Playwright (sync) for JS-rendered content + BeautifulSoup for parsing
- Infinite-scroll / load-more handling (cuộn và chờ load thêm)
- Chỉ thu detail links dạng .../mua-ban-oto-.../<id>.htm
- Dedupe bằng `seen` set; stop when `max_listings` reached
- Option --stop-link: nếu tìm thấy link này thì dừng collection và crawl ngay
- Robust parsing of specs: "Key Value", "Key: Value", "<label>Key:</label> Value"
- robots.txt read with encoding fallback
- rotate User-Agent, optional proxies, pool contexts
- Save JSON with ensure_ascii=False
"""

import argparse
import json
import logging
import random
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from tqdm import tqdm

# ---------- CONFIG ----------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
]

DEFAULT_SLEEP_MIN = 0.25
DEFAULT_SLEEP_MAX = 0.6
DEFAULT_CONTEXT_POOL = 2
NAVIGATE_TIMEOUT = 20000  # ms

PRICE_RE = re.compile(r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?\s*(?:đ|vnđ|vnd|đồng))", re.I)
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
KM_RE = re.compile(r"(\d[\d\.,]*\s*km)", re.I)

# detail link patterns (examples seen on site)
DETAIL_REL_RE = re.compile(r"^/mua-ban-oto-[^/]+/\d+\.htm(?:$|\?)", re.I)
DETAIL_FULL_RE = re.compile(
    r"^https?://(?:www\.)?xe\.chotot\.com/mua-ban-oto-[^/]+/\d+\.htm(?:$|\?)", re.I
)


# ---------- Helpers ----------
def polite_sleep(min_s=DEFAULT_SLEEP_MIN, max_s=DEFAULT_SLEEP_MAX):
    time.sleep(random.uniform(min_s, max_s))


def safe_check_robots(base_url, path="/"):
    """
    Read robots.txt with encoding fallback, parse with robotparser.
    Return (allowed: bool, robots_url: str).
    """
    from urllib import robotparser

    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = robotparser.RobotFileParser()
    try:
        # read manually to avoid UnicodeDecodeError
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        r = requests.get(robots_url, headers=headers, timeout=8)
        r.raise_for_status()
        raw = r.content
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            # fallback to latin-1 and ignore errors
            text = raw.decode("latin-1", errors="ignore")
        rp.parse(text.splitlines())
        allowed = rp.can_fetch("*", path)
        return allowed, robots_url
    except Exception as e:
        logging.warning("Could not read robots.txt (%s). Assuming allowed.", e)
        return True, robots_url


def normalize_model_name(raw_title: str) -> str:
    """
    Heuristics to pick a clean model name from raw title.
    Remove trailing '- price' style fragments; drop year/km tokens.
    """
    if not raw_title:
        return ""
    s = re.sub(r"[\n\r\t]+", " ", raw_title).strip()
    # split by '-' or '|' and prefer the leftmost chunk without price/year
    parts = [p.strip() for p in re.split(r"\s*[-|]\s*", s) if p.strip()]

    def is_noise(tok):
        if YEAR_RE.search(tok):
            return True
        if KM_RE.search(tok):
            return True
        if PRICE_RE.search(tok):
            return True
        return False

    meaningful = [p for p in parts if not is_noise(p)]
    if meaningful:
        # pick longest meaningful (usually contains brand + model)
        meaningful.sort(key=lambda x: (-len(x), x))
        return meaningful[0]
    # fallback: remove price-like suffix
    return re.sub(r"(-|\|).*$", "", s).strip()


def extract_price(text: str):
    if not text:
        return None
    m = PRICE_RE.search(text)
    if m:
        return m.group(1).strip()
    # also try forms like '610.000.000 đ' without trailing 'đ' captured
    m2 = re.search(r"(\d{1,3}(?:[.,]\d{3})+)\s*(đ|vnđ|vnd)?", text, re.I)
    return m2.group(0).strip() if m2 else None


def parse_specs_from_soup(soup: BeautifulSoup, page_text: str):
    """
    Flexible parser for Chotot listing "Thông số kỹ thuật" section.
    We look for the header text 'Thông số kỹ thuật' (or 'Thông số') and then
    parse the sibling block line-by-line.
    Also attempt to parse simple <li><label>... cases.
    Return dict of cleaned key -> value.
    """
    specs = {}

    # 1) Try to find structured list items like <li><label>Key:</label> Value</li>
    for li in soup.select("li"):
        # if li has a label tag, prefer that segmentation
        lbl = li.select_one("label")
        if lbl:
            key = lbl.get_text(" ", strip=True).rstrip(":").strip()
            # value might be the remaining text nodes after label
            val = ""
            # join all text nodes in li except label text
            texts = []
            for node in li.contents:
                if getattr(node, "name", None) == "label":
                    continue
                txt = getattr(node, "get_text", None)
                if callable(txt):
                    texts.append(node.get_text(" ", strip=True))
                else:
                    # plain NavigableString
                    t = str(node).strip()
                    if t:
                        texts.append(t)
            val = " ".join([t for t in texts if t]).strip()
            if key and val:
                specs[key] = val

    # 2) Find 'Thông số kỹ thuật' header and parse following block (lines)
    header = None
    for candidate in soup.find_all(["h2", "h3", "h4", "strong", "div"]):
        txt = candidate.get_text(" ", strip=True) if candidate else ""
        if txt and "Thông số" in txt:
            header = candidate
            break

    def parse_text_block(block_text: str):
        kv = {}
        lines = [ln.strip() for ln in block_text.splitlines() if ln.strip()]
        # known keys to match at line start
        known_keys = [
            "Hãng",
            "Dòng xe",
            "Năm sản xuất",
            "Hộp số",
            "Nhiên liệu",
            "Kiểu dáng",
            "Số chỗ",
            "Số Km",
            "Số Km đã đi",
            "Số Km đã đi",
            "Xuất xứ",
            "Tình trạng",
            "Số đời chủ",
            "Màu ngoại thất",
            "Màu nội thất",
        ]
        for ln in lines:
            # if line contains ':' split
            if ":" in ln:
                parts = [p.strip() for p in ln.split(":", 1)]
                if parts[0] and parts[1]:
                    kv[parts[0]] = parts[1]
                    continue
            # if line starts with known key
            matched = False
            for k in known_keys:
                if ln.lower().startswith(k.lower()):
                    val = ln[len(k) :].strip()
                    val = val.lstrip(":").strip()
                    kv[k] = val
                    matched = True
                    break
            if matched:
                continue
            # generic split: first run of two+ spaces OR split on last token if last token numeric/endswith km or ø
            m_space = re.match(r"^(.+?)\s{2,}(.+)$", ln)
            if m_space:
                kk = m_space.group(1).strip()
                vv = m_space.group(2).strip()
                kv[kk] = vv
                continue
            # fallback: split at last space if right side looks like value (number, km, word)
            m = re.match(r"^(.+?)\s+([^\s]+)$", ln)
            if m:
                kk = m.group(1).strip()
                vv = m.group(2).strip()
                # small heuristic: if kk short (<30 chars) treat as key
                if len(kk) <= 50 and len(vv) <= 50:
                    kv[kk] = vv
                    continue
            # otherwise put into 'Thông số khác'
            kv.setdefault("Thông số khác", []).append(ln)
        if "Thông số khác" in kv:
            kv["Thông số khác"] = "; ".join(kv["Thông số khác"])
        return kv

    if header:
        # often the specs are in the next sibling or within the parent
        block = header.find_next_sibling()
        if block:
            # collect text from several siblings up to a reasonable boundary
            texts = []
            # gather next 6 siblings or until another H2/H3
            cur = block
            count = 0
            while cur and count < 10:
                txt = cur.get_text(" ", strip=True)
                if txt:
                    texts.append(txt)
                # stop if next sibling is a new major section
                nxt = cur.find_next_sibling()
                if nxt and nxt.name and nxt.name.lower() in ["h2", "h3"]:
                    break
                cur = nxt
                count += 1
            if texts:
                block_text = "\n".join(texts)
                specs.update(parse_text_block(block_text))

    # 3) fallback scanning page_text by regex for common fields if not present
    if not specs:
        candidates = {
            "Năm sản xuất": r"(Năm sản xuất[:\s]*|Năm[:\s]*)(\d{4})",
            "Số Km đã đi": r"(\d[\d\.,]*\s*km)",
            "Nhiên liệu": r"(Xăng|Dầu|Điện|Hybrid|Nhiên liệu[:\s]*[^\n,;]+)",
            "Hộp số": r"(Tự động|Số tự động|Số sàn|Hộp số[:\s]*[^\n,;]+)",
            "Tình trạng": r"(Đã sử dụng|Mới|Tình trạng[:\s]*[^\n,;]+)",
        }
        for k, pat in candidates.items():
            m = re.search(pat, page_text, re.I)
            if m:
                val = (
                    m.group(2) if m.lastindex and m.lastindex >= 2 else m.group(0)
                ).strip()
                specs[k] = val

    # clean: trim label words accidentally included
    clean_specs = {}
    for k, v in specs.items():
        if isinstance(v, str):
            vv = v.replace("\n", " ").strip()
            # remove label prefix if repeated like "Nhiên liệu: Xăng"
            vv = re.sub(r"^[^\w]{0,3}[^:]{1,60}:\s*", "", vv)
            clean_specs[k.strip()] = vv
        else:
            clean_specs[k.strip()] = v
    return clean_specs


def parse_listing_page(html: str, url: str):
    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text("\n")

    # title
    title_tag = soup.find(["h1", "h2"]) or soup.find("title")
    raw_title = title_tag.get_text(" ", strip=True) if title_tag else ""
    title = normalize_model_name(raw_title)

    # price - try top area and page text
    price = None
    # some pages present price near top in a tag with price; search price regex on first 2k chars
    price = extract_price(page_text[:3000]) or extract_price(raw_title)

    # id from url like .../127960577.htm
    m = re.search(r"/(\d+)\.htm", url)
    listing_id = m.group(1) if m else None

    # posted date rough
    mdate = re.search(r"Đăng\s*([^\n\r]+)", page_text)
    posted = mdate.group(1).strip() if mdate else None

    # parse specs
    specs = parse_specs_from_soup(soup, page_text)

    # try to fill top quick info if missing
    # top 5 small stat items often at top: year, km, fuel, gearbox, owners
    # find short blocks at top - first few numeric tokens
    if not specs.get("Năm sản xuất"):
        ym = YEAR_RE.search(page_text[:600])
        if ym:
            specs.setdefault("Năm sản xuất", ym.group(0))
    if not specs.get("Số Km đã đi"):
        km = KM_RE.search(page_text[:600])
        if km:
            specs.setdefault("Số Km đã đi", km.group(1))

    # images: collect image urls from gallery
    imgs = []
    for img in soup.find_all("img"):
        src = img.get("data-src") or img.get("src") or ""
        if src and src.startswith("http"):
            imgs.append(src)
    imgs = list(dict.fromkeys(imgs))

    return {
        "url": url,
        "id": listing_id,
        "raw_title": raw_title,
        "title": title,
        "price": price,
        "posted_date": posted,
        "specs": specs,
        "images": imgs,
    }


# ---------- Crawler (Playwright sync) ----------
def crawl_chotot_category(
    category_url,
    pages=5,
    max_listings=200,
    out_json="chotot_results.json",
    headless=True,
    pool_size=DEFAULT_CONTEXT_POOL,
    sleep_min=DEFAULT_SLEEP_MIN,
    sleep_max=DEFAULT_SLEEP_MAX,
    proxies=None,
    check_robots_flag=True,
    stop_link=None,
):
    allowed, robots_url = safe_check_robots(category_url)
    if check_robots_flag and not allowed:
        logging.warning(
            "robots.txt (%s) may disallow crawling %s — exiting.",
            robots_url,
            category_url,
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
                p = proxies_list[i % len(proxies_list)].strip()
                if p:
                    proxy_config = {"server": p}
            ctx = browser.new_context(
                user_agent=ua,
                locale="vi-VN",
                timezone_id="Asia/Bangkok",
                extra_http_headers={"Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8"},
                proxy=proxy_config,
            )
            contexts.append(ctx)

        # use first context to collect links (we'll rotate pages to speed)
        ctx = contexts[0]
        page = ctx.new_page()
        try:
            logging.info("Loading category page: %s", category_url)
            page.goto(category_url, wait_until="networkidle", timeout=NAVIGATE_TIMEOUT)
        except Exception as e:
            logging.warning("Could not load category %s: %s", category_url, e)
            page.close()
            for c in contexts:
                try:
                    c.close()
                except Exception:
                    pass
            browser.close()
            return []

        seen = set()
        found_links = []

        # Infinite-scroll / load-more loop: repeat 'pages' times (heuristic)
        no_new_iters = 0
        for iter_idx in range(1, pages + 1):
            # collect anchors that look like detail pages
            try:
                body_text = page.inner_text("body")
            except Exception:
                body_text = page.content()
            # find anchors that contain '/mua-ban-oto-'
            anchors = page.query_selector_all("a[href*='/mua-ban-oto-']")
            new_links = []
            for a in anchors:
                try:
                    href = a.get_attribute("href") or ""
                except Exception:
                    href = ""
                if not href:
                    continue
                full = urljoin(
                    f"{urlparse(category_url).scheme}://{urlparse(category_url).netloc}",
                    href.split("?")[0],
                )
                # accept only actual detail pages with id .htm
                if not (DETAIL_REL_RE.match(href) or DETAIL_FULL_RE.match(full)):
                    continue
                canonical = full.rstrip("/")
                if canonical not in seen:
                    seen.add(canonical)
                    found_links.append(canonical)
                    new_links.append(canonical)
                    # if stop_link provided and matched, stop collection immediately
                    if stop_link and (
                        canonical == stop_link or canonical.split("?")[0] == stop_link
                    ):
                        logging.info(
                            "Found stop_link %s -> stop collecting further links.",
                            stop_link,
                        )
                        break
            logging.info(
                "Iter %d: found %d new links (total %d)",
                iter_idx,
                len(new_links),
                len(found_links),
            )
            if new_links:
                logging.debug("Sample new links: %s", new_links[:5])
                no_new_iters = 0
            else:
                no_new_iters += 1

            # if stop_link found, break outer loop
            if stop_link and any(
                (stop_link in nl) or (nl in stop_link) for nl in new_links
            ):
                break

            if max_listings and len(found_links) >= max_listings:
                found_links = found_links[:max_listings]
                break

            # scroll to bottom to let site load more items (infinite scroll)
            last_height = page.evaluate("() => document.body.scrollHeight")
            page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            # wait for a bit for new content to load
            polite_sleep(sleep_min, sleep_max)
            # wait short extra to let JS append items
            try:
                page.wait_for_timeout(1000)
            except Exception:
                pass
            # if no new items in several iterations, stop
            if no_new_iters >= 3:
                logging.info(
                    "No new links after %d iterations -> stop scrolling.", no_new_iters
                )
                break

        # close collecting page
        try:
            page.close()
        except Exception:
            pass

        # if max_listings limit
        if max_listings:
            found_links = found_links[:max_listings]

        logging.info(
            "Collected %d listing links, begin crawling details...", len(found_links)
        )

        # Crawl each listing rotating contexts
        results = []
        for idx, link in enumerate(tqdm(found_links, desc="Crawling listings")):
            ctx = contexts[(idx) % len(contexts)]
            p = ctx.new_page()
            try:
                p.goto(link, wait_until="networkidle", timeout=NAVIGATE_TIMEOUT)
                try:
                    body_text = p.inner_text("body")
                except Exception:
                    body_text = p.content()
                # detect simple block/captcha
                if "Nhập mã xác nhận" in (
                    body_text or ""
                ) or "Bạn đã submit quá nhiều lần" in (body_text or ""):
                    logging.warning(
                        "Blocked on %s (captcha or rate limit). Skipping; consider proxies.",
                        link,
                    )
                    p.close()
                    polite_sleep(sleep_min * 3, sleep_max * 3)
                    continue
                html = p.content()
                data = parse_listing_page(html, link)
                results.append(data)
            except PlaywrightTimeoutError:
                logging.warning("Timeout loading %s", link)
            except Exception as e:
                logging.warning("Error crawling %s: %s", link, e)
            finally:
                try:
                    p.close()
                except Exception:
                    pass
            polite_sleep(sleep_min, sleep_max)

        # cleanup
        for c in contexts:
            try:
                c.close()
            except Exception:
                pass
        browser.close()

        # save
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logging.info("Saved %d listings to %s", len(results), out_json)
        return results


# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(
        description="Crawl xe.chotot.com listing pages (Playwright)"
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    p_cat = sub.add_parser("category")
    p_cat.add_argument(
        "category_url", help="category url e.g. https://xe.chotot.com/...."
    )
    p_cat.add_argument(
        "--pages", type=int, default=8, help="max scroll iterations (heuristic)"
    )
    p_cat.add_argument("--max-listings", type=int, default=200)
    p_cat.add_argument("--out", default="chotot_results.json")
    p_cat.add_argument("--headless", action="store_true", default=True)
    p_cat.add_argument("--pool-size", type=int, default=DEFAULT_CONTEXT_POOL)
    p_cat.add_argument("--sleep-min", type=float, default=DEFAULT_SLEEP_MIN)
    p_cat.add_argument("--sleep-max", type=float, default=DEFAULT_SLEEP_MAX)
    p_cat.add_argument(
        "--proxies",
        type=str,
        default=None,
        help="comma-separated proxy urls (http://ip:port,...).",
    )
    p_cat.add_argument(
        "--no-robots", action="store_true", help="Don't check robots.txt"
    )
    p_cat.add_argument(
        "--stop-link",
        type=str,
        default=None,
        help="If found this exact link while collecting, stop collecting and crawl immediately",
    )

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )

    if args.mode == "category":
        crawl_chotot_category(
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
            stop_link=args.stop_link,
        )


if __name__ == "__main__":
    main()
