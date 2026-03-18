import argparse
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


RBC_BASE_URL = "https://www.rbc.ru/short_news/"
RBC_API_URL = "https://www.rbc.ru/api/search/v1/search/ajax-cc"
RIA_BASE_URL = "https://ria.ru/"
DZEN_BASE_URL = "https://dzen.ru/news"
DZEN_API_URL = "https://dzen.ru/api/v3/launcher/more?news=1"
LENTA_BASE_URL = "https://lenta.ru/"
TPROGER_NEWS_BASE_URL = "https://tproger.ru/news"
REN_NEWS_BASE_URL = "https://ren.tv/news"
MK_NEWS_BASE_URL = "https://www.mk.ru/news/"
M24_NEWS_BASE_URL = "https://www.m24.ru/news"
GAZETA_NEWS_BASE_URL = "https://www.gazeta.ru/news/"
GAZETA_NEWS_SITEMAP_URL = "https://www.gazeta.ru/sitemap_news.xml"
RBC_RSSHUB_FEEDS = [
    "https://rsshub.app/rbc/short_news",
    "https://rsshub.rssforever.com/rbc/short_news",
    "https://rsshub.pseudoyu.com/rbc/short_news",
    "https://rsshub.feeded.xyz/rbc/short_news",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


@dataclass
class NewsItem:
    title: str
    url: str
    content: str
    source: str
    published_at: str = ""


def is_deadline_reached(deadline_ts: float | None) -> bool:
    return bool(deadline_ts is not None and time.time() >= deadline_ts)


def setup_logging(level: str) -> Path:
    logs_dir = Path(__file__).resolve().parents[1] / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "latest.log"

    logger = logging.getLogger()
    logger.setLevel(level.upper())
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return log_file


def fetch_html(session: requests.Session, url: str, source_tag: str) -> str:
    logging.info("[%s] HTTP GET: %s", source_tag, url)
    response = session.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    logging.info("[%s] HTTP %s, bytes=%s", source_tag, response.status_code, len(response.text))
    return response.text


def fetch_json(session: requests.Session, url: str, params: dict[str, str], source_tag: str) -> dict:
    logging.info("[%s] HTTP GET JSON: %s params=%s", source_tag, url, params)
    response = session.get(url, headers=HEADERS, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    logging.info("[%s] HTTP %s JSON keys=%s", source_tag, response.status_code, list(payload.keys()))
    return payload


def fetch_rbc_json_with_retries(
    session: requests.Session,
    *,
    params: dict[str, str],
    cookie: str = "",
    referer: str = RBC_BASE_URL,
    retries: int = 3,
) -> dict:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            headers = {
                **HEADERS,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                "Referer": referer or RBC_BASE_URL,
                "Origin": "https://www.rbc.ru",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
            if cookie:
                headers["Cookie"] = cookie
            logging.info("[RBC] API attempt %s/%s params=%s", attempt, retries, params)
            response = session.get(RBC_API_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            return payload
        except Exception as exc:
            last_exc = exc
            logging.warning("[RBC] API attempt %s failed: %s", attempt, exc)
            try:
                # Session warmup sometimes helps with anti-bot edge cases.
                warmup_headers = {**HEADERS, "Referer": referer or RBC_BASE_URL}
                if cookie:
                    warmup_headers["Cookie"] = cookie
                _ = session.get(RBC_BASE_URL, headers=warmup_headers, timeout=20)
            except Exception:
                pass
    if last_exc:
        raise last_exc
    raise RuntimeError("RBC API retries exhausted")


def parse_publish_datetime(value: str) -> datetime | None:
    if not value:
        return None
    raw = value.strip().replace("UTC", "+00:00").replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def extract_article_text(html: str) -> tuple[str, str, str]:
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    h1 = soup.select_one("h1")
    if h1:
        title = h1.get_text(" ", strip=True)
    if not title and soup.title:
        title = soup.title.get_text(" ", strip=True)
    if not title:
        og_title = soup.select_one("meta[property='og:title']")
        title = (og_title.get("content", "") if og_title else "").strip()

    published_at = ""
    pub_meta = (
        soup.select_one("meta[property='article:published_time']")
        or soup.select_one("meta[name='article:published_time']")
        or soup.select_one("meta[property='og:published_time']")
        or soup.select_one("time[datetime]")
    )
    if pub_meta:
        published_at = (pub_meta.get("content") or pub_meta.get("datetime") or "").strip()

    selectors = [
        "p.paragraph",
        "article p",
        ".article__text p",
        ".article__content p",
        ".article__body p",
        ".layout-article p",
    ]
    chunks: list[str] = []
    for selector in selectors:
        nodes = soup.select(selector)
        for node in nodes:
            text = node.get_text(" ", strip=True)
            if len(text) >= 40:
                chunks.append(text)
        if chunks:
            break

    content = "\n\n".join(chunks).strip()
    if not content:
        desc = soup.select_one("meta[name='description']")
        content = ((desc.get("content") if desc else "") or "").strip()

    return title, content, published_at


def normalize_datetime_or_now(value: str, fallback_url: str = "") -> str:
    dt = parse_publish_datetime(value)
    if dt:
        return dt.isoformat()
    if fallback_url:
        m = re.search(r"/(\d{2})/(\d{2})/(\d{4})/", fallback_url)
        if m:
            day, month, year = m.groups()
            try:
                return datetime(int(year), int(month), int(day), tzinfo=timezone.utc).isoformat()
            except ValueError:
                pass
    return datetime.now(timezone.utc).isoformat()


def is_rbc_short_news_url(url: str) -> bool:
    low = (url or "").strip().lower()
    return bool(low) and (
        "from=short_news" in low
        or "/rbcfreenews/" in low
        or "/short_news/" in low
    )


def extract_text_from_html_fragment(value: str) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    return soup.get_text(" ", strip=True)


def is_rbc_short_news_item(item: dict) -> bool:
    # type=short_news is the most stable signal in RBC API response.
    return str(item.get("type") or "").strip().lower() == "short_news"


def fetch_rbc_short_news_api_items(
    session: requests.Session,
    *,
    hours: int,
    max_pages: int,
    max_items: int,
    cookie: str = "",
    referer: str = RBC_BASE_URL,
    deadline_ts: float | None = None,
) -> tuple[list[dict], dict]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)

    cursor: str | None = None
    collected: list[dict] = []
    seen_urls: set[str] = set()

    stats = {
        "pages_visited": 0,
        "items_seen": 0,
        "items_recent": 0,
        "items_old_filtered": 0,
        "items_not_short_news_filtered": 0,
        "items_no_url": 0,
        "duplicates_filtered": 0,
        "more_exists_last": False,
        "cursor_last": "",
        "api_ok": False,
    }

    for page in range(1, max_pages + 1):
        if is_deadline_reached(deadline_ts):
            logging.warning("[RBC] Stop by runtime limit in API fetch.")
            break
        params: dict[str, str] = {}
        if cursor:
            params["endCursor"] = cursor

        try:
            payload = fetch_rbc_json_with_retries(
                session,
                params=params,
                cookie=cookie,
                referer=referer,
                retries=3,
            )
            stats["api_ok"] = True
        except Exception as exc:
            logging.warning("[RBC] API request failed on page %s: %s", page, exc)
            break

        items = payload.get("items", []) or []
        more_exists = bool(payload.get("moreExists"))
        cursor = payload.get("endCursor") or ""

        stats["pages_visited"] += 1
        stats["items_seen"] += len(items)
        stats["more_exists_last"] = more_exists
        stats["cursor_last"] = cursor

        if not items:
            break

        page_recent = 0
        page_old = 0
        for it in items:
            if is_deadline_reached(deadline_ts):
                logging.warning("[RBC] Stop by runtime limit while processing API items.")
                break
            if len(collected) >= max_items:
                break

            if not is_rbc_short_news_item(it):
                stats["items_not_short_news_filtered"] += 1
                continue

            pub_raw = (it.get("publishDate") or "").strip()
            pub_dt = parse_publish_datetime(pub_raw) or now
            if pub_dt < cutoff:
                page_old += 1
                stats["items_old_filtered"] += 1
                continue

            url = (it.get("fronturl") or "").strip()
            if not url:
                stats["items_no_url"] += 1
                continue
            if url.startswith("/"):
                url = urljoin("https://www.rbc.ru", url)

            if url in seen_urls:
                stats["duplicates_filtered"] += 1
                continue
            seen_urls.add(url)

            title = (it.get("title") or "").strip() or (it.get("header") or "").strip()
            if not title:
                title = url.rsplit("/", 1)[-1]

            collected.append(
                {
                    "title": title,
                    "url": url,
                    "publishDate": pub_dt.isoformat(),
                    "body": (it.get("body") or "").strip(),
                }
            )
            page_recent += 1
            stats["items_recent"] += 1

        if len(collected) >= max_items:
            break
        if page_recent == 0 and page_old > 0:
            break
        if not more_exists:
            break

    return collected, stats


def fetch_rbc_short_news_rsshub_items(
    session: requests.Session,
    *,
    hours: int,
    max_items: int,
    feed_urls: list[str],
    deadline_ts: float | None = None,
) -> tuple[list[dict], dict]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    collected: list[dict] = []
    seen_urls: set[str] = set()

    stats = {
        "feeds_total": len(feed_urls),
        "feeds_ok": 0,
        "feeds_failed": 0,
        "feeds_forbidden": 0,
        "feeds_not_xml": 0,
        "items_seen": 0,
        "items_recent": 0,
        "items_old_filtered": 0,
        "items_non_short_filtered": 0,
        "items_no_link": 0,
        "duplicates_filtered": 0,
    }

    for feed_url in feed_urls:
        if is_deadline_reached(deadline_ts):
            logging.warning("[RBC] Stop by runtime limit in RSS fallback.")
            break
        if len(collected) >= max_items:
            break
        try:
            logging.info("[RBC-RSS] Probe feed: %s", feed_url)
            response = session.get(feed_url, headers=HEADERS, timeout=20)
            status_code = response.status_code
            content_type = (response.headers.get("content-type") or "").lower()
            if status_code in (401, 403):
                stats["feeds_failed"] += 1
                stats["feeds_forbidden"] += 1
                logging.warning("[RBC] RSSHub forbidden (%s): %s", status_code, feed_url)
                continue
            response.raise_for_status()
            xml_text = response.text
            if "xml" not in content_type and "<rss" not in xml_text[:400].lower():
                stats["feeds_failed"] += 1
                stats["feeds_not_xml"] += 1
                logging.warning("[RBC] RSSHub returned non-XML content: %s (%s)", feed_url, content_type)
                continue
            stats["feeds_ok"] += 1
        except Exception as exc:
            stats["feeds_failed"] += 1
            logging.warning("[RBC] RSSHub feed unavailable: %s (%s)", feed_url, exc)
            continue

        try:
            root = ET.fromstring(xml_text)
        except Exception as exc:
            stats["feeds_failed"] += 1
            logging.warning("[RBC] RSS XML parse failed: %s (%s)", feed_url, exc)
            continue

        feed_items = root.findall(".//item")
        stats["items_seen"] += len(feed_items)

        for item in feed_items:
            if is_deadline_reached(deadline_ts):
                logging.warning("[RBC] Stop by runtime limit while processing RSS items.")
                break
            if len(collected) >= max_items:
                break

            link = (item.findtext("link") or "").strip()
            if not link:
                stats["items_no_link"] += 1
                continue
            if not is_rbc_short_news_url(link):
                stats["items_non_short_filtered"] += 1
                continue

            pub_raw = (item.findtext("pubDate") or "").strip()
            pub_dt = parse_publish_datetime(pub_raw)
            if not pub_dt:
                try:
                    pub_dt = parsedate_to_datetime(pub_raw)
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pub_dt = now
            if pub_dt < cutoff:
                stats["items_old_filtered"] += 1
                continue

            if link in seen_urls:
                stats["duplicates_filtered"] += 1
                continue
            seen_urls.add(link)

            title = (item.findtext("title") or "").strip()
            if not title:
                title = link.rsplit("/", 1)[-1]

            desc = extract_text_from_html_fragment((item.findtext("description") or "").strip())

            collected.append(
                {
                    "title": title,
                    "url": link,
                    "publishDate": pub_dt.isoformat(),
                    "body": desc,
                }
            )
            stats["items_recent"] += 1

    return collected, stats


def parse_rbc_source(
    *,
    hours: int,
    max_pages: int,
    max_items: int,
    fulltext_limit: int,
    feed_urls: list[str] | None = None,
    cookie: str = "",
    referer: str = RBC_BASE_URL,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    session = requests.Session()
    stats = {
        "entry_ok": False,
        "links_found": 0,
        "articles_success": 0,
        "articles_failed": 0,
        "fulltext_attempted": 0,
        "fulltext_skipped": 0,
        "api": {},
        "rss": {},
    }

    records, api_stats = fetch_rbc_short_news_api_items(
        session,
        hours=hours,
        max_pages=max_pages,
        max_items=max_items,
        cookie=cookie,
        referer=referer,
        deadline_ts=deadline_ts,
    )
    stats["api"] = api_stats
    stats["entry_ok"] = bool(api_stats.get("api_ok"))

    effective_feeds = [u.strip() for u in (feed_urls or RBC_RSSHUB_FEEDS) if u.strip()]
    if not records:
        records, rss_stats = fetch_rbc_short_news_rsshub_items(
            session,
            hours=hours,
            max_items=max_items,
            feed_urls=effective_feeds,
            deadline_ts=deadline_ts,
        )
        stats["rss"] = rss_stats
        stats["entry_ok"] = stats["entry_ok"] or bool(rss_stats.get("feeds_ok"))
    else:
        stats["rss"] = {
            "feeds_total": len(effective_feeds),
            "feeds_ok": 0,
            "feeds_failed": 0,
            "feeds_forbidden": 0,
            "feeds_not_xml": 0,
            "items_seen": 0,
            "items_recent": 0,
            "items_old_filtered": 0,
            "items_non_short_filtered": 0,
            "items_no_link": 0,
            "duplicates_filtered": 0,
            "note": "rss_fallback_not_needed",
        }

    stats["links_found"] = len(records)
    logging.info("[RBC] Collected records in %sh window: %s", hours, len(records))

    out: list[NewsItem] = []
    for idx, rec in enumerate(records, start=1):
        if is_deadline_reached(deadline_ts):
            logging.warning("[RBC] Stop by runtime limit while parsing articles.")
            break
        link = rec["url"]
        if idx > max(0, fulltext_limit):
            stats["fulltext_skipped"] += 1
            out.append(
                NewsItem(
                    title=rec.get("title") or link.rsplit("/", 1)[-1],
                    url=link,
                    content=rec.get("body") or rec.get("title") or link,
                    source="RBC short_news",
                    published_at=normalize_datetime_or_now(rec.get("publishDate", ""), link),
                )
            )
            stats["articles_success"] += 1
            continue

        try:
            stats["fulltext_attempted"] += 1
            html = fetch_html(session, link, "RBC")
            title, content, page_dt = extract_article_text(html)
            if not title:
                title = rec.get("title") or link.rsplit("/", 1)[-1]
            if not content:
                content = rec.get("body") or title
            out.append(
                NewsItem(
                    title=title,
                    url=link,
                    content=content,
                    source="RBC short_news",
                    published_at=normalize_datetime_or_now(page_dt or rec.get("publishDate", ""), link),
                )
            )
            stats["articles_success"] += 1
        except Exception as exc:
            stats["articles_failed"] += 1
            logging.exception("[RBC] Article parse failed: %s (%s)", link, exc)

    return out, stats


def extract_ria_main_links(html: str, max_items: int) -> tuple[list[str], dict]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    seen: set[str] = set()

    stats = {
        "anchors_total": 0,
        "filtered_external": 0,
        "filtered_non_article": 0,
        "filtered_duplicates": 0,
    }

    for a in soup.select("a[href]"):
        stats["anchors_total"] += 1
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(RIA_BASE_URL, href)
        parsed = urlparse(full)
        if not parsed.netloc.endswith("ria.ru"):
            stats["filtered_external"] += 1
            continue

        path = parsed.path.lower()
        # Только ссылки материалов с главной (обычно /YYYYMMDD/...).
        is_article = bool(re.search(r"/20\d{6}/", path))
        if not is_article:
            stats["filtered_non_article"] += 1
            continue

        clean = full.split("#", 1)[0]
        if clean in seen:
            stats["filtered_duplicates"] += 1
            continue
        seen.add(clean)
        links.append(clean)
        if len(links) >= max_items:
            break

    return links, stats


def parse_ria_source(
    *,
    max_items: int,
    fulltext_limit: int,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    session = requests.Session()
    stats = {
        "entry_ok": False,
        "links_found": 0,
        "articles_success": 0,
        "articles_failed": 0,
        "fulltext_attempted": 0,
        "fulltext_skipped": 0,
        "link_extract": {},
    }

    main_html = fetch_html(session, RIA_BASE_URL, "RIA")
    stats["entry_ok"] = True

    links, link_stats = extract_ria_main_links(main_html, max_items=max_items)
    stats["links_found"] = len(links)
    stats["link_extract"] = link_stats
    logging.info("[RIA] Extracted links from main page: %s", len(links))

    out: list[NewsItem] = []
    for idx, link in enumerate(links, start=1):
        if is_deadline_reached(deadline_ts):
            logging.warning("[RIA] Stop by runtime limit while parsing articles.")
            break
        if idx > max(0, fulltext_limit):
            stats["fulltext_skipped"] += 1
            out.append(
                NewsItem(
                    title=link.rsplit("/", 1)[-1],
                    url=link,
                    content=link,
                    source="RIA main",
                    published_at=normalize_datetime_or_now("", link),
                )
            )
            stats["articles_success"] += 1
            continue

        try:
            stats["fulltext_attempted"] += 1
            html = fetch_html(session, link, "RIA")
            title, content, page_dt = extract_article_text(html)
            if not title:
                title = link.rsplit("/", 1)[-1]
            if not content:
                content = title
            out.append(
                NewsItem(
                    title=title,
                    url=link,
                    content=content,
                    source="RIA main",
                    published_at=normalize_datetime_or_now(page_dt, link),
                )
            )
            stats["articles_success"] += 1
        except Exception as exc:
            stats["articles_failed"] += 1
            logging.exception("[RIA] Article parse failed: %s (%s)", link, exc)

    return out, stats


def extract_links_by_patterns(
    *,
    html: str,
    base_url: str,
    max_items: int,
    include_patterns: list[str],
    exclude_patterns: list[str] | None = None,
) -> tuple[list[str], dict]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    seen: set[str] = set()
    compiled_includes = [re.compile(p, re.IGNORECASE) for p in include_patterns]
    compiled_excludes = [re.compile(p, re.IGNORECASE) for p in (exclude_patterns or [])]

    base_host = urlparse(base_url).netloc.lower()
    if base_host.startswith("www."):
        base_host = base_host[4:]

    stats = {
        "anchors_total": 0,
        "filtered_external": 0,
        "filtered_non_article": 0,
        "filtered_duplicates": 0,
    }

    for a in soup.select("a[href]"):
        stats["anchors_total"] += 1
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(base_url, href).split("#", 1)[0]
        parsed = urlparse(full)
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if not host.endswith(base_host):
            stats["filtered_external"] += 1
            continue

        path = parsed.path or "/"
        if not any(rx.search(path) for rx in compiled_includes):
            stats["filtered_non_article"] += 1
            continue
        if any(rx.search(path) for rx in compiled_excludes):
            stats["filtered_non_article"] += 1
            continue

        clean = full
        if clean in seen:
            stats["filtered_duplicates"] += 1
            continue
        seen.add(clean)
        links.append(clean)
        if len(links) >= max_items:
            break

    return links, stats


def parse_generic_source(
    *,
    source_tag: str,
    source_name: str,
    base_url: str,
    max_items: int,
    fulltext_limit: int,
    include_patterns: list[str],
    exclude_patterns: list[str] | None = None,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    session = requests.Session()
    stats = {
        "entry_ok": False,
        "links_found": 0,
        "articles_success": 0,
        "articles_failed": 0,
        "fulltext_attempted": 0,
        "fulltext_skipped": 0,
        "link_extract": {},
    }

    main_html = fetch_html(session, base_url, source_tag)
    stats["entry_ok"] = True

    links, link_stats = extract_links_by_patterns(
        html=main_html,
        base_url=base_url,
        max_items=max_items,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    )
    stats["links_found"] = len(links)
    stats["link_extract"] = link_stats
    logging.info("[%s] Extracted links from main page: %s", source_tag, len(links))

    out: list[NewsItem] = []
    for idx, link in enumerate(links, start=1):
        if is_deadline_reached(deadline_ts):
            logging.warning("[%s] Stop by runtime limit while parsing articles.", source_tag)
            break
        if idx > max(0, fulltext_limit):
            stats["fulltext_skipped"] += 1
            out.append(
                NewsItem(
                    title=link.rsplit("/", 1)[-1],
                    url=link,
                    content=link,
                    source=source_name,
                    published_at=normalize_datetime_or_now("", link),
                )
            )
            stats["articles_success"] += 1
            continue

        try:
            stats["fulltext_attempted"] += 1
            html = fetch_html(session, link, source_tag)
            title, content, page_dt = extract_article_text(html)
            if not title:
                title = link.rsplit("/", 1)[-1]
            if not content:
                content = title
            out.append(
                NewsItem(
                    title=title,
                    url=link,
                    content=content,
                    source=source_name,
                    published_at=normalize_datetime_or_now(page_dt, link),
                )
            )
            stats["articles_success"] += 1
        except Exception as exc:
            stats["articles_failed"] += 1
            logging.exception("[%s] Article parse failed: %s (%s)", source_tag, link, exc)

    return out, stats


def fetch_gazeta_news_sitemap_items(
    session: requests.Session,
    *,
    max_items: int,
    deadline_ts: float | None = None,
) -> tuple[list[dict], dict]:
    stats = {
        "sitemap_ok": False,
        "urls_seen": 0,
        "urls_added": 0,
        "urls_non_news_filtered": 0,
        "duplicates_filtered": 0,
    }
    records: list[dict] = []
    seen: set[str] = set()

    try:
        xml_text = fetch_html(session, GAZETA_NEWS_SITEMAP_URL, "GAZETA-SITEMAP")
        root = ET.fromstring(xml_text)
        stats["sitemap_ok"] = True
    except Exception as exc:
        stats["error"] = str(exc)
        return [], stats

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    for node in root.findall("sm:url", ns):
        if is_deadline_reached(deadline_ts):
            break
        if len(records) >= max_items:
            break

        loc = (node.findtext("sm:loc", default="", namespaces=ns) or "").strip()
        if not loc:
            continue
        stats["urls_seen"] += 1

        parsed = urlparse(loc)
        path = parsed.path or "/"
        if not re.search(r"^/(?:[^/]+/)*news/\d{4}/\d{2}/\d{2}/\d+\.shtml$", path, re.IGNORECASE):
            stats["urls_non_news_filtered"] += 1
            continue
        if loc in seen:
            stats["duplicates_filtered"] += 1
            continue

        seen.add(loc)
        lastmod = (node.findtext("sm:lastmod", default="", namespaces=ns) or "").strip()
        records.append(
            {
                "title": loc.rsplit("/", 1)[-1],
                "url": loc,
                "publishDate": lastmod,
                "body": "",
            }
        )
        stats["urls_added"] += 1

    return records, stats


def fetch_dzen_news_api_items(
    session: requests.Session,
    *,
    hours: int,
    max_pages: int,
    max_items: int,
    deadline_ts: float | None = None,
) -> tuple[list[dict], dict]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    next_url = DZEN_API_URL
    collected: list[dict] = []
    seen_urls: set[str] = set()

    stats = {
        "pages_visited": 0,
        "items_seen": 0,
        "items_recent": 0,
        "items_old_filtered": 0,
        "items_non_card_filtered": 0,
        "items_no_url": 0,
        "duplicates_filtered": 0,
        "api_ok": False,
    }

    for _page in range(1, max_pages + 1):
        if is_deadline_reached(deadline_ts):
            logging.warning("[DZEN] Stop by runtime limit in API fetch.")
            break
        if not next_url or len(collected) >= max_items:
            break
        try:
            logging.info("[DZEN] HTTP GET JSON: %s", next_url)
            resp = session.get(next_url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            stats["api_ok"] = True
        except Exception as exc:
            logging.warning("[DZEN] API request failed: %s", exc)
            break

        stats["pages_visited"] += 1
        items = payload.get("items", []) or []
        stats["items_seen"] += len(items)
        next_url = (payload.get("more") or {}).get("link") or ""
        if not items:
            break

        page_recent = 0
        page_old = 0
        for it in items:
            if is_deadline_reached(deadline_ts):
                logging.warning("[DZEN] Stop by runtime limit while processing API items.")
                break
            if len(collected) >= max_items:
                break
            if str(it.get("type") or "").strip().lower() != "card":
                stats["items_non_card_filtered"] += 1
                continue

            raw_link = (it.get("ext_link") or it.get("link") or "").strip()
            if not raw_link:
                stats["items_no_url"] += 1
                continue
            # Remove tracking params, keep stable dedupe key.
            url = raw_link.split("?", 1)[0]
            if url in seen_urls:
                stats["duplicates_filtered"] += 1
                continue

            pub_value = it.get("publication_date")
            pub_dt: datetime | None = None
            if isinstance(pub_value, (int, float)):
                try:
                    pub_dt = datetime.fromtimestamp(float(pub_value), tz=timezone.utc)
                except Exception:
                    pub_dt = None
            if not pub_dt:
                pub_dt = parse_publish_datetime(str(pub_value or ""))
            if not pub_dt:
                pub_dt = now

            if pub_dt < cutoff:
                page_old += 1
                stats["items_old_filtered"] += 1
                continue

            seen_urls.add(url)
            title = str(it.get("title") or "").strip() or url.rsplit("/", 1)[-1]
            body = str(it.get("text") or "").strip()
            collected.append(
                {
                    "title": title,
                    "url": url,
                    "publishDate": pub_dt.isoformat(),
                    "body": body,
                }
            )
            page_recent += 1
            stats["items_recent"] += 1

        if page_recent == 0 and page_old > 0:
            break

    return collected, stats


def parse_dzen_source(
    *,
    hours: int,
    max_pages: int,
    max_items: int,
    fulltext_limit: int,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    session = requests.Session()
    stats = {
        "entry_ok": False,
        "links_found": 0,
        "articles_success": 0,
        "articles_failed": 0,
        "fulltext_attempted": 0,
        "fulltext_skipped": 0,
        "api": {},
    }

    records, api_stats = fetch_dzen_news_api_items(
        session,
        hours=hours,
        max_pages=max_pages,
        max_items=max_items,
        deadline_ts=deadline_ts,
    )
    stats["api"] = api_stats
    stats["entry_ok"] = bool(api_stats.get("api_ok"))
    stats["links_found"] = len(records)
    logging.info("[DZEN] Collected records in %sh window: %s", hours, len(records))

    out: list[NewsItem] = []
    for idx, rec in enumerate(records, start=1):
        if is_deadline_reached(deadline_ts):
            logging.warning("[DZEN] Stop by runtime limit while parsing articles.")
            break
        link = rec["url"]
        if idx > max(0, fulltext_limit):
            stats["fulltext_skipped"] += 1
            out.append(
                NewsItem(
                    title=rec.get("title") or link.rsplit("/", 1)[-1],
                    url=link,
                    content=rec.get("body") or rec.get("title") or link,
                    source="Dzen news",
                    published_at=normalize_datetime_or_now(rec.get("publishDate", ""), link),
                )
            )
            stats["articles_success"] += 1
            continue
        try:
            stats["fulltext_attempted"] += 1
            html = fetch_html(session, link, "DZEN")
            title, content, page_dt = extract_article_text(html)
            if not title:
                title = rec.get("title") or link.rsplit("/", 1)[-1]
            if not content:
                content = rec.get("body") or title
            out.append(
                NewsItem(
                    title=title,
                    url=link,
                    content=content,
                    source="Dzen news",
                    published_at=normalize_datetime_or_now(page_dt or rec.get("publishDate", ""), link),
                )
            )
            stats["articles_success"] += 1
        except Exception as exc:
            stats["articles_failed"] += 1
            logging.exception("[DZEN] Article parse failed: %s (%s)", link, exc)

    return out, stats


def parse_lenta_source(
    *,
    max_items: int,
    fulltext_limit: int,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    return parse_generic_source(
        source_tag="LENTA",
        source_name="Lenta",
        base_url=LENTA_BASE_URL,
        max_items=max_items,
        fulltext_limit=fulltext_limit,
        include_patterns=[
            r"^/news/\d{4}/\d{2}/\d{2}/",
            r"^/articles/\d{4}/\d{2}/\d{2}/",
        ],
        deadline_ts=deadline_ts,
    )


def parse_tproger_source(
    *,
    max_items: int,
    fulltext_limit: int,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    return parse_generic_source(
        source_tag="TPROGER",
        source_name="Tproger news",
        base_url=TPROGER_NEWS_BASE_URL,
        max_items=max_items,
        fulltext_limit=fulltext_limit,
        include_patterns=[
            r"^/news/",
        ],
        exclude_patterns=[
            r"^/news/?$",
            r"^/news/page/",
        ],
        deadline_ts=deadline_ts,
    )


def parse_ren_source(
    *,
    max_items: int,
    fulltext_limit: int,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    return parse_generic_source(
        source_tag="REN",
        source_name="REN TV news",
        base_url=REN_NEWS_BASE_URL,
        max_items=max_items,
        fulltext_limit=fulltext_limit,
        include_patterns=[
            r"^/news/",
            r"^/video/",
            r"/\d{4}/\d{2}/\d{2}/",
        ],
        exclude_patterns=[
            r"^/news/?$",
            r"^/news/page/",
        ],
        deadline_ts=deadline_ts,
    )


def parse_mk_source(
    *,
    max_items: int,
    fulltext_limit: int,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    return parse_generic_source(
        source_tag="MK",
        source_name="MK news",
        base_url=MK_NEWS_BASE_URL,
        max_items=max_items,
        fulltext_limit=fulltext_limit,
        include_patterns=[
            r"^/news/",
            r"/\d{4}/\d{2}/\d{2}/",
        ],
        exclude_patterns=[
            r"^/news/?$",
            r"^/news/page/",
        ],
        deadline_ts=deadline_ts,
    )


def parse_m24_source(
    *,
    max_items: int,
    fulltext_limit: int,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    return parse_generic_source(
        source_tag="M24",
        source_name="M24 news",
        base_url=M24_NEWS_BASE_URL,
        max_items=max_items,
        fulltext_limit=fulltext_limit,
        include_patterns=[
            r"^/news/",
        ],
        exclude_patterns=[
            r"^/news/?$",
            r"^/news/page/",
        ],
        deadline_ts=deadline_ts,
    )


def parse_gazeta_source(
    *,
    max_items: int,
    fulltext_limit: int,
    deadline_ts: float | None = None,
) -> tuple[list[NewsItem], dict]:
    session = requests.Session()
    stats = {
        "entry_ok": False,
        "links_found": 0,
        "articles_success": 0,
        "articles_failed": 0,
        "fulltext_attempted": 0,
        "fulltext_skipped": 0,
        "sso_redirected": 0,
        "sitemap": {},
    }
    records, sitemap_stats = fetch_gazeta_news_sitemap_items(
        session,
        max_items=max_items,
        deadline_ts=deadline_ts,
    )
    stats["sitemap"] = sitemap_stats
    stats["entry_ok"] = bool(sitemap_stats.get("sitemap_ok"))
    stats["links_found"] = len(records)

    gazeta_cookie = (os.getenv("GAZETA_COOKIE", "") or "").strip() or "unity_pause_sso=1"
    gazeta_referer = (os.getenv("GAZETA_REFERER", "") or "").strip() or GAZETA_NEWS_BASE_URL

    out: list[NewsItem] = []
    for idx, rec in enumerate(records, start=1):
        if is_deadline_reached(deadline_ts):
            break
        link = rec["url"]
        fallback_title = rec.get("title") or link.rsplit("/", 1)[-1]
        fallback_content = rec.get("body") or fallback_title or link

        if idx > max(0, fulltext_limit):
            out.append(
                NewsItem(
                    title=fallback_title,
                    url=link,
                    content=fallback_content,
                    source="Gazeta news",
                    published_at=normalize_datetime_or_now(rec.get("publishDate", ""), link),
                )
            )
            stats["articles_success"] += 1
            stats["fulltext_skipped"] += 1
            continue

        try:
            stats["fulltext_attempted"] += 1
            headers = {
                **HEADERS,
                "Referer": gazeta_referer,
                "Cookie": gazeta_cookie,
            }
            response = session.get(link, headers=headers, timeout=30, allow_redirects=True)
            response.raise_for_status()
            html = response.text

            final_url = (response.url or "").lower()
            if "/auth/sso.shtml" in final_url:
                stats["sso_redirected"] += 1
                title = fallback_title
                content = fallback_content
                page_dt = rec.get("publishDate", "")
            else:
                title, content, page_dt = extract_article_text(html)
                if not title:
                    title = fallback_title
                if not content:
                    content = fallback_content

            out.append(
                NewsItem(
                    title=title,
                    url=link,
                    content=content,
                    source="Gazeta news",
                    published_at=normalize_datetime_or_now(page_dt or rec.get("publishDate", ""), link),
                )
            )
            stats["articles_success"] += 1
        except Exception as exc:
            stats["articles_failed"] += 1
            logging.exception("[GAZETA] Article parse failed: %s (%s)", link, exc)
            out.append(
                NewsItem(
                    title=fallback_title,
                    url=link,
                    content=fallback_content,
                    source="Gazeta news",
                    published_at=normalize_datetime_or_now(rec.get("publishDate", ""), link),
                )
            )
            stats["articles_success"] += 1

    return out, stats


def dedupe_and_sort(items: list[NewsItem]) -> list[NewsItem]:
    uniq: dict[str, NewsItem] = {}
    for item in items:
        if item.url not in uniq:
            uniq[item.url] = item

    def sort_key(x: NewsItem) -> datetime:
        dt = parse_publish_datetime(x.published_at)
        return dt if dt else datetime.min.replace(tzinfo=timezone.utc)

    return sorted(uniq.values(), key=sort_key, reverse=True)


def run(
    *,
    hours: int,
    rbc_max_pages: int,
    rbc_max_items: int,
    rbc_fulltext_limit: int,
    rbc_rss_urls: list[str],
    rbc_cookie: str,
    rbc_referer: str,
    dzen_max_pages: int,
    ria_max_items: int,
    ria_fulltext_limit: int,
    dzen_max_items: int,
    dzen_fulltext_limit: int,
    lenta_max_items: int,
    lenta_fulltext_limit: int,
    tproger_max_items: int,
    tproger_fulltext_limit: int,
    ren_max_items: int,
    ren_fulltext_limit: int,
    mk_max_items: int,
    mk_fulltext_limit: int,
    m24_max_items: int,
    m24_fulltext_limit: int,
    gazeta_max_items: int,
    gazeta_fulltext_limit: int,
    max_search_seconds: int,
    output_path: str = "",
) -> tuple[Path, dict]:
    deadline_ts = time.time() + max(1, int(max_search_seconds))
    def safe_source_result(source_name: str, future) -> tuple[list[NewsItem], dict]:
        try:
            return future.result()
        except Exception as exc:
            logging.exception("[%s] source failed: %s", source_name, exc)
            return [], {
                "entry_ok": False,
                "links_found": 0,
                "articles_success": 0,
                "articles_failed": 0,
                "fulltext_attempted": 0,
                "fulltext_skipped": 0,
                "error": str(exc),
            }

    with ThreadPoolExecutor(max_workers=9) as executor:
        rbc_future = executor.submit(
            parse_rbc_source,
            hours=hours,
            max_pages=rbc_max_pages,
            max_items=rbc_max_items,
            fulltext_limit=rbc_fulltext_limit,
            feed_urls=rbc_rss_urls,
            cookie=rbc_cookie,
            referer=rbc_referer,
            deadline_ts=deadline_ts,
        )
        ria_future = executor.submit(
            parse_ria_source,
            max_items=ria_max_items,
            fulltext_limit=ria_fulltext_limit,
            deadline_ts=deadline_ts,
        )
        dzen_future = executor.submit(
            parse_dzen_source,
            hours=hours,
            max_pages=dzen_max_pages,
            max_items=dzen_max_items,
            fulltext_limit=dzen_fulltext_limit,
            deadline_ts=deadline_ts,
        )
        lenta_future = executor.submit(
            parse_lenta_source,
            max_items=lenta_max_items,
            fulltext_limit=lenta_fulltext_limit,
            deadline_ts=deadline_ts,
        )
        tproger_future = executor.submit(
            parse_tproger_source,
            max_items=tproger_max_items,
            fulltext_limit=tproger_fulltext_limit,
            deadline_ts=deadline_ts,
        )
        ren_future = executor.submit(
            parse_ren_source,
            max_items=ren_max_items,
            fulltext_limit=ren_fulltext_limit,
            deadline_ts=deadline_ts,
        )
        mk_future = executor.submit(
            parse_mk_source,
            max_items=mk_max_items,
            fulltext_limit=mk_fulltext_limit,
            deadline_ts=deadline_ts,
        )
        m24_future = executor.submit(
            parse_m24_source,
            max_items=m24_max_items,
            fulltext_limit=m24_fulltext_limit,
            deadline_ts=deadline_ts,
        )
        gazeta_future = executor.submit(
            parse_gazeta_source,
            max_items=gazeta_max_items,
            fulltext_limit=gazeta_fulltext_limit,
            deadline_ts=deadline_ts,
        )

        rbc_items, rbc_stats = safe_source_result("RBC", rbc_future)
        ria_items, ria_stats = safe_source_result("RIA", ria_future)
        dzen_items, dzen_stats = safe_source_result("DZEN", dzen_future)
        lenta_items, lenta_stats = safe_source_result("LENTA", lenta_future)
        tproger_items, tproger_stats = safe_source_result("TPROGER", tproger_future)
        ren_items, ren_stats = safe_source_result("REN", ren_future)
        mk_items, mk_stats = safe_source_result("MK", mk_future)
        m24_items, m24_stats = safe_source_result("M24", m24_future)
        gazeta_items, gazeta_stats = safe_source_result("GAZETA", gazeta_future)

    merged = dedupe_and_sort(
        rbc_items
        + ria_items
        + dzen_items
        + lenta_items
        + tproger_items
        + ren_items
        + mk_items
        + m24_items
        + gazeta_items
    )

    out_dir = Path(__file__).resolve().parents[1] / "news"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = Path(output_path) if output_path else out_dir / f"news_{date.today().isoformat()}.json"

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": [
            RBC_BASE_URL,
            RIA_BASE_URL,
            DZEN_BASE_URL,
            LENTA_BASE_URL,
            TPROGER_NEWS_BASE_URL,
            REN_NEWS_BASE_URL,
            MK_NEWS_BASE_URL,
            M24_NEWS_BASE_URL,
            GAZETA_NEWS_BASE_URL,
        ],
        "count": len(merged),
        "stats": {
            "rbc": rbc_stats,
            "ria": ria_stats,
            "dzen": dzen_stats,
            "lenta": lenta_stats,
            "tproger": tproger_stats,
            "ren": ren_stats,
            "mk": mk_stats,
            "m24": m24_stats,
            "gazeta": gazeta_stats,
            "combined": {
                "before_dedupe": (
                    len(rbc_items)
                    + len(ria_items)
                    + len(dzen_items)
                    + len(lenta_items)
                    + len(tproger_items)
                    + len(ren_items)
                    + len(mk_items)
                    + len(m24_items)
                    + len(gazeta_items)
                ),
                "after_dedupe": len(merged),
            },
        },
        "items": [asdict(item) for item in merged],
    }
    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logging.info("Saved JSON: %s", out_file)
    logging.info("Combined result count: %s", len(merged))
    return out_file, payload["stats"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Parallel news parser: RBC short_news + RIA + Dzen + Lenta + Tproger + REN + MK + M24 + Gazeta"
        )
    )
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--rbc-max-pages", type=int, default=300)
    parser.add_argument("--rbc-max-items", type=int, default=3000)
    parser.add_argument("--rbc-fulltext-limit", type=int, default=200)
    parser.add_argument(
        "--rbc-rss-urls",
        default=",".join(RBC_RSSHUB_FEEDS),
        help="Comma-separated RSSHub feed URLs for RBC short news failover",
    )
    parser.add_argument(
        "--rbc-cookie",
        default=os.getenv("RBC_COOKIE", ""),
        help="Optional Cookie header for RBC requests (or set RBC_COOKIE env)",
    )
    parser.add_argument(
        "--rbc-referer",
        default=os.getenv("RBC_REFERER", RBC_BASE_URL),
        help="Optional Referer for RBC API requests (or set RBC_REFERER env)",
    )
    parser.add_argument("--ria-max-items", type=int, default=300)
    parser.add_argument("--ria-fulltext-limit", type=int, default=150)
    parser.add_argument("--dzen-max-pages", type=int, default=8)
    parser.add_argument("--dzen-max-items", type=int, default=200)
    parser.add_argument("--dzen-fulltext-limit", type=int, default=80)
    parser.add_argument("--lenta-max-items", type=int, default=200)
    parser.add_argument("--lenta-fulltext-limit", type=int, default=80)
    parser.add_argument("--tproger-max-items", type=int, default=200)
    parser.add_argument("--tproger-fulltext-limit", type=int, default=80)
    parser.add_argument("--ren-max-items", type=int, default=200)
    parser.add_argument("--ren-fulltext-limit", type=int, default=80)
    parser.add_argument("--mk-max-items", type=int, default=200)
    parser.add_argument("--mk-fulltext-limit", type=int, default=80)
    parser.add_argument("--m24-max-items", type=int, default=200)
    parser.add_argument("--m24-fulltext-limit", type=int, default=80)
    parser.add_argument("--gazeta-max-items", type=int, default=200)
    parser.add_argument("--gazeta-fulltext-limit", type=int, default=80)
    parser.add_argument(
        "--max-search-seconds",
        type=int,
        default=300,
        help="Global time limit for search/collection stage in seconds (default: 300)",
    )
    parser.add_argument("--output", default="")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    log_file = setup_logging(args.log_level)
    logging.info(
        "Parser start. Sources: %s | %s | %s | %s | %s | %s | %s | %s | %s",
        RBC_BASE_URL,
        RIA_BASE_URL,
        DZEN_BASE_URL,
        LENTA_BASE_URL,
        TPROGER_NEWS_BASE_URL,
        REN_NEWS_BASE_URL,
        MK_NEWS_BASE_URL,
        M24_NEWS_BASE_URL,
        GAZETA_NEWS_BASE_URL,
    )
    logging.info("Log file: %s", log_file)

    out_file, stats = run(
        hours=args.hours,
        rbc_max_pages=args.rbc_max_pages,
        rbc_max_items=args.rbc_max_items,
        rbc_fulltext_limit=args.rbc_fulltext_limit,
        rbc_rss_urls=[u.strip() for u in args.rbc_rss_urls.split(",") if u.strip()],
        rbc_cookie=args.rbc_cookie.strip(),
        rbc_referer=args.rbc_referer.strip() or RBC_BASE_URL,
        dzen_max_pages=args.dzen_max_pages,
        ria_max_items=args.ria_max_items,
        ria_fulltext_limit=args.ria_fulltext_limit,
        dzen_max_items=args.dzen_max_items,
        dzen_fulltext_limit=args.dzen_fulltext_limit,
        lenta_max_items=args.lenta_max_items,
        lenta_fulltext_limit=args.lenta_fulltext_limit,
        tproger_max_items=args.tproger_max_items,
        tproger_fulltext_limit=args.tproger_fulltext_limit,
        ren_max_items=args.ren_max_items,
        ren_fulltext_limit=args.ren_fulltext_limit,
        mk_max_items=args.mk_max_items,
        mk_fulltext_limit=args.mk_fulltext_limit,
        m24_max_items=args.m24_max_items,
        m24_fulltext_limit=args.m24_fulltext_limit,
        gazeta_max_items=args.gazeta_max_items,
        gazeta_fulltext_limit=args.gazeta_fulltext_limit,
        max_search_seconds=args.max_search_seconds,
        output_path=args.output,
    )
    logging.info("Done. Output=%s", out_file)
    logging.info("Final stats: %s", stats)


if __name__ == "__main__":
    main()
