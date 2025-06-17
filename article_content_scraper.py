import asyncio
import datetime as dt
import logging
from pathlib import Path
from typing import List

import aiohttp
import pandas as pd
from selectolax.parser import HTMLParser

DATA_DIR = Path("data")
ARTICLE_DIR = DATA_DIR / "article-content"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def previous_date_str() -> str:
    return (dt.datetime.utcnow().date() - dt.timedelta(days=1)).strftime("%Y-%m-%d")


def load_url_log(date_str: str) -> pd.DataFrame:
    paths = [
        DATA_DIR / f"bbc_most_read_{date_str}.csv",
        DATA_DIR / f"bbc_front_page_promos_{date_str}.csv",
    ]
    frames = []
    for p in paths:
        if not p.exists():
            continue
        df = pd.read_csv(p)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.rename(columns={"link": "url"})
        frames.append(df[["timestamp", "url"]])
    if not frames:
        return pd.DataFrame(columns=["url", "first_appeared_at"])
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("timestamp")
    df = df.drop_duplicates("url", keep="first")
    df = df.rename(columns={"timestamp": "first_appeared_at"})
    return df.reset_index(drop=True)


def parse_article_html(html: str):
    tree = HTMLParser(html)
    canonical = None
    node = tree.css_first('link[rel="canonical"]')
    if node and node.attributes.get("href"):
        canonical = node.attributes["href"].strip()
    title = ""
    tnode = tree.css_first('meta[property="og:title"]')
    if tnode and tnode.attributes.get("content"):
        title = tnode.attributes["content"].strip()
    if not title:
        h1 = tree.css_first("h1")
        if h1:
            title = h1.text(strip=True)
    authors = {a.text(strip=True) for a in tree.css('[rel="author"], [itemprop="name"]') if a.text(strip=True)}
    meta_byl = tree.css_first('meta[name="byl"]')
    if meta_byl and meta_byl.attributes.get('content'):
        authors.add(meta_byl.attributes['content'].strip())
    body_nodes = tree.css('[data-component="text-block"]')
    if body_nodes:
        article_html = "".join(n.html for n in body_nodes)
        article_text = " ".join(n.text(separator=" ", strip=True) for n in body_nodes)
    else:
        main = tree.css_first("main") or tree.body
        article_html = main.html if main else html
        article_text = main.text(separator=" ", strip=True) if main else tree.text(separator=" ", strip=True)
    return canonical, title, sorted(authors), article_html, article_text


async def fetch_one(session: aiohttp.ClientSession, url: str):
    await asyncio.sleep(0.1)  # rate limit 10 req/s
    try:
        async with session.get(url, headers=HEADERS, allow_redirects=True) as resp:
            text = await resp.text()
            ok = resp.status == 200
            canonical, title, authors, article_html, article_text = parse_article_html(text)
            return {
                "url": canonical or str(resp.url),
                "title": title,
                "authors": ";".join(authors),
                "article_html": article_html,
                "article_text": article_text,
                "fetch_ok": ok,
            }
    except Exception as exc:
        logging.exception("Failed to fetch %s", url)
        return {
            "url": url,
            "title": "",
            "authors": "",
            "article_html": "",
            "article_text": "",
            "fetch_ok": False,
        }


async def fetch_articles(url_df: pd.DataFrame) -> pd.DataFrame:
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [fetch_one(session, row.url) for row in url_df.itertuples(index=False)]
        results = await asyncio.gather(*tasks)
    for r, (_, first) in zip(results, url_df.itertuples(index=False)):
        r["first_appeared_at"] = first
    return pd.DataFrame(results)


def save_parquet(df: pd.DataFrame, date_str: str) -> Path:
    ARTICLE_DIR.mkdir(parents=True, exist_ok=True)
    path = ARTICLE_DIR / f"{date_str}.parquet"
    df.to_parquet(path, index=False)
    return path


def main():
    logging.basicConfig(level=logging.INFO)
    date_str = previous_date_str()
    urls = load_url_log(date_str)
    if urls.empty:
        logging.info("No URLs found for %s", date_str)
        return
    df = asyncio.run(fetch_articles(urls))
    save_parquet(df, date_str)
    logging.info("Saved %d articles", len(df))


if __name__ == "__main__":
    main()
