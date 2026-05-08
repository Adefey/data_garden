import asyncio
import logging
import os
import time
from email.utils import format_datetime

import aiohttp
import feedparser
import redis.asyncio as redis
from utils import db

logger = logging.getLogger(__name__)

redis_host = os.environ.get("REDIS_HOST")
rss_query_timeout_sec = os.environ.get("RSS_QUERY_TIMEOUT_SEC")
rss_query_delay_sec = os.environ.get("RSS_QUERY_TIMEOUT_SEC")
rss_user_agent = os.environ.get("RSS_USER_AGENT")
rss_max_parallel_requests = int(os.environ.get("RSS_MAX_PARALLEL_REQUESTS", 50))

cache = redis.Redis(host=redis_host)


async def fetch_rss_feed(
    session: aiohttp.ClientSession, url: str, etag: str = "", modified: str = ""
) -> tuple[int, str, str, str]:
    headers = {"User-Agent": rss_user_agent}
    if etag:
        headers["If-None-Match"] = etag
    if modified:
        if isinstance(modified, time.struct_time):
            modified = format_datetime(modified, usegmt=True)
        headers["If-Modified-Since"] = modified
    try:
        async with session.get(url, headers=headers, timeout=1) as resp:
            status = resp.status
            text = await resp.text()
            new_etag = resp.headers.get("ETag")
            new_modified = resp.headers.get("Last-Modified")
            return status, text, new_etag, new_modified
    except Exception as e:
        logger.info(f"Error fetching {url}: {repr(e)}")
        return None, None, None, None


async def stream_news(sources_list: list[str]):
    logger.info(f"Started streaming news from {len(sources_list)} sources")
    logger.debug(f"{sources_list=}")

    # Populate cache
    for source in sources_list:
        await cache.hmset(source, mapping={"etag": "", "modified": ""})

    connector = aiohttp.TCPConnector(limit=rss_max_parallel_requests)
    while True:
        new_items_count = 0
        start_time = time.time()
        logger.info(f"{time.ctime()}: Start query all {len(sources_list)} RSS")

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = []
            for source in sources_list:
                source_cache = await cache.hmget(source, keys=["etag", "modified"])
                etag = source_cache["etag"]
                modified = source_cache["modified"]
                tasks.append(fetch_rss_feed(session, source, etag, modified))

            results = await asyncio.gather(*tasks)

            for url, (status, text, new_etag, new_modified) in zip(sources_list, results):
                if status is None:
                    continue
                if status == 304:
                    logger.debug(f"{url}: 304 Not Modified")
                    continue
                if status != 200:
                    logger.debug(f"{url}: status {status}")
                    continue

                await cache.hmset(source, {"etag": new_etag, "modified": new_modified})

                feed = feedparser.parse(text)
                if feed is None or feed.bozo:
                    logger.debug(f"{url}: parsing error: {feed=}; {feed.bozo=}")
                    continue

                for entry in feed.entries:
                    if entry.get("id"):
                        key = entry.id
                    else:
                        key = f"{entry.link}:{entry.published}"

                    existing_item = db.get_news_item_by_key(key)
                    if existing_item:
                        continue

                    logger.warning(f"{entry=}")

                    # db.add_news_item(key,)

        elapsed_time = time.time() - start_time
        logger.info(
            f"{time.ctime()}: Finish query all {len(sources_list)} RSS in {elapsed_time:.2f} sec, new items:"
            f" {new_items_count}"
        )
