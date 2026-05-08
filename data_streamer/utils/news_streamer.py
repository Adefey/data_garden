import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from email.utils import format_datetime

import aiohttp
import feedparser
from utils.cache import cache
from utils.db import add_news_item, get_news_item_by_key

logger = logging.getLogger(__name__)


rss_query_timeout_sec = int(os.environ.get("RSS_QUERY_TIMEOUT_SEC", 5))
rss_query_delay_sec = int(os.environ.get("RSS_QUERY_TIMEOUT_SEC", 10))
rss_user_agent = os.environ.get("RSS_USER_AGENT")
rss_max_parallel_requests = int(os.environ.get("RSS_MAX_PARALLEL_REQUESTS", 50))

redis_news_channel = os.environ.get("REDIS_NEWS_CHANNEL")


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
        async with session.get(url, headers=headers, timeout=rss_query_timeout_sec) as resp:
            status = resp.status
            text = await resp.text()
            new_etag = resp.headers.get("ETag")
            new_modified = resp.headers.get("Last-Modified")
            return status, text, new_etag, new_modified
    except Exception as e:
        logger.error(f"Error fetching {url}: {repr(e)}")
        return None, None, None, None


async def stream_news(sources_list: list[str]):
    logger.info(f"Started streaming news from {len(sources_list)} sources")
    logger.debug(f"{sources_list=}")

    # Populate cache
    for source in sources_list:
        await cache.hmset(source, mapping={"etag": "", "modified": ""})

    connector = aiohttp.TCPConnector(limit=rss_max_parallel_requests)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            new_items_count = 0
            start_time = time.time()
            logger.info(
                f"{time.ctime()}: Start query all {len(sources_list)} RSS with {rss_max_parallel_requests} max parallel"
                " requests"
            )

            tasks = []
            for source in sources_list:
                source_cache = await cache.hmget(source, keys=["etag", "modified"])
                etag = source_cache[0]
                modified = source_cache[1]
                tasks.append(fetch_rss_feed(session, source, etag, modified))

            results = await asyncio.gather(*tasks)

            for url, (status, text, new_etag, new_modified) in zip(sources_list, results):
                if status is None:
                    logger.debug(f"{url}: empty response or exception")
                    continue
                if status == 304:
                    logger.debug(f"{url}: 304 Not Modified")
                    continue
                if status != 200:
                    logger.debug(f"{url}: status {status}")
                    continue

                feed = feedparser.parse(text)
                if feed is None or feed.bozo:
                    logger.debug(f"{url}: parsing error: {feed=}; {feed.bozo=}")
                    continue

                logger.info(f"Processing results from {url}, {status=}, {new_etag=}, {new_modified=}")

                if new_etag is None:
                    new_etag = ""
                if isinstance(new_etag, bytes):
                    new_etag = new_etag.decode("utf-8")
                if new_modified is None:
                    new_modified = format_datetime(datetime.now(timezone.utc), usegmt=True)
                await cache.hmset(source, {"etag": new_etag, "modified": new_modified})

                for entry in feed.entries:
                    if entry.get("id"):
                        key = entry.id
                        if isinstance(key, bytes):
                            key = key.decode("utf-8")
                    else:
                        key = f"{entry.link}:{entry.published}"
                    existing_item = await get_news_item_by_key(key)
                    if existing_item:
                        continue
                    new_items_count += 1

                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    link = entry.get("link", "")
                    published_parsed = entry.get("published_parsed")
                    if published_parsed:
                        timedate = datetime(*published_parsed[:6])
                    else:
                        timedate = datetime.now(timezone.utc)
                    summary_detail = entry.get("summary_detail")
                    if summary_detail:
                        language = summary_detail.get("language")
                    else:
                        language = None

                    await add_news_item(key, title, summary, link, timedate, language)

                    message_for_channel = {
                        "news_key": key,
                        "title": title,
                        "summary": summary,
                        "link": link,
                        "timedate": timedate,
                        "language": language,
                        "cluster_id": 0,
                    }
                    message_for_channel = {key: str(value) for key, value in message_for_channel.items()}
                    message_for_channel = json.dumps(message_for_channel)
                    await cache.publish(
                        redis_news_channel,
                        message=message_for_channel,
                    )

            elapsed_time = time.time() - start_time
            logger.info(
                f"{time.ctime()}: Finish query all {len(sources_list)} RSS in {elapsed_time:.3f} sec, new items:"
                f" {new_items_count}"
            )
            logger.info(f"Waiting {rss_query_delay_sec} seconds for next iteration")
            await asyncio.sleep(rss_query_delay_sec)
