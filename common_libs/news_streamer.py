import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from email.utils import format_datetime

import aiohttp
import feedparser
import redis.asyncio as redis

from common_libs.async_utils import gather_with_limit
from common_libs.content_utils import get_safe_string
from common_libs.db import Database
from common_libs.models import NewsItemModel

rss_query_timeout_sec = int(os.environ.get("RSS_QUERY_TIMEOUT_SEC"))
rss_query_delay_sec = int(os.environ.get("RSS_QUERY_DELAY_SEC"))
rss_user_agent = os.environ.get("RSS_USER_AGENT")
redis_news_channel = os.environ.get("REDIS_NEWS_CHANNEL")
parse_timeout = int(os.environ.get("PARSE_TIMEOUT"))
redis_host = os.environ.get("REDIS_HOST")

logger = logging.getLogger(__name__)
cache = redis.Redis(host=redis_host)
db = Database()


class NewsStreamer:

    def __init__(self, sources_list: list[str] = None):
        if sources_list is None:
            sources_list = []
        self.sources_list = sources_list

    async def fetch_rss_feed(
        self, session: aiohttp.ClientSession, url: str, etag: str = "", modified: str = ""
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
                text = get_safe_string(text)
                new_etag = get_safe_string(resp.headers.get("ETag"))
                new_modified = get_safe_string(resp.headers.get("Last-Modified"))
                return status, text, new_etag, new_modified
        except Exception as e:
            logger.error(f"Error fetching {url}: {repr(e)}")
            return None, None, None, None

    async def update_source(self, source: str, new_etag: str, new_modified: str):
        if new_etag is None:
            new_etag = ""
        if new_modified is None:
            new_modified = format_datetime(datetime.now(timezone.utc), usegmt=True)
        await cache.hmset(source, {"etag": new_etag, "modified": new_modified})

    async def extract_rss_info(self, text: str) -> list[NewsItemModel]:
        try:
            feed = await asyncio.wait_for(
                asyncio.to_thread(feedparser.parse, text), timeout=parse_timeout
            )
        except TimeoutError:
            logger.error(f"Parsing timed out after {parse_timeout}s, text length: {len(text)}")
            return []

        if feed is None or feed.bozo:
            logger.error(f"parsing error: {feed.bozo=} ({text[:25]=})")
            return []

        parsed_items = []

        for entry in feed.entries:

            entry_id = get_safe_string(entry.get("id"))
            entry_link = get_safe_string(entry.get("link"))
            entry_published = get_safe_string(entry.get("published"))

            key = f"{entry_id}:{entry_link}:{entry_published}"

            entry_title = get_safe_string(entry.get("title"))
            entry_summary = get_safe_string(entry.get("summary"))

            published_parsed = entry.get("published_parsed")
            if published_parsed:
                entry_timedate = datetime(*published_parsed[:6])
            else:
                entry_timedate = datetime.now(timezone.utc)

            summary_detail = entry.get("summary_detail")
            if summary_detail:
                entry_language = get_safe_string(summary_detail.get("language"))
            else:
                entry_language = None

            news_item = NewsItemModel(
                news_key=key,
                title=entry_title,
                summary=entry_summary,
                link=entry_link,
                timedate=entry_timedate,
                language=entry_language,
            )

            parsed_items.append(news_item)

        return parsed_items

    async def stream_news(self):
        logger.info(f"Started streaming news from {len(self.sources_list)} sources")
        logger.debug(f"{self.sources_list=}")

        async with aiohttp.ClientSession() as session:
            while True:
                start_time = time.time()
                logger.info(
                    f"{time.ctime()}: Start query all {len(self.sources_list)} RSS requests"
                )

                logger.info(
                    f"Preparing etag, modified cache for {len(self.sources_list)} RSS sources"
                )
                for source in self.sources_list:
                    source_cache = await cache.hmget(source, keys=["etag", "modified"])
                    etag = source_cache[0]
                    modified = source_cache[1]
                    if not etag and not modified:
                        await cache.hmset(source, mapping={"etag": "", "modified": ""})
                logger.info(
                    f"Prepared etag, modified cache for {len(self.sources_list)} RSS sources"
                )

                fetch_tasks = []
                for source in self.sources_list:
                    source_cache = await cache.hmget(source, keys=["etag", "modified"])
                    etag = source_cache[0]
                    modified = source_cache[1]
                    fetch_tasks.append(self.fetch_rss_feed(session, source, etag, modified))

                logger.info(f"Sending {len(fetch_tasks)} fetch RSS requests")
                fetch_results = await gather_with_limit(*fetch_tasks)
                logger.info(f"Got {len(fetch_results)} fetch RSS responses")

                update_source_tasks = []
                parse_sourse_tasks = []
                for url, (status, text, new_etag, new_modified) in zip(
                    self.sources_list, fetch_results
                ):
                    if status is None:
                        logger.error(f"{url}: empty response or exception")
                        continue
                    if status == 304:
                        logger.error(f"{url}: 304 Not Modified")
                        continue
                    if status != 200:
                        logger.error(f"{url}: status {status}")
                        continue
                    update_source_tasks.append(self.update_source(url, new_etag, new_modified))
                    parse_sourse_tasks.append(self.extract_rss_info(text))

                total_processed_news = []

                logger.info(f"Starting {len(update_source_tasks)} update sources jobs")
                await gather_with_limit(*update_source_tasks)
                logger.info(f"Finished {len(update_source_tasks)} update sources jobs")

                logger.info(f"Starting {len(parse_sourse_tasks)} parse RSS items jobs")
                lists_of_news = await gather_with_limit(*parse_sourse_tasks)
                total_processed_news: list[NewsItemModel] = [
                    item for list_of_news in lists_of_news for item in list_of_news
                ]
                logger.info(
                    f"Finished {len(parse_sourse_tasks)} parse RSS items jobs - got"
                    f" {len(total_processed_news)} total news"
                )

                logger.info(
                    f"Starting selecting relevant record from {len(total_processed_news)} total"
                )
                existing_keys = await db.news_keys_exist(
                    [item.news_key for item in total_processed_news]
                )
                filtered_news = [
                    item for item in total_processed_news if item.news_key in existing_keys
                ]
                logger.info(f"Finished {len(filtered_news)} databases update jobs")

                logger.info(f"Starting publishing {len(filtered_news)} new news in RDBMS and Redis")
                await db.add_news_items(filtered_news)
                for news_item in filtered_news:
                    await cache.publish(redis_news_channel, news_item.model_dump_json())
                logger.info(f"Finished publishing {len(filtered_news)} new news in RDBMS and Redis")

                elapsed_time = time.time() - start_time
                logger.info(
                    f"{time.ctime()}: Finish query all {len(self.sources_list)} RSS in"
                    f" {elapsed_time:.3f} sec, new items: {len(total_processed_news)}"
                )
                logger.info(f"Waiting {rss_query_delay_sec} seconds for next iteration")
                await asyncio.sleep(rss_query_delay_sec)
