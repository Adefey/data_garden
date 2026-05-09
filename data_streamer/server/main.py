import logging
import os
import sys
from asyncio import create_task, sleep
from contextlib import asynccontextmanager
from datetime import datetime

import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from common_libs.db import Database
from common_libs.models import NewsItemModel, NewsSourceModel
from common_libs.news_streamer import NewsStreamer

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(
            filename=f'./logs/data_streamer_{datetime.now().strftime("%y_%m_%d_%H:%M:%S")}.log'
        ),
        logging.StreamHandler(stream=sys.stdout),
    ],
)

app_name = os.environ.get("APP_NAME")
app_summary = os.environ.get("APP_SUMMARY")
app_description = os.environ.get("APP_DESCRIPTION")
app_version = os.environ.get("APP_VERSION")
initial_rss_sources = os.environ.get("INITIAL_RSS_SOURCES").split(",")
redis_news_channel = os.environ.get("REDIS_NEWS_CHANNEL")
start_sleep = float(os.environ.get("START_SLEEP"))
redis_host = os.environ.get("REDIS_HOST")
redis_timeout = float(os.environ.get("REDIS_TIMEOUT"))

logger = logging.getLogger(__name__)
cache = redis.Redis(
    host=redis_host,
    socket_connect_timeout=redis_timeout,
    socket_timeout=redis_timeout,
    retry_on_timeout=True,
)
db = Database()
streamer = NewsStreamer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Sleeping {start_sleep} seconds before start to allow databases to start")
    await sleep(start_sleep)
    logger.info("Starting!")
    await db.prepare_tables()
    if await db.tables_empty():
        await db.add_sources(
            sources=[
                NewsSourceModel(link=source_link, is_enabled=True)
                for source_link in initial_rss_sources
            ]
        )
    logger.info("Database loaded!")
    all_sources = await db.get_enabled_sources()
    logger.info(f"Sources list arrived! Total {len(all_sources)} entries")
    streamer.sources_list = [source.link for source in all_sources if source.is_enabled]
    task = create_task(streamer.stream_news())
    logger.info("Async data collection task started!")
    yield
    task.cancel()


app = FastAPI(
    title=app_name,
    summary=app_summary,
    description=app_description,
    version=app_version,
    lifespan=lifespan,
)


@app.websocket("/stream_news")
async def websocket_news(websocket: WebSocket):
    logger.info("Websocket - new connection")
    await websocket.accept()
    pubsub = cache.pubsub()
    await pubsub.subscribe(redis_news_channel)
    try:
        recent_news = await db.get_latest_news_items()
        for item in recent_news:
            await websocket.send_text(item.model_dump_json())

        async for message in pubsub.listen():
            if message["type"] == "message":
                news_item_json = message["data"].decode("utf-8")
                # Verify that model can be assempled with the data
                try:
                    NewsItemModel.model_validate_json(news_item_json)
                    await websocket.send_text(news_item_json)
                except ValidationError as e:
                    logger.error(
                        f"Channel returned invalid data that does not pass validation: {repr(e)=}"
                        f" {news_item_json=}"
                    )
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(redis_news_channel)
        logger.info("Websocket - closed connection")


@app.get("/health")
async def health():
    """
    Health check
    """
    return {"status": "healthy"}


@app.post("/new_sources")
async def health(sources: list[NewsSourceModel]):
    """
    Add new enabled sources. Takes affect on next iteration
    """
    streamer.sources_list += [source.link for source in sources if source.is_enabled]
