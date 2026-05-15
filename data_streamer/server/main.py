import asyncio
import logging
import os
import sys
from asyncio import create_task, sleep
from contextlib import asynccontextmanager
from datetime import datetime

import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from common_libs.async_utils import handle_exception
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
initial_rss_sources = [
    s.strip() for s in os.environ.get("INITIAL_RSS_SOURCES", "").split(",") if s.strip()
]
redis_news_channel = os.environ.get("REDIS_NEWS_CHANNEL")
redis_processed_news_channel = os.environ.get("REDIS_PROCESSED_NEWS_CHANNEL")
start_sleep = float(os.environ.get("START_SLEEP"))
redis_host = os.environ.get("REDIS_HOST")
redis_timeout = float(os.environ.get("REDIS_TIMEOUT"))
max_used_sources_count = int(os.environ.get("MAX_USED_SOURCES_COUNT"))
max_latest_news_count = int(os.environ.get("MAX_LATEST_NEWS_COUNT"))

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
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_exception)
    logger.info("Exception handling for background tasks enabled!")
    await db.prepare_tables()
    if await db.tables_empty():
        await db.add_sources(
            sources=[
                NewsSourceModel(link=source_link, is_enabled=True)
                for source_link in initial_rss_sources
            ]
        )
    logger.info("Database loaded!")
    task = create_task(streamer.stream_news())
    logger.info("Async data collection task started!")
    yield
    logger.info("Cancelling task")
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
    logger.info("Websocket stream_news - new connection")
    await websocket.accept()
    async with cache.pubsub() as pubsub:
        await pubsub.subscribe(redis_news_channel)
        try:
            recent_news = await db.get_news_items(limit=max_latest_news_count)
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
                            "Channel returned invalid data that does not pass validation:"
                            f" {repr(e)=} {news_item_json=}"
                        )
        except WebSocketDisconnect:
            pass
        finally:
            await pubsub.unsubscribe(redis_news_channel)
            logger.info("Websocket stream_news - closed connection")


@app.websocket("/stream_processed_news")
async def websocket_processed_news(websocket: WebSocket):
    logger.info("Websocket stream_processed_news - new connection")
    await websocket.accept()
    async with cache.pubsub() as pubsub:
        await pubsub.subscribe(redis_processed_news_channel)
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    news_item_json = message["data"].decode("utf-8")
                    # Verify that model can be assempled with the data
                    try:
                        NewsItemModel.model_validate_json(news_item_json)
                        await websocket.send_text(news_item_json)
                    except ValidationError as e:
                        logger.error(
                            "Channel returned invalid data that does not pass validation:"
                            f" {repr(e)=} {news_item_json=}"
                        )
        except WebSocketDisconnect:
            pass
        finally:
            await pubsub.unsubscribe(redis_processed_news_channel)
            logger.info("Websocket stream_processed_news - closed connection")


@app.get("/health")
async def health():
    """
    Health check
    """
    return {"status": "healthy"}


@app.post("/sources")
async def post_sources(sources: list[NewsSourceModel]):
    """
    Add sources. Takes affect on next iteration
    """
    await db.add_sources(sources)


@app.delete("/sources")
async def delete_sources(sources: list[NewsSourceModel]):
    """
    Delete sources that are sent by link. Takes affect on next iteration
    """
    await db.delete_sources(sources)


@app.get("/sources", response_model=list[NewsSourceModel])
async def get_sources(limit: int = max_used_sources_count, offset: int = 0):
    """
    Remove sources that are sent. Takes affect on next iteration
    """
    return await db.get_sources(limit, offset)


@app.get("/news", response_model=list[NewsItemModel])
async def get_news(limit: int = max_latest_news_count, offset: int = 0):
    """
    Return news
    """
    return await db.get_news_items(limit, offset)
