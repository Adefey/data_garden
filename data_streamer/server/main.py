import json
import logging
import os
import sys
from asyncio import create_task, sleep
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from utils.cache import cache
from utils.db import get_all_sources, get_recent_news_items, init_db
from utils.news_streamer import stream_news

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(filename=f'./logs/data_streamer_{datetime.now().strftime("%y_%m_%d_%H:%M:%S")}.log'),
        logging.StreamHandler(stream=sys.stdout),
    ],
)

app_name = os.environ.get("APP_NAME")
app_summary = os.environ.get("APP_SUMMARY")
app_description = os.environ.get("APP_DESCRIPTION")
app_version = os.environ.get("APP_VERSION")
initial_rss_sources = os.environ.get("INITIAL_RSS_SOURCES", "").split(",")
redis_news_channel = os.environ.get("REDIS_NEWS_CHANNEL")
start_sleep = float(os.environ.get("START_SLEEP", 5))
latest_news_count = int(os.environ.get("LATEST_NEWS_COUNT", 50))

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Sleeping {start_sleep} seconds before start to allow databases to start")
    await sleep(start_sleep)
    logger.info("Starting!")
    await init_db(sources_list=initial_rss_sources)
    logger.info("Database loaded!")
    all_sources = await get_all_sources()
    logger.info(f"Sources list arrived! Total {len(all_sources)} entries")
    task = create_task(stream_news(all_sources))
    logger.info("Async data collection task started!")
    yield
    task.cancel()


app = FastAPI(title=app_name, summary=app_summary, description=app_description, version=app_version, lifespan=lifespan)


@app.websocket("/stream_news")
async def websocket_news(websocket: WebSocket):
    logger.info("Websocket - new connection")
    await websocket.accept()
    pubsub = cache.pubsub()
    await pubsub.subscribe(redis_news_channel)
    try:
        recent_news = await get_recent_news_items(latest_news_count)
        for item in recent_news:
            item = {key: str(value) for key, value in item.items()}
            await websocket.send_text(json.dumps(item))

        async for message in pubsub.listen():
            if message["type"] == "message":
                response = message["data"].decode("utf-8")
                await websocket.send_text(response)
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(redis_news_channel)
        logger.info("Websocket - closed connection")


@app.get("/health")
async def health():
    return {"status": "healthy"}
