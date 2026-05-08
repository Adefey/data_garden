import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket
from utils import db, news_streamer

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(filename=f'logs/data_streamer_{datetime.now().strftime("%y_%m_%d_%H:%M:%S")}.log'),
        logging.StreamHandler(stream=sys.stdout),
    ],
)

app_name = os.environ.get("APP_NAME")
app_summary = os.environ.get("APP_SUMMARY")
app_description = os.environ.get("APP_DESCRIPTION")
app_version = os.environ.get("APP_VERSION")
initial_rss_sources = os.environ.get("INITIAL_RSS_SOURCES", "").split(",")

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db(sources_list=initial_rss_sources)
    all_sources = await db.get_all_sources()
    await news_streamer.stream_news(all_sources)
    yield


app = FastAPI(title=app_name, summary=app_summary, description=app_name, version=app_version, lifespan=lifespan)


@app.websocket("/stream_texts")
async def stream_texts(websocket: WebSocket):
    await websocket.accept()
    while True:
        data = ...
        await websocket.send_text(f"Message text was: {data}")
