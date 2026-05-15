import asyncio
import logging
import os
import sys
from asyncio import create_task, sleep
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI

from common_libs.async_utils import handle_exception
from common_libs.news_clustering import NewsClustering
from common_libs.vector_db import VectorDB

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(
            filename=f'./logs/embeddings_{datetime.now().strftime("%y_%m_%d_%H:%M:%S")}.log'
        ),
        logging.StreamHandler(stream=sys.stdout),
    ],
)

app_name = os.environ.get("APP_NAME")
app_summary = os.environ.get("APP_SUMMARY")
app_description = os.environ.get("APP_DESCRIPTION")
app_version = os.environ.get("APP_VERSION")
start_sleep = float(os.environ.get("START_SLEEP", 5))

logger = logging.getLogger(__name__)
news_clustering = NewsClustering()
vector_db = VectorDB()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Sleeping {start_sleep} seconds before start to allow databases to start")
    await sleep(start_sleep)
    logger.info("Starting!")
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_exception)
    logger.info("Exception handling for background tasks enabled!")
    await vector_db.prepare_table()
    logger.info("Vector database loaded!")
    task = create_task(news_clustering.cluster_news())
    logger.info("Async data clustering task started!")
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


@app.get("/health")
async def health():
    return {"status": "healthy"}
