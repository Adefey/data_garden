import asyncio
import logging
import os
import sys
from asyncio import create_task, sleep
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from common_libs.async_utils import handle_exception
from common_libs.db import Database
from common_libs.models import NewsItemModel

# Reuse model
from common_libs.news_clustering import NewsClustering, embeddings_model
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
max_latest_news_count = int(os.environ.get("MAX_LATEST_NEWS_COUNT"))
compute_timeout = int(os.environ.get("COMPUTE_TIMEOUT"))
cors_origins = [s.strip() for s in os.environ.get("CORS_ORIGINS", "").split(",") if s.strip()]

logger = logging.getLogger(__name__)
news_clustering = NewsClustering()
vector_db = VectorDB()
db = Database()


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

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/search", response_model=list[NewsItemModel])
async def get_news(
    query: str,
    limit: int = max_latest_news_count,
):
    """
    Return news
    """
    embedding = await asyncio.wait_for(
        asyncio.to_thread(embeddings_model.get_embeds, query), timeout=compute_timeout
    )

    search_result = await vector_db.search_similar(embedding, limit=limit)
    logger.info(f"Query: {query}, result: {search_result}")
    news_keys = [news_key for news_key, _ in search_result]
    news_items = await db.get_news_by_news_keys(news_keys)
    return news_items
