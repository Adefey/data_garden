import logging
import os
import sys
from asyncio import sleep
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Sleeping {start_sleep} seconds before start to allow databases to start")
    await sleep(start_sleep)
    logger.info("Starting!")
    yield


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
