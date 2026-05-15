import asyncio
import logging
import os
import time

import redis.asyncio as redis

from common_libs.db import Database
from common_libs.embeddings import EmbeddingModel
from common_libs.streaming_clusterization import DBStream
from common_libs.vector_db import VectorDB

logger = logging.getLogger(__name__)

compute_timeout = int(os.environ.get("COMPUTE_TIMEOUT"))
redis_processed_news_channel = os.environ.get("REDIS_PROCESSED_NEWS_CHANNEL")
clustering_delay_sec = float(os.environ.get("CLUSTERING_DELAY_SEC"))
clustering_batch_size = int(os.environ.get("CLUSTERING_BATCH_SIZE"))
redis_host = os.environ.get("REDIS_HOST")
redis_cache_set = os.environ.get("REDIS_CACHE_SET")
redis_timeout = float(os.environ.get("REDIS_TIMEOUT"))

cache = redis.Redis(
    host=redis_host,
    socket_connect_timeout=redis_timeout,
    socket_timeout=redis_timeout,
    retry_on_timeout=True,
)
db = Database()
dbstream = DBStream()
embeddings_model = EmbeddingModel()
vector_db = VectorDB()


class NewsClustering:

    def __init__(self):
        pass

    async def get_embeddings(self, texts: list[str]) -> list[list[float]]:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(embeddings_model.get_embeds, texts), timeout=compute_timeout
            )
        except TimeoutError:
            logger.error(
                f"Getting embeddings timed out after {compute_timeout}s, text length: {len(texts)}"
            )
            return []

    async def get_cluster_ids(self, embeddings: list[list[float]]) -> list[int]:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(dbstream.predict_clusters, embeddings), timeout=compute_timeout
            )
        except TimeoutError:
            logger.error(
                f"Getting cluster ids timed out after {compute_timeout}s, text length:"
                f" {len(embeddings)}"
            )
            return []

    async def cluster_news(self):
        logger.info("Started updating database with cluster IDs")

        while True:
            logger.info(f"{time.ctime()}: Start query database for news without cluster ID")
            start_time = time.time()

            logger.info("Start getting oldest news without cluster ID from database")
            news_items = await db.get_news_items(
                limit=clustering_batch_size, with_empty_cluster_id=True, newest_first=False
            )
            logger.info(
                f"Finished getting oldest {len(news_items)} news without cluster ID from database"
            )

            texts = [item.get_text_value() for item in news_items]

            logger.info(f"Start getting embeddings for {len(texts)} texts")
            embeddings = await self.get_embeddings(texts)
            logger.info(f"Completed getting embeddings for {len(texts)} texts")

            if len(embeddings) != len(texts):
                logger.warning(
                    f"Something went wrong during calculating embeddings: {len(embeddings)=} !="
                    f" {len(texts)=}. Skipping iteration"
                )
                continue

            logger.info(f"Start getting cluster IDs for {len(embeddings)} embeddings")
            cluster_ids = await self.get_cluster_ids(embeddings)
            logger.info(f"Completed getting cluster IDs for {len(embeddings)} embeddings")

            if len(cluster_ids) != len(embeddings):
                logger.warning(
                    f"Something went wrong during calculating cluster ids {len(cluster_ids)=} !="
                    f" {len(embeddings)=}. Skipping iteration"
                )
                continue

            cluster_id_mapping = {
                news_item.news_key: cluster_id
                for news_item, cluster_id in zip(news_items, cluster_ids)
            }

            logger.info(f"Start update database with {len(cluster_ids)} cluster IDs")
            await db.set_cluster_ids_by_news_items_ids(cluster_id_mapping)
            logger.info(
                f"Completed update database with {len(cluster_ids)} cluster IDs: {cluster_ids}"
            )

            embedding_mapping = {
                news_item.news_key: embedding
                for news_item, embedding in zip(news_items, embeddings)
            }

            logger.info(f"Start update vector database with {len(embeddings)} embeddings")
            await vector_db.upload_points(embedding_mapping)
            logger.info(f"Completed update vector database with {len(embeddings)} embeddings")

            for news_item, cluster_id in zip(news_items, cluster_ids):
                news_item.cluster_id = cluster_id

            logger.info(f"Starting publishing {len(news_items)} processed news in Redis")
            for news_item in news_items:
                await cache.publish(redis_processed_news_channel, news_item.model_dump_json())
            logger.info(f"Finished publishing {len(news_items)} processed news in Redis")

            elapsed_time = time.time() - start_time
            logger.info(
                f"{time.ctime()}: Finished updating database for news without cluster ID in"
                f" {elapsed_time:.3f} with {len(cluster_ids)} items"
            )

            logger.info(f"Waiting {clustering_delay_sec} seconds for next iteration")
            await asyncio.sleep(clustering_delay_sec)
