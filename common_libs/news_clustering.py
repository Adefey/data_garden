import asyncio
import logging
import time

from common_libs.db import Database
from common_libs.embeddings import EmbeddingModel
from common_libs.streaming_clusterization import DBStream
from common_libs.vector_db import VectorDB

logger = logging.getLogger(__name__)

db = Database()
dbstream = DBStream()
embeddings_model = EmbeddingModel()
vector_db = VectorDB()


class NewsClustering:

    def __init__(self):
        pass

    async def get_embeddings(self, texts: list[str]) -> list[list[float]]:
        return await asyncio.wait_for(asyncio.to_thread(embeddings_model.get_embeds, texts))

    async def get_cluster_ids(self, embeddings: list[list[float]]) -> list[int]:
        return await asyncio.wait_for(asyncio.to_thread(dbstream.predict_clusters, embeddings))

    async def cluster_news(self):
        logger.info("Started updating database with cluster IDs")

        while True:
            logger.info(f"{time.ctime()}: Start query database for news without cluster ID")
            start_time = time.time()

            logger.info("Start getting news without cluster ID from database")
            news_items = await db.get_latest_news_items(with_empty_cluster_id=True)
            logger.info(f"Finished getting {len(news_items)} news without cluster ID from database")

            texts = [item.get_text_value() for item in news_items]

            logger.info(f"Start getting embeddings for {len(texts)} texts")
            embeddings = await self.get_embeddings(texts)
            logger.info(f"Completed getting embeddings for {len(texts)} texts")

            logger.info(f"Start getting cluster IDs for {len(embeddings)} embeddings")
            cluster_ids = await self.get_cluster_ids(embeddings)
            logger.info(f"Completed getting embeddings for {len(embeddings)} embeddings")

            mapping = {
                news_item.news_id: cluster_id
                for news_item, cluster_id in zip(news_items, cluster_ids)
            }

            logger.info(f"Start update database with {len(cluster_ids)} cluster IDs")
            await db.set_cluster_ids_by_news_items_ids(mapping)
            logger.info(f"Completed update database with {len(cluster_ids)} cluster IDs")

            logger.info(f"Start update vector database with {len(cluster_ids)} cluster IDs")
            await vector_db.upload_points(mapping)
            logger.info(f"Completed update vector database with {len(cluster_ids)} cluster IDs")

            elapsed_time = time.time() - start_time
            logger.info(
                f"{time.ctime()}: Finished updating database for news without cluster ID in"
                f" {elapsed_time:.3f} with {len(cluster_ids)} items"
            )
