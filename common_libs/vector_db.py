import logging
import os
import uuid

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

qdrant_collection_name = os.environ.get("QDRANT_COLLECTION_NAME")
qdrant_host = os.environ.get("QDRANT_HOST")
qdrant_port = int(os.environ.get("QDRANT_PORT"))
embeddings_size = int(os.environ.get("EMBEDDINGS_SIZE"))
max_search_count = int(os.environ.get("MAX_SEARCH_COUNT"))

logger = logging.getLogger(__name__)


class VectorDB:

    def __init__(self):
        self.qdrant = AsyncQdrantClient(host=qdrant_host, port=qdrant_port)

    async def prepare_table(self):
        if not await self.qdrant.collection_exists(qdrant_collection_name):
            logger.info(
                f"Creating db {qdrant_collection_name} with embedding size {embeddings_size}"
            )
            await self.qdrant.create_collection(
                collection_name=qdrant_collection_name,
                vectors_config=VectorParams(
                    size=embeddings_size,
                    distance=Distance.COSINE,
                ),
            )

    async def upload_points(self, points_data: dict[str, list[float]]):
        try:
            await self.qdrant.upload_points(
                qdrant_collection_name,
                points=[
                    PointStruct(
                        id=str(uuid.uuid5(uuid.NAMESPACE_URL, news_id)),
                        vector=embedding,
                    )
                    for news_id, embedding in points_data.items()
                ],
            )
        except Exception as exc:
            logger.error(f"Failed to upload {len(points_data)} points: {repr(exc)}")

    async def search_similar(
        self, embedding: list[float], limit: int = max_search_count
    ) -> list[tuple[str, float]]:
        try:
            result = await self.qdrant.query_points(
                collection_name=qdrant_collection_name,
                query=embedding,
                limit=limit,
            )
            result = result.points
        except Exception as exc:
            logger.error(f"Failed to search for points: {repr(exc)}")

        news_and_scores = [(item.id, item.score) for item in result]
        return news_and_scores
