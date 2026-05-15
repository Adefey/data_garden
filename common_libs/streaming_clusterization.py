import logging
import os

from river import cluster, stream

clustering_threshold = float(os.environ.get("DBSTREAM_CLUSTERING_THRESHOLD"))
fading_factor = float(os.environ.get("DBSTREAM_FADING_FACTOR"))
cleanup_interval = float(os.environ.get("DBSTREAM_CLEANUP_INTERVAL"))
intersection_factor = float(os.environ.get("DBSTREAM_INTERSECTION_FACTOR"))
minimum_weight = float(os.environ.get("DBSTREAM_MINIMUM_WEIGHT"))

logger = logging.getLogger(__name__)


class DBStream:
    def __init__(self):
        self.dbstream = cluster.DBSTREAM(
            clustering_threshold=clustering_threshold,
            fading_factor=fading_factor,
            cleanup_interval=cleanup_interval,
            intersection_factor=intersection_factor,
            minimum_weight=minimum_weight,
        )

    def predict_clusters(self, embeddings: list[list[float]]) -> list[int]:
        if not embeddings:
            return []
        try:
            for x, _ in stream.iter_array(embeddings):
                self.dbstream.learn_one(x)

            result = []

            for x in embeddings:
                input_data = dict(enumerate(x))
                result.append(self.dbstream.predict_one(input_data))

            return result
        except Exception as exc:
            logger.error(f"Failed to get data for {len(embeddings)} embeddings: {repr(exc)}")
            return [-1] * len(embeddings)
