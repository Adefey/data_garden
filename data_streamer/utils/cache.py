import os

import redis.asyncio as redis

redis_host = os.environ.get("REDIS_HOST")
cache = redis.Redis(host=redis_host)
