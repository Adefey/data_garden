import asyncio
import logging
import os

max_parallel_coros = int(os.environ.get("MAX_PARALLEL_COROS"))

logger = logging.getLogger(__name__)


async def gather_with_limit(*coros, max_coros: int = max_parallel_coros):

    logger.debug(f"Got {len(coros)} coros, using Semaphore({max_coros})")

    semaphore = asyncio.Semaphore(max_coros)

    async def sem_coro(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(coro) for coro in coros))
