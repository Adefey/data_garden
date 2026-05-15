import asyncio
import logging
import os
from datetime import datetime

from sqlalchemy import DateTime, String, asc, delete, desc, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from common_libs.models import NewsItemModel, NewsSourceModel

logger = logging.getLogger(__name__)

mariadb_host = os.environ.get("MARIADB_HOST")
mariadb_port = os.environ.get("MARIADB_PORT")
mariadb_user = os.environ.get("MARIADB_USER")
mariadb_password = os.environ.get("MARIADB_PASSWORD")
mariadb_database = os.environ.get("MARIADB_DATABASE")
database_dialect = os.environ.get("DATABASE_DIALECT")
news_table_name = os.environ.get("NEWS_TABLE_NAME")
sources_table_name = os.environ.get("SOURCES_TABLE_NAME")
max_latest_news_count = int(os.environ.get("MAX_LATEST_NEWS_COUNT"))
max_used_sources_count = int(os.environ.get("MAX_USED_SOURCES_COUNT"))
db_debug_logging = os.environ.get("DB_DEBUG_LOGGING", "False").lower() in (
    "true",
    "1",
    "t",
    "enabled",
)

mariadb_reconnect_wait = int(os.environ.get("MARIADB_RECONNECT_WAIT"))
mariadb_reconnect_retries = int(os.environ.get("MARIADB_RECONNECT_RETRIES"))


class Base(DeclarativeBase):
    pass


class NewsItem(Base):
    __tablename__ = news_table_name

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, nullable=False)
    news_key: Mapped[str] = mapped_column(String(1024), nullable=False, unique=True)
    title: Mapped[str] = mapped_column(String(1024), nullable=True)
    summary: Mapped[str] = mapped_column(String(4096), nullable=True)
    link: Mapped[str] = mapped_column(String(2048), nullable=False)
    timedate: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    language: Mapped[str] = mapped_column(String(64), nullable=True)
    cluster_id: Mapped[int] = mapped_column(nullable=True)


class Source(Base):
    __tablename__ = sources_table_name

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, nullable=False)
    link: Mapped[str] = mapped_column(String(2048), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(default=True, nullable=False)


class Database:

    def __init__(self):
        self.url = f"{database_dialect}://{mariadb_user}:{mariadb_password}@{mariadb_host}:{mariadb_port}/{mariadb_database}"
        self.engine = create_async_engine(self.url, echo=db_debug_logging)
        self.async_session = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def prepare_tables(self):
        for attempt in range(1, mariadb_reconnect_retries + 1):
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("Tables created successfully")
                return
            except Exception as exc:
                logger.warning(f"Attempt {attempt}/{mariadb_reconnect_retries} failed: {exc}")
                if attempt == mariadb_reconnect_retries:
                    logger.error("All retries exhausted, tables were not created")
                    raise
                await asyncio.sleep(mariadb_reconnect_wait)

    async def tables_empty(self):
        try:
            async with self.async_session() as session:
                async with session.begin():
                    news_count_statement = select(func.count(NewsItem.id))
                    sources_count_statement = select(func.count(Source.id))
                    news_count = await session.scalar(news_count_statement)
                    sources_count = await session.scalar(sources_count_statement)
                    return news_count == sources_count == 0
        except Exception as exc:
            logger.error(f"Exception in tables_empty: {exc}")
            return True

    async def add_sources(self, sources: list[NewsSourceModel]):
        try:
            async with self.async_session() as session:
                async with session.begin():
                    session.add_all([
                        Source(link=source.link, is_enabled=source.is_enabled) for source in sources
                    ])
        except Exception as exc:
            logger.error(f"Exception in add_sources: {exc}")

    async def get_sources(
        self, limit: int = max_used_sources_count, offset: int = 0
    ) -> list[NewsSourceModel]:
        try:
            async with self.async_session() as session:
                async with session.begin():
                    sources_select_statement = select(Source).offset(offset).limit(limit)
                    result = await session.scalars(sources_select_statement)
                    return [
                        NewsSourceModel(id=item.id, link=item.link, is_enabled=item.is_enabled)
                        for item in result
                    ]
        except Exception as exc:
            logger.error(f"Exception in get_sources: {exc}")
            return []

    async def delete_sources(self, sources: list[NewsSourceModel]):
        links = [item.link for item in sources]
        try:
            async with self.async_session() as session:
                async with session.begin():
                    sources_delete_statement = delete(Source).where(Source.link.in_(links))
                    await session.execute(sources_delete_statement)
        except Exception as exc:
            logger.error(f"Exception in delete_sources: {exc}")

    async def add_news_items(self, news_items: list[NewsItemModel]):
        try:
            async with self.async_session() as session:
                async with session.begin():
                    session.add_all([
                        NewsItem(
                            news_key=news_item.news_key,
                            title=news_item.title,
                            summary=news_item.summary,
                            link=news_item.link,
                            timedate=news_item.timedate,
                            language=news_item.language,
                            cluster_id=news_item.cluster_id,
                        )
                        for news_item in news_items
                    ])
        except Exception as exc:
            logger.error(f"Exception in add_news_items: {exc}")

    async def get_news_items(
        self,
        limit: int = max_latest_news_count,
        offset: int = 0,
        with_empty_cluster_id: bool = False,
        newest_first: bool = True,
    ) -> list[NewsItemModel]:
        try:
            async with self.async_session() as session:
                async with session.begin():
                    news_item_select_statement = select(NewsItem)

                    if with_empty_cluster_id:
                        news_item_select_statement = news_item_select_statement.where(
                            NewsItem.cluster_id.is_(None)
                        )

                    if newest_first:
                        news_item_select_statement = news_item_select_statement.order_by(
                            desc(NewsItem.timedate)
                        )
                    else:
                        news_item_select_statement = news_item_select_statement.order_by(
                            asc(NewsItem.timedate)
                        )

                    news_item_select_statement = news_item_select_statement.offset(offset).limit(
                        limit
                    )

                    result = await session.scalars(news_item_select_statement)
                    return [
                        NewsItemModel(
                            id=item.id,
                            news_key=item.news_key,
                            title=item.title,
                            summary=item.summary,
                            link=item.link,
                            timedate=item.timedate,
                            language=item.language,
                            cluster_id=item.cluster_id,
                        )
                        for item in result
                    ]
        except Exception as exc:
            logger.error(f"Exception in get_news_items: {exc}")
            return []

    async def news_key_exists(self, news_key: str) -> bool:
        try:
            async with self.async_session() as session:
                async with session.begin():
                    result = await session.scalar(
                        select(func.count(NewsItem.id)).where(NewsItem.news_key == news_key)
                    )
                    return result > 0
        except Exception as exc:
            logger.error(f"Exception in news_key_exists: {exc}")
            return False

    async def news_keys_exist(self, keys: list[str]) -> list[str]:
        if not keys:
            return []
        try:
            async with self.async_session() as session:
                async with session.begin():
                    keys_select_statement = select(NewsItem.news_key).where(
                        NewsItem.news_key.in_(keys)
                    )
                    result = await session.execute(keys_select_statement)
                    return result.scalars().all()
        except Exception as exc:
            logger.error(f"Exception in news_keys_exist: {exc}")
            return []

    async def set_cluster_ids_by_news_items_ids(
        self, news_id_to_cluster_id_mapping: list[tuple[str, int]]
    ):
        if not news_id_to_cluster_id_mapping:
            return
        try:
            async with self.async_session() as session:
                async with session.begin():
                    for news_key, cluster_id in news_id_to_cluster_id_mapping.items():
                        update_statement = (
                            update(NewsItem)
                            .where(NewsItem.news_key == news_key)
                            .values(cluster_id=cluster_id)
                        )
                        await session.execute(update_statement)
        except Exception as exc:
            logger.error(f"Exception in set_cluster_ids_by_news_items_ids: {exc}")
