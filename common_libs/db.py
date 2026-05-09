import os
from datetime import datetime

from sqlalchemy import DateTime, String, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from common_libs.models import NewsItemModel, NewsSourceModel

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
    cluster_id: Mapped[int] = mapped_column(default=0, nullable=False)


class Source(Base):
    __tablename__ = sources_table_name

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, nullable=False)
    link: Mapped[str] = mapped_column(String(2048), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(default=True, nullable=False)


class Database:

    def __init__(self):
        self.url = f"{database_dialect}://{mariadb_user}:{mariadb_password}@{mariadb_host}:{mariadb_port}/{mariadb_database}"
        self.engine = create_async_engine(self.url, echo=True)
        self.async_session = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def prepare_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def tables_empty(self):
        async with self.async_session() as session:
            async with session.begin():
                news_count_statement = select(func.count(NewsItem.id))
                sources_count_statement = select(func.count(Source.id))
                news_count = await session.scalar(news_count_statement)
                sources_count = await session.scalar(sources_count_statement)
                return news_count == sources_count == 0

    async def add_sources(self, sources: list[NewsSourceModel]):
        async with self.async_session() as session:
            async with session.begin():
                session.add_all(
                    [Source(link=source.link, is_enabled=source.is_enabled) for source in sources]
                )

    async def get_enabled_sources(
        self, limit: int = max_used_sources_count
    ) -> list[NewsSourceModel]:
        async with self.async_session() as session:
            async with session.begin():
                sources_select_statement = (
                    select(Source).where(Source.is_enabled == True).limit(limit)
                )
                result = await session.scalars(sources_select_statement)
                return [
                    NewsSourceModel(id=item.id, link=item.link, is_enabled=item.is_enabled)
                    for item in result
                ]

    async def add_news_items(self, news_items: list[NewsItemModel]):
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

    async def get_latest_news_items(
        self, limit: int = max_latest_news_count
    ) -> list[NewsItemModel]:
        async with self.async_session() as session:
            async with session.begin():
                news_item_select_statement = (
                    select(NewsItem).order_by(desc(NewsItem.timedate)).limit(limit)
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

    async def news_key_exists(self, news_key: str) -> bool:
        async with self.async_session() as session:
            async with session.begin():
                result = await session.scalars(
                    select(func.count()).where(NewsItem.news_key == news_key)
                )
                return bool(result.all())

    async def news_keys_exist(self, keys: list[str]) -> list[str]:
        if not keys:
            return []
        keys_select_statement = select(NewsItem.news_key).where(NewsItem.news_key.in_(keys))
        async with self.async_session() as session:
            result = await session.execute(keys_select_statement)
            return result.scalars().all()
