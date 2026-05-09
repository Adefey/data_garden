from datetime import datetime

from pydantic import BaseModel, Field


class NewsItemModel(BaseModel):
    id: int = Field(default=0)
    news_key: str
    title: str | None
    summary: str | None
    link: str
    timedate: datetime
    language: str | None
    cluster_id: int = Field(default=0)


class NewsSourceModel(BaseModel):
    id: int = Field(default=0)
    link: str
    is_enabled: bool = Field(default=True)
