from datetime import datetime

from pydantic import BaseModel, Field


class NewsItemModel(BaseModel):
    id: int = Field(default=0)
    news_key: str
    title: str | None = Field(default=None)
    summary: str | None = Field(default=None)
    link: str
    timedate: datetime
    language: str | None = Field(default=None)
    cluster_id: int | None = Field(default=None)

    def get_text_value(self) -> str:
        parts = [self.title, self.summary]
        return " : ".join(part for part in parts if part)


class NewsSourceModel(BaseModel):
    id: int = Field(default=0)
    link: str
    is_enabled: bool = Field(default=True)
