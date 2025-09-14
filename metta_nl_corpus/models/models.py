from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel, Relationship


class TextPair(SQLModel, table=True):
    """Represents a premise-hypothesis text pair."""

    id: Optional[int] = Field(default=None, primary_key=True)
    premise_id: str = Field(index=True)
    hypothesis_id: str = Field(index=True)
    premise_text: str
    hypothesis_text: str
    language: str = Field(index=True)
    created_at: datetime = Field(default_factory=datetime.now)


class ProcessedExample(SQLModel, table=True):
    """Represents a processed example with tokenization and metadata."""

    id: Optional[int] = Field(default=None, primary_key=True)
    text_pair_id: int = Field(foreign_key="textpair.id")
    processed_at: datetime = Field(default_factory=datetime.now)

    tokens: list["Tokens"] = Relationship(back_populates="tokens")


class Tokens(SQLModel, table=True):
    """Represents tokens for a processed example."""

    id: Optional[int] = Field(default=None, primary_key=True)
    token_text: str
    token_position: int
    part_of_speech: Optional[str]
    created_at: datetime = Field(default_factory=datetime.now)

    processed_example: ProcessedExample | None = Relationship(
        back_populates="processedexample"
    )
