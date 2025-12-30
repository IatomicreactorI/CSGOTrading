"""Unified data models across multiple APIs."""

from typing import Optional
from pydantic import BaseModel

class OHLCVCandle(BaseModel):
    """Unified OHLCV candle data model."""
    open: float
    high: float
    low: float
    close: float
    volume: int
    date: str

class MediaNews(BaseModel):
    """Unified news item model."""
    title: str
    publish_time: str
    publisher: str
    link: Optional[str] = None
    summary: Optional[str] = None
    score: Optional[int] = None  
    num_comments: Optional[int] = None  