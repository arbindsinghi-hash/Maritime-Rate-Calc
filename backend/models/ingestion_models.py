"""
Ingestion pipeline data models.

PageExtract: per-page output from PDF parser.
IngestionResult: result of run_ingestion() DAG.
"""

from typing import List, Any

from pydantic import BaseModel, Field


class BBox(BaseModel):
    """Bounding box [x0, y0, x1, y1] in page coordinates."""
    x0: float
    y0: float
    x1: float
    y1: float


class PageExtract(BaseModel):
    """Text and bounding boxes for one PDF page."""
    page_number: int = Field(..., ge=1)
    text: str = ""
    bbox: List[Any] = Field(default_factory=list)  # list of bbox dicts or [x0,y0,x1,y1]


class IngestionResult(BaseModel):
    """Result of the offline ingestion DAG."""
    status: str = Field(..., description="success | partial | failed | low_confidence")
    rules_count: int = Field(0, ge=0)
    message: str = ""
    eval_metrics: dict = Field(default_factory=dict)
