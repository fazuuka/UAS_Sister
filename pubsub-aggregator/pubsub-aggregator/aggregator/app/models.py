"""
Pydantic schemas untuk validasi event.
Event JSON minimal sesuai spesifikasi tugas.
"""
from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field, field_validator
import re


class EventSchema(BaseModel):
    topic: str = Field(..., min_length=1, max_length=255)
    event_id: str = Field(..., min_length=1, max_length=255)
    timestamp: datetime = Field(...)
    source: str = Field(..., min_length=1, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("topic")
    @classmethod
    def validate_topic(cls, v: str) -> str:
        if not re.match(r"^[a-z0-9][a-z0-9._-]*$", v):
            raise ValueError("topic harus lowercase alphanumeric, boleh mengandung . _ -")
        return v

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, v: str) -> str:
        stripped = v.strip()
        if len(stripped) < 8:
            raise ValueError("event_id terlalu pendek (minimal 8 karakter)")
        return stripped


class EventResponse(BaseModel):
    topic: str
    event_id: str
    source: str
    timestamp: datetime
    payload: dict[str, Any]
    received_at: datetime


class BatchPublishRequest(BaseModel):
    events: list[EventSchema] = Field(..., min_length=1, max_length=1000)


class PublishResponse(BaseModel):
    accepted: int
    duplicate_dropped: int
    queued: bool
    message: str


class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: list[str]   
    uptime_seconds: float
    workers_active: int = 0


# Legacy aliases kept for the existing application imports.
Event = EventSchema
EventBatch = BatchPublishRequest
