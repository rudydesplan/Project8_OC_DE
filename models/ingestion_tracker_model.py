# models/ingestion_tracker_model.py
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class IngestionTrackerModel(BaseModel):
    """
    Full ingestion tracker document stored in MongoDB.
    Used for initial creation (upsertOnInsert).
    """

    s3_key: str

    first_seen: datetime = Field(default_factory=datetime.utcnow)
    last_processed: datetime = Field(default_factory=datetime.utcnow)

    success: bool = True
    error_message: Optional[str] = None

    lines_read: Optional[int] = None
    file_hash: Optional[str] = None
    
    dq_validated: bool = False
    dq_run_at: Optional[datetime] = None


class IngestionTrackerUpdate(BaseModel):
    """
    Only the fields that can be modified during ingestion.
    """

    last_processed: datetime = Field(default_factory=datetime.utcnow)
    success: bool
    error_message: Optional[str] = None

    lines_read: Optional[int] = None
    file_hash: Optional[str] = None
    
    dq_validated: Optional[bool] = None
    dq_run_at: Optional[datetime] = None