import pytest
from datetime import datetime
from models.ingestion_tracker_model import (
    IngestionTrackerModel, 
    IngestionTrackerUpdate
)

def test_tracker_defaults():
    t = IngestionTrackerModel(s3_key="folder/x.json")
    assert t.success is True
    assert t.first_seen <= datetime.utcnow()
    assert t.last_processed <= datetime.utcnow()
    assert t.dq_validated is False

def test_tracker_missing_key():
    with pytest.raises(Exception):
        IngestionTrackerModel()

def test_tracker_update_minimal():
    u = IngestionTrackerUpdate(success=True)
    assert u.success is True
    assert u.last_processed <= datetime.utcnow()

def test_tracker_update_invalid_type():
    with pytest.raises(Exception):
        IngestionTrackerUpdate(success="oops")  # bool attendu
