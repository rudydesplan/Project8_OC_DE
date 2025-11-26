import pytest
from ingest.ingestion_tracker import IngestionTracker


def test_start_ingestion_creates(fake_mongo):
    tracker = IngestionTracker(fake_mongo)

    tracker.start_ingestion("A.jsonl")
    doc = tracker.collection.find_one({"s3_key": "A.jsonl"})

    assert doc is not None
    assert doc["success"] is False
    assert doc["file_hash"] is None


def test_start_ingestion_no_overwrite(fake_mongo):
    tracker = IngestionTracker(fake_mongo)

    tracker.collection.insert_one({
        "s3_key": "A.jsonl",
        "success": True,
        "file_hash": "XYZ",
    })

    tracker.start_ingestion("A.jsonl")

    doc = tracker.collection.find_one({"s3_key": "A.jsonl"})
    assert doc["file_hash"] == "XYZ"
    assert doc["success"] is True


def test_list_known_files(fake_mongo):
    tracker = IngestionTracker(fake_mongo)

    tracker.collection.insert_many([
        {"s3_key": "A", "success": True},
        {"s3_key": "B", "success": False},
    ])

    assert tracker.list_known_files() == {"A", "B"}


def test_get_file_hash(fake_mongo):
    tracker = IngestionTracker(fake_mongo)

    tracker.collection.insert_one({"s3_key": "A", "file_hash": "HASH"})
    assert tracker.get_file_hash("A") == "HASH"
    assert tracker.get_file_hash("B") is None


def test_was_successful(fake_mongo):
    tracker = IngestionTracker(fake_mongo)

    tracker.collection.insert_one({"s3_key": "A", "success": True})
    tracker.collection.insert_one({"s3_key": "B", "success": False})

    assert tracker.was_successful("A") is True
    assert tracker.was_successful("B") is False


def test_mark_success(fake_mongo):
    tracker = IngestionTracker(fake_mongo)

    tracker.start_ingestion("A")
    updated = tracker.mark_success("A", lines_read=10, file_hash="HASH")

    assert updated["success"] is True
    assert updated["lines_read"] == 10
    assert updated["file_hash"] == "HASH"
    assert updated["error_message"] is None


def test_mark_failure(fake_mongo):
    tracker = IngestionTracker(fake_mongo)

    tracker.start_ingestion("A")
    updated = tracker.mark_failure("A", "ERR")

    assert updated["success"] is False
    assert updated["error_message"] == "ERR"
    assert updated["lines_read"] is None


def test_reset_file(fake_mongo):
    tracker = IngestionTracker(fake_mongo)

    tracker.collection.insert_one({"s3_key": "A"})
    tracker.reset_file("A")

    assert tracker.collection.find_one({"s3_key": "A"}) is None


def test_get_pending_or_failed(fake_mongo):
    tracker = IngestionTracker(fake_mongo)

    tracker.collection.insert_many([
        {"s3_key": "A", "success": False},
        {"s3_key": "B", "success": True},
        {"s3_key": "C", "success": False},
    ])

    docs = tracker.get_pending_or_failed()
    keys = {d["s3_key"] for d in docs}
    assert keys == {"A", "C"}
