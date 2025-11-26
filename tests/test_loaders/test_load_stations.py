import pytest
import json

from loaders.load_stations import (
    extract_stations_from_file,
    upsert_station,
)


# ============================================================
#  extract_stations_from_file
# ============================================================

def test_extract_stations_from_file_valid(fake_s3):
    """
    Should extract stations from first JSONL line under _airbyte_data.
    """
    fake_s3.lines = [
        json.dumps({
            "_airbyte_data": {
                "stations": [
                    {"id": "ST001", "name": "Station A"},
                    {"id": "ST002", "name": "Station B"}
                ]
            }
        }).encode()
    ]

    class FakeReader:
        def __init__(self, s3):
            self.s3 = s3

    reader = FakeReader(fake_s3)

    stations = extract_stations_from_file(reader, "file.jsonl")

    assert len(stations) == 2
    assert stations[0]["id"] == "ST001"
    assert stations[1]["name"] == "Station B"


def test_extract_stations_from_file_empty(fake_s3):
    """
    Should raise ValueError if no lines exist.
    """
    fake_s3.lines = []

    class FakeReader:
        def __init__(self, s3):
            self.s3 = s3

    reader = FakeReader(fake_s3)

    with pytest.raises(ValueError):
        extract_stations_from_file(reader, "file.jsonl")


# ============================================================
#  upsert_station
# ============================================================

def test_upsert_station_insert(fake_mongo):
    """
    Should insert new station when id not present.
    """
    collection = fake_mongo.get_collection(fake_mongo.settings.stations_collection)

    raw_station = {"id": "ST001", "name": "Station A"}
    upsert_station(collection, raw_station)

    doc = collection.find_one({"id": "ST001"})
    assert doc is not None
    assert doc["name"] == "Station A"


def test_upsert_station_update(fake_mongo):
    """
    Should update existing station when fields differ.
    """
    collection = fake_mongo.get_collection(fake_mongo.settings.stations_collection)

    collection.insert_one({"id": "ST001", "name": "Old Name"})

    new_station = {"id": "ST001", "name": "New Name"}
    upsert_station(collection, new_station)

    doc = collection.find_one({"id": "ST001"})
    assert doc["name"] == "New Name"


def test_upsert_station_no_change(fake_mongo):
    """
    Should NOT update when station is identical (after Pydantic normalization).
    """
    collection = fake_mongo.get_collection(fake_mongo.settings.stations_collection)

    raw = {"id": "ST001", "name": "Same Name"}

    # Insert raw minimal station — Mongo adds _id automatically
    collection.insert_one(raw.copy())

    # Upsert with same minimal data
    upsert_station(collection, raw)

    final = collection.find_one({"id": "ST001"})

    # Build what upsert_station EXPECTS the doc to look like
    from models.stations_model import StationModel
    expected_doc = StationModel.model_validate(raw).model_dump()

    # Compare all fields except _id
    for k, v in expected_doc.items():
        assert final[k] == v


def test_upsert_station_invalid(fake_mongo):
    """
    Should raise validation error for invalid station.
    (Missing required 'id' or 'name')
    """
    collection = fake_mongo.get_collection(fake_mongo.settings.stations_collection)

    with pytest.raises(Exception):
        upsert_station(collection, {"name": "NoID"})
