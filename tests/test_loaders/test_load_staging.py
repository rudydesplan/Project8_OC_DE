import pytest
import json

from loaders.load_staging import (
    ingest_file_to_staging,
    resolve_station_id,
)
from models.hourly_staging_model import HourlyStagingModel


# ============================================================
# resolve_station_id
# ============================================================

def test_resolve_station_id_ichtegem(fake_mongo):
    """
    Should return station id when filename contains 'ichtegem'.
    """
    stations = fake_mongo.get_collection(fake_mongo.settings.stations_collection)
    stations.insert_one({"city": "Ichtegem", "id": "ST_ICH"})

    station_id = resolve_station_id(fake_mongo, "Ichtegem_2025_09_01.jsonl")
    assert station_id == "ST_ICH"


def test_resolve_station_id_madeleine(fake_mongo):
    stations = fake_mongo.get_collection(fake_mongo.settings.stations_collection)
    stations.insert_one({"city": "La Madeleine", "id": "ST_LAM"})

    station_id = resolve_station_id(fake_mongo, "La_Madeleine_2025.jsonl")
    assert station_id == "ST_LAM"


def test_resolve_station_id_none(fake_mongo):
    """
    Should return None when filename doesn't contain a known city.
    """
    station_id = resolve_station_id(fake_mongo, "UnknownCity.jsonl")
    assert station_id is None


def test_resolve_station_id_missing_in_db(fake_mongo):
    """
    Should raise ValueError when city detected but no matching station in DB.
    """
    with pytest.raises(ValueError):
        resolve_station_id(fake_mongo, "Ichtegem_file.jsonl")


# ============================================================
# ingest_file_to_staging
# ============================================================

class FakeTracker:
    def __init__(self):
        self.started = []
        self.success = []
        self.failed = []

    def start_ingestion(self, key):
        self.started.append(key)

    def mark_success(self, s3_key, lines_read, file_hash):
        self.success.append((s3_key, lines_read, file_hash))

    def mark_failure(self, s3_key, error_message):
        self.failed.append((s3_key, error_message))


class FakeReader:
    def __init__(self, records, file_hash="HASH123"):
        self._records = records
        self._hash = file_hash

    def iter_records(self, key):
        for r in self._records:
            yield r

    @property
    def s3(self):
        class H:
            def __init__(self, h):
                self._h = h

            def compute_file_hash(self, key):
                return self._h

        return H(self._hash)


def test_ingest_file_to_staging_ok(fake_mongo):
    """
    Should validate and insert staging rows, apply station override,
    compute hash, and mark success.
    """
    # Insert station so resolve_station_id works
    stations = fake_mongo.get_collection(fake_mongo.settings.stations_collection)
    stations.insert_one({"city": "Ichtegem", "id": "STICH"})

    # Fake records
    records = [
        {"dh_utc": None, "temperature_C": "10"},
        {"dh_utc": None, "temperature_C": "12"},
    ]
    reader = FakeReader(records)

    tracker = FakeTracker()

    ingest_file_to_staging(
        "Ichtegem_2025.jsonl",
        reader,
        fake_mongo,
        tracker,
    )

    staging = fake_mongo.get_collection(fake_mongo.settings.staging_collection)
    docs = list(staging.find({}))

    assert len(docs) == 2
    assert docs[0]["id_station"] == "STICH"
    assert docs[0]["s3_key"] == "Ichtegem_2025.jsonl"

    # Tracker
    assert tracker.started == ["Ichtegem_2025.jsonl"]
    assert tracker.success == [("Ichtegem_2025.jsonl", 2, "HASH123")]
    assert tracker.failed == []


def test_ingest_file_to_staging_missing_station_id(fake_mongo):
    """
    Should fail if station_id cannot be inferred or provided.
    """
    records = [
        {"dh_utc": None, "temperature_C": "10"},  # missing id_station
    ]

    reader = FakeReader(records)
    tracker = FakeTracker()

    with pytest.raises(ValueError):
        ingest_file_to_staging(
            "UnknownCity.jsonl",
            reader,
            fake_mongo,
            tracker,
        )

    assert tracker.failed  # failure recorded


def test_ingest_file_to_staging_model_validation_error(fake_mongo):
    """
    Should catch model validation errors and mark failure.
    """
    # Insert station so resolve_station_id works
    stations = fake_mongo.get_collection(fake_mongo.settings.stations_collection)
    stations.insert_one({"city": "Ichtegem", "id": "STICH"})

    # Invalid record → missing required fields
    records = [
        {"temperature_C": 42},  # missing required s3_key once injected
    ]

    reader = FakeReader(records)
    tracker = FakeTracker()

    with pytest.raises(Exception):
        ingest_file_to_staging(
            "Ichtegem_2025.jsonl",
            reader,
            fake_mongo,
            tracker,
        )

    assert tracker.failed
