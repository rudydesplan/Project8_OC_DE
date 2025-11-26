import pytest
import json

from loaders.load_metadata import (
    extract_metadata_from_file,
    upsert_metadata,
)
from models.metadata_model import MetadataModel


# ============================================================
#  extract_metadata_from_file
# ============================================================

def test_extract_metadata_from_file__valid_metadata(fake_s3):
    """
    Should return metadata found under _airbyte_data.metadata.
    """
    fake_s3.lines = [
        json.dumps({
            "_airbyte_data": {
                "metadata": {
                    "id": "infoclimat",
                    "temperature": "12",
                    "vent_moyen": "5"
                }
            }
        }).encode("utf-8")
    ]

    class FakeReader:
        def __init__(self, s3):
            self.s3 = s3

    reader = FakeReader(fake_s3)

    metadata = extract_metadata_from_file(reader, "file.jsonl")

    assert metadata == {
        "id": "infoclimat",
        "temperature": "12",
        "vent_moyen": "5"
    }


def test_extract_metadata_from_file__fallback_top_level(fake_s3):
    """
    Should return metadata when metadata exists at top-level
    but _airbyte_data exists without metadata inside it.
    """
    fake_s3.lines = [
        json.dumps({
            "_airbyte_data": {},   # required, otherwise code continues
            "metadata": {
                "id": "infoclimat",
                "pression": "1013"
            }
        }).encode()
    ]

    class FakeReader:
        def __init__(self, s3):
            self.s3 = s3

    reader = FakeReader(fake_s3)

    metadata = extract_metadata_from_file(reader, "file.jsonl")

    assert metadata == {"id": "infoclimat", "pression": "1013"}



def test_extract_metadata_from_file__no_metadata_found(fake_s3):
    """
    Should raise ValueError when no metadata block exists.
    """
    fake_s3.lines = [
        json.dumps({"_airbyte_data": {"stations": []}}).encode()
    ]

    class FakeReader:
        def __init__(self, s3):
            self.s3 = s3

    reader = FakeReader(fake_s3)

    with pytest.raises(ValueError, match="No metadata block"):
        extract_metadata_from_file(reader, "file.jsonl")


# ============================================================
#  upsert_metadata
# ============================================================

def test_upsert_metadata__insert_new(fake_mongo):
    """
    Should insert a brand-new metadata document.
    """
    payload = {
        "id": "infoclimat",
        "temperature": "10",
        "humidite": "90"
    }

    upsert_metadata(fake_mongo, payload)
    collection = fake_mongo.get_collection(fake_mongo.settings.metadata_collection)

    doc = collection.find_one({"id": "infoclimat"})
    assert doc is not None
    assert doc["temperature"] == "10"
    assert doc["humidite"] == "90"


def test_upsert_metadata__update_existing(fake_mongo):
    """
    Should update only changed fields.
    """
    collection = fake_mongo.get_collection(fake_mongo.settings.metadata_collection)

    # Pre-existing document
    existing = {
        "id": "infoclimat",
        "temperature": "10",
        "humidite": "80"
    }
    collection.insert_one(existing)

    # New values: change humidite only
    new_payload = {
        "id": "infoclimat",
        "temperature": "10",
        "humidite": "92"
    }

    upsert_metadata(fake_mongo, new_payload)

    updated = collection.find_one({"id": "infoclimat"})
    assert updated["temperature"] == "10"
    assert updated["humidite"] == "92"


def test_upsert_metadata__no_changes(fake_mongo):
    """
    Should NOT perform any update when incoming metadata matches existing.
    """
    collection = fake_mongo.get_collection(fake_mongo.settings.metadata_collection)

    existing = {
        "id": "infoclimat",
        "temperature": "12",
        "vent_moyen": "3"
    }
    collection.insert_one(existing)

    new_payload = existing.copy()

    upsert_metadata(fake_mongo, new_payload)

    final_doc = collection.find_one({"id": "infoclimat"})
    assert final_doc == existing
