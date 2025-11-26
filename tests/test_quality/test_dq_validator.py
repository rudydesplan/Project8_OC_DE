import pytest
import pandas as pd
from datetime import datetime
from quality.dq_validator import DataQualityValidator
from pandera.errors import SchemaErrors


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

class FakeS3Reader:
    """
    Only used to test detect_source(s3_key) via real S3JSONLReader rules.
    We override only detect_source() but ignore actual S3 calls.
    """
    def __init__(self):
        pass

    def detect_source(self, key: str):
        if "infoclimat" in key.lower():
            return "infoclimat"
        if "wunderground" in key.lower():
            return "wunderground"
        return None


@pytest.fixture
def dq(fake_mongo, monkeypatch):
    """
    Build real DataQualityValidator but override the internal S3 reader
    with a simplified FakeS3Reader to avoid Moto + network calls.
    """
    validator = DataQualityValidator(fake_mongo)

    # override S3 reader
    monkeypatch.setattr(validator, "s3_reader", FakeS3Reader())

    return validator


# -------------------------------------------------------------------
# stringify_keys
# -------------------------------------------------------------------

def test_stringify_keys_nested():
    """
    Numeric dict keys MUST be converted to strings.
    """
    row = {
        0: {"a": 1, 2: {"b": 3}},
        "x": [{4: 5}]
    }

    res = DataQualityValidator.stringify_keys(row)

    assert "0" in res
    assert "2" in res["0"]
    assert "4" in res["x"][0]


# -------------------------------------------------------------------
# Infoclimat schema VALID rows
# -------------------------------------------------------------------

def test_validate_file_infoclimat_valid(dq, fake_mongo):
    """
    Fully valid row → dq_checked=True, error=None, file marked validated.
    """

    # Insert ingestion record
    fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).insert_one(
        {"s3_key": "infoclimat_001.jsonl", "success": True}
    )

    # Valid staging row (Infoclimat schema)
    fake_mongo.get_collection(fake_mongo.settings.staging_collection).insert_one({
        "s3_key": "infoclimat_001.jsonl",
        "id_station": "ST01",
        "dh_utc": pd.Timestamp("2024-01-01 00:00:00"),
        "temperature_C": "10",
        "pression_hPa": "1000",
        "humidite_pct": "50",
        "point_de_rosee_C": "5",
        "vent_moyen_kmh": "12",
        "vent_rafales_kmh": "20",
        "visibilite_m": "10000",
        "neige_au_sol_cm": "0",
        "nebulosite_okta": "5",
        "vent_direction_deg": "180",
        "pluie_3h_mm": "1",
        "pluie_1h_mm": "0",
        "temps_omm_code": "SKC",
    })

    dq.validate_file("infoclimat_001.jsonl", "infoclimat")

    row = fake_mongo.get_collection(fake_mongo.settings.staging_collection).find_one({})
    assert row["dq_checked"] is True
    assert row["error"] is None

    file_rec = fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).find_one({})
    assert file_rec["dq_validated"] is True
    assert "dq_run_at" in file_rec


# -------------------------------------------------------------------
# Infoclimat INVALID rows
# -------------------------------------------------------------------

def test_validate_file_infoclimat_invalid(dq, fake_mongo):
    """
    Invalid Infoclimat row → dq_checked=False, error=True, file invalid.
    """

    fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).insert_one(
        {"s3_key": "infoclimat_bad.jsonl", "success": True}
    )

    # Bad numeric value (pression_hPa out of range)
    fake_mongo.get_collection(fake_mongo.settings.staging_collection).insert_one({
        "s3_key": "infoclimat_bad.jsonl",
        "id_station": "ST01",
        "dh_utc": pd.Timestamp("2024-01-01 00:00:00"),
        "temperature_C": "10",
        "pression_hPa": "5000",  # ❌ invalid
        "humidite_pct": "50",
        "point_de_rosee_C": "5",
        "vent_moyen_kmh": "12",
        "vent_rafales_kmh": "20",
        "visibilite_m": "10000",
        "neige_au_sol_cm": "0",
        "nebulosite_okta": "5",
        "vent_direction_deg": "180",
        "pluie_3h_mm": "1",
        "pluie_1h_mm": "0",
        "temps_omm_code": "SKC",
    })

    dq.validate_file("infoclimat_bad.jsonl", "infoclimat")

    row = fake_mongo.get_collection(fake_mongo.settings.staging_collection).find_one({})
    assert row["dq_checked"] is False
    assert row["error"] is True

    file_rec = fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).find_one({})
    assert file_rec["dq_validated"] is False


# -------------------------------------------------------------------
# Wunderground schema VALID row
# -------------------------------------------------------------------

def test_validate_file_wunderground_valid(dq, fake_mongo):
    fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).insert_one(
        {"s3_key": "wunderground_001.jsonl", "success": True}
    )

    fake_mongo.get_collection(fake_mongo.settings.staging_collection).insert_one({
        "s3_key": "wunderground_001.jsonl",
        "id_station": "ST01",
        "time_local": "12:04 AM",
        "temperature_F": "50 °F",
        "dew_point_F": "40 °F",
        "humidite_pct": "50 %",
        "pressure_inHg": "29.92 in",
        "wind_speed_mph": "5 mph",
        "wind_gust_mph": "10 mph",
        "precip_rate_in": "0 in",
        "precip_accum_in": "0 in",
        "solar_wm2": "50 w/m²",
        "uv_index": "3",
        "wind_direction_text": "NW",
    })

    dq.validate_file("wunderground_001.jsonl", "wunderground")

    row = fake_mongo.get_collection(fake_mongo.settings.staging_collection).find_one({})
    assert row["dq_checked"] is True
    assert row["error"] is None

    file_rec = fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).find_one({})
    assert file_rec["dq_validated"] is True


# -------------------------------------------------------------------
# Wunderground INVALID row
# -------------------------------------------------------------------

def test_validate_file_wunderground_invalid(dq, fake_mongo):
    fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).insert_one(
        {"s3_key": "wunderground_bad.jsonl", "success": True}
    )

    # invalid: wind_gust_mph < wind_speed_mph
    fake_mongo.get_collection(fake_mongo.settings.staging_collection).insert_one({
        "s3_key": "wunderground_bad.jsonl",
        "id_station": "ST01",
        "time_local": "12:04 AM",
        "temperature_F": "50 °F",
        "dew_point_F": "40 °F",
        "humidite_pct": "50 %",
        "pressure_inHg": "29.92 in",
        "wind_speed_mph": "10 mph",
        "wind_gust_mph": "5 mph",   # ❌ invalid
        "precip_rate_in": "0 in",
        "precip_accum_in": "0 in",
        "solar_wm2": "50 w/m²",
        "uv_index": "3",
        "wind_direction_text": "NW",
    })

    dq.validate_file("wunderground_bad.jsonl", "wunderground")

    row = fake_mongo.get_collection(fake_mongo.settings.staging_collection).find_one({})
    assert row["dq_checked"] is False
    assert row["error"] is True

    file_rec = fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).find_one({})
    assert file_rec["dq_validated"] is False


# -------------------------------------------------------------------
# Missing rows (should not crash)
# -------------------------------------------------------------------

def test_validate_file_no_staging(dq, fake_mongo, caplog):
    fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).insert_one(
        {"s3_key": "infoclimat_empty.jsonl", "success": True}
    )

    dq.validate_file("infoclimat_empty.jsonl", "infoclimat")

    # Should not throw exceptions, simply log warning
    file_rec = fake_mongo.get_collection(fake_mongo.settings.ingestion_tracker_collection).find_one({})
    assert file_rec.get("dq_validated") in (None, False)
