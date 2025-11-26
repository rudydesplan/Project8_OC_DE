import pytest
from datetime import datetime, timezone
from models.hourly_measurements_model import HourlyMeasurementsModel

def test_required_fields_ok():
    model = HourlyMeasurementsModel(
        id_station="ST001",
        dh_utc=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        s3_key="folder/file.json"
    )
    assert model.id_station == "ST001"
    assert model.s3_key == "folder/file.json"

def test_missing_required_fields():
    with pytest.raises(Exception):
        HourlyMeasurementsModel()

def test_accepts_float_fields():
    m = HourlyMeasurementsModel(
        id_station="ST",
        dh_utc=datetime.now(timezone.utc),
        s3_key="x",
        temperature_C=12.5,
        vent_moyen_kmh=10.2,
        nebulosite_okta=4
    )
    assert m.temperature_C == 12.5
    assert m.vent_moyen_kmh == 10.2
    assert m.nebulosite_okta == 4

def test_invalid_float_raises():
    with pytest.raises(Exception):
        HourlyMeasurementsModel(
            id_station="ST",
            dh_utc=datetime.now(timezone.utc),
            s3_key="x",
            temperature_C="NOT_A_FLOAT"
        )

def test_boundary_temperature_values():
    m = HourlyMeasurementsModel(
        id_station="ST",
        dh_utc=datetime.now(timezone.utc),
        s3_key="x",
        temperature_C=-273.15
    )
    assert m.temperature_C == pytest.approx(-273.15)
