import pytest
from models.hourly_staging_model import HourlyStagingModel

def test_minimal_valid_staging():
    m = HourlyStagingModel(id_station="ST", s3_key="file.json")
    assert m.id_station == "ST"
    assert m.dq_checked is False  # valeur par défaut

def test_missing_required_field():
    with pytest.raises(Exception):
        HourlyStagingModel(id_station="ST")   # s3_key absent

def test_optional_fields_accept_strings():
    m = HourlyStagingModel(
        id_station="ST",
        s3_key="x",
        temperature_C="12.5",
        vent_moyen_kmh="10",
        humidite_pct="80"
    )
    assert m.temperature_C == "12.5"
    assert m.vent_moyen_kmh == "10"
    assert m.humidite_pct == "80"

def test_boundary_time():
    m = HourlyStagingModel(
        id_station="ST",
        s3_key="x",
        time_local="23:59"
    )
    assert m.time_local == "23:59"
