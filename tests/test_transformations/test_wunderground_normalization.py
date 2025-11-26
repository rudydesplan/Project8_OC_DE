# tests/test_transformations/test_wunderground_normalization.py

import pytest
from datetime import date
from transform.transformations import transform_document

def test_wunderground_basic():
    doc = {
        "id_station": "WU01",
        "s3_key": "/Ichtegem_011024/",
        "time_local": "01:30 AM",
        "temperature_F": "50",
        "wind_speed_mph": "10",
    }

    out = transform_document(doc)

    assert out["id_station"] == "WU01"
    assert out["temperature_C"] == pytest.approx(10.0, abs=0.5)
    assert out["vent_moyen_kmh"] == pytest.approx(16.09, abs=0.1)
    assert out["dh_utc"] is not None
