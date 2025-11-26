# tests/test_transformations/test_infoclimat_normalization.py

import pytest
from transform.transformations import transform_infoclimat

def test_infoclimat_basic_transform():
    doc = {
        "id_station": "IC001",
        "dh_utc": "2024-01-01T12:00:00Z",
        "temperature_C": "14,2",
        "vent_moyen_kmh": "15",
        "nebulosite_okta": "4",
    }

    out = transform_infoclimat(doc)

    assert out["id_station"] == "IC001"
    assert out["temperature_C"] == 14.2
    assert out["vent_moyen_kmh"] == 15.0
    assert out["nebulosite_okta"] == 4

def test_infoclimat_missing_values():
    doc = {"id_station": "IC001"}
    out = transform_infoclimat(doc)
    assert out["temperature_C"] is None
    assert out["nebulosite_okta"] is None
