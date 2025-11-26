# tests/test_transformations/test_wind_speed.py

import pytest
from transform.transformations import mph_to_kmh

def test_mph_to_kmh_nominal():
    assert mph_to_kmh(1) == pytest.approx(1.609, abs=0.001)

@pytest.mark.parametrize("value", [None, "", "null"])
def test_mph_to_kmh_none(value):
    assert mph_to_kmh(value) is None
