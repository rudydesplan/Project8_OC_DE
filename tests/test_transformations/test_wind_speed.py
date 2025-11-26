# tests/test_transformations/test_wind_direction.py

import pytest
from transform.transformations import convert_wind_direction


def test_direction_basic():
    assert convert_wind_direction("N") == 0.0
    assert convert_wind_direction("SW") == 225.0
    assert convert_wind_direction("East") == 90.0

def test_direction_invalid_returns_none():
    assert convert_wind_direction("INVALID") is None
    assert convert_wind_direction("") is None
    assert convert_wind_direction(None) is None
