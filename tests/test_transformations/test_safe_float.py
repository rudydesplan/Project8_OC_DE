# tests/test_transformations/test_safe_float.py

import pytest
from transform.transformations import safe_float, safe_float2

@pytest.mark.parametrize("value, expected", [
    ("12.4", 12.4),
    ("12,4", 12.4),
    ("  14.02  ", 14.02),
    ("▓13.5", 13.5),
    ("-5.2", -5.2),
    ("- 5.2", -5.2),   # caractères parasites
    ("12W/m2", 12.0),
])
def test_safe_float_valid(value, expected):
    assert safe_float(value) == pytest.approx(expected)

@pytest.mark.parametrize("value", ["", None, "-", ".", "-.", ".-"])
def test_safe_float_invalid_returns_none(value):
    assert safe_float(value) is None

def test_safe_float2_numeric_and_commas():
    assert safe_float2("21,5") == pytest.approx(21.5)
    assert safe_float2("21.5") == pytest.approx(21.5)

def test_safe_float2_invalid():
    assert safe_float2("abc") is None
    assert safe_float2("▓") is None
