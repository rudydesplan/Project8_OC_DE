# tests/test_transformations/test_temp_conversions.py

import pytest
from transform.transformations import f_to_c, inhg_to_hpa, inches_to_mm

def test_f_to_c_nominal():
    assert f_to_c(32) == pytest.approx(0.0)
    assert f_to_c(212) == pytest.approx(100.0)

@pytest.mark.parametrize("value", [None, "", "null"])
def test_f_to_c_none_cases(value):
    assert f_to_c(value) is None

def test_inhg_to_hpa():
    assert inhg_to_hpa(1.0) == pytest.approx(33.8639, abs=0.01)

def test_inches_to_mm():
    assert inches_to_mm(1) == pytest.approx(25.4, abs=0.01)
