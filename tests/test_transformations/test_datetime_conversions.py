# tests/test_transformations/test_datetime_conversions.py

import pytest
from datetime import datetime, date
from zoneinfo import ZoneInfo
from transform.transformations import convert_time_local_to_utc

def test_convert_time_local_to_utc_midday(example_base_date):
    utc_dt = convert_time_local_to_utc("12:00 PM", example_base_date)
    assert utc_dt.tzinfo == ZoneInfo("UTC")
    # 12:00 Paris (CEST, UTC+2) = 10:00 UTC
    assert utc_dt.hour == 10

def test_convert_time_local_to_utc_invalid():
    assert convert_time_local_to_utc("BAD", date(2024,1,1)) is None
    assert convert_time_local_to_utc(None, None) is None
