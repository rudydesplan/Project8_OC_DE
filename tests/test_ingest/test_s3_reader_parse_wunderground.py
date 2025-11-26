from ingest.s3_reader import S3JSONLReader

def test_parse_wunderground_basic():
    r = S3JSONLReader()

    data = {
        "Time": "12:00 PM",
        "Temperature": "50",
        "Wind": "NW",
        "Speed": "10",
        "Gust": "20",
        "Humidity": "80",
        "Solar": "200",
    }

    out = r.parse_wunderground(data)

    assert out["temperature_F"] == "50"
    assert out["wind_speed_mph"] == "10"
    assert out["wind_direction_text"] == "NW"

def test_parse_wunderground_missing_fields():
    r = S3JSONLReader()
    out = r.parse_wunderground({})
    assert out["temperature_F"] is None
    assert out["wind_speed_mph"] is None
