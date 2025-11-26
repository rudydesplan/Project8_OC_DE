from ingest.s3_reader import S3JSONLReader

def test_parse_infoclimat_basic():
    r = S3JSONLReader()

    row = {
        "id_station": "07015",
        "dh_utc": "2024-10-05 00:00:00",
        "temperature": 11.2,
        "pression": 1013.7,
        "vent_moyen": 12,
        "nebulosite": 5
    }

    out = r.parse_infoclimat(row)

    assert out["id_station"] == "07015"
    assert out["temperature_C"] == 11.2
    assert out["pression_hPa"] == 1013.7
    assert out["nebulosite_okta"] == 5

def test_parse_infoclimat_missing_fields():
    r = S3JSONLReader()
    out = r.parse_infoclimat({})
    assert out["id_station"] is None
    assert out["temperature_C"] is None
