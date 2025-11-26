import json
import pytest
from ingest.s3_reader import S3JSONLReader


def test_iter_records_wunderground(monkeypatch, fake_s3):
    r = S3JSONLReader()

    fake_jsonl = [
        json.dumps({"_airbyte_data": {"Time": "01:00 AM", "Temperature": "50"}})
    ]

    # Remplace entièrement le client S3 par le fake
    fake_s3.lines = [l.encode() for l in fake_jsonl]
    monkeypatch.setattr(r, "s3", fake_s3)

    records = list(r.iter_records("Ichtegem_2024.jsonl"))
    assert len(records) == 1
    assert records[0]["temperature_F"] == "50"


def test_iter_records_infoclimat(monkeypatch, fake_s3):
    r = S3JSONLReader()

    fake_jsonl = [
        json.dumps({
            "_airbyte_data": {
                "hourly": {
                    "07015": [
                        {"id_station": "07015", "temperature": 11}
                    ]
                }
            }
        })
    ]

    fake_s3.lines = [l.encode() for l in fake_jsonl]
    monkeypatch.setattr(r, "s3", fake_s3)

    records = list(r.iter_records("InfoClimat_2024.jsonl"))
    assert len(records) == 1
    assert records[0]["temperature_C"] == 11


def test_iter_records_invalid_json(monkeypatch, fake_s3):
    r = S3JSONLReader()

    fake_s3.lines = [b"not a json"]
    monkeypatch.setattr(r, "s3", fake_s3)

    records = list(r.iter_records("file.jsonl"))
    assert records == []
