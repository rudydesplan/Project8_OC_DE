import pytest
from ingest.s3_reader import S3JSONLReader

def test_detect_source_wunderground():
    r = S3JSONLReader()
    assert r.detect_source("Ichtegem_2024.jsonl") == "wunderground"
    assert r.detect_source("La_Madeleine_2024.jsonl") == "wunderground"

def test_detect_source_infoclimat():
    r = S3JSONLReader()
    assert r.detect_source("InfoClimat_2024.jsonl") == "infoclimat"

def test_detect_source_unknown():
    r = S3JSONLReader()
    assert r.detect_source("random/file.jsonl") == "unknown"
