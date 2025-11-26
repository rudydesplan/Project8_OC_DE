import pytest
from ingest.s3_reader import S3JSONLReader

def test_clean_value_basic():
    assert S3JSONLReader.clean_value("  abc  ") == "abc"

def test_clean_value_nbsp():
    assert S3JSONLReader.clean_value("a\xa0b") == "a b"

def test_clean_value_collapse_spaces():
    assert S3JSONLReader.clean_value("a   b   c") == "a b c"

def test_clean_value_none():
    assert S3JSONLReader.clean_value(None) is None

def test_clean_value_non_str():
    assert S3JSONLReader.clean_value(12.5) == 12.5
