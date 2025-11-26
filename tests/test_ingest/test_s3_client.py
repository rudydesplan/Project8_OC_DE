import pytest
from ingest.s3_client import S3Client
from botocore.exceptions import ClientError

# ----------------------------------------------------------------------
# LIST JSONL FILES
# ----------------------------------------------------------------------

def test_list_jsonl_files(monkeypatch, fake_s3_with_objects):
    """
    Le client doit lister les fichiers JSONL à partir du S3 mocké.
    """
    client = S3Client()
    monkeypatch.setattr(client, "s3", fake_s3_with_objects)

    files = client.list_jsonl_files()
    assert files == ["sources/a.jsonl", "sources/b.jsonl"]
    assert len(files) == 2


def test_list_jsonl_files_error(monkeypatch, fake_s3_fail):
    """
    Si le S3 mocké force une erreur AWS, list_jsonl_files doit lever ClientError.
    """
    client = S3Client()
    monkeypatch.setattr(client, "s3", fake_s3_fail)

    with pytest.raises(ClientError):
        client.list_jsonl_files()


# ----------------------------------------------------------------------
# STREAM JSONL LINES
# ----------------------------------------------------------------------

def test_stream_jsonl_lines(monkeypatch, fake_s3):
    """
    Récupération des lignes depuis get_object → Body.iter_lines().
    """
    client = S3Client()
    fake_s3.lines = [b"line1\n", b"line2\n"]
    monkeypatch.setattr(client, "s3", fake_s3)

    lines = list(client.stream_jsonl_lines("key"))
    assert lines == ["line1", "line2"]


def test_stream_jsonl_lines_error(monkeypatch, fake_s3_fail):
    """
    Si get_object() échoue, stream_jsonl_lines doit lever ClientError.
    """
    client = S3Client()
    monkeypatch.setattr(client, "s3", fake_s3_fail)

    with pytest.raises(ClientError):
        list(client.stream_jsonl_lines("key"))
