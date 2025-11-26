import os
import sys
import pytest
import json
import mongomock
import boto3
from botocore.exceptions import ClientError
from loguru import logger
from moto import mock_aws 
from pathlib import Path





# =====================================================================
#   1) MOTO S3 FIXTURES — REAL BOTO3 BEHAVIOR
# =====================================================================

@pytest.fixture
def moto_s3():
    """
    Provides fully mocked AWS S3 (Moto 5.x via mock_aws).
    Creates an empty test bucket.
    """
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)
        yield s3, bucket


@pytest.fixture
def moto_s3_with_jsonl(moto_s3):
    """
    Uploads one or more JSONL files into Moto S3.
    Returns: (s3_client, bucket_name)
    """
    s3, bucket = moto_s3

    files = {
        "raw/infoclimat_2024_01.jsonl": [
            json.dumps({"_airbyte_data": {"metadata": {"id": "infoclimat"}}}),
            json.dumps({"_airbyte_data": {"hourly": {}}}),
        ],
        "raw/wunderground_ichtegem_2024.jsonl": [
            json.dumps({"_airbyte_data": {"Temperature": "10"}}),
        ],
    }

    for key, lines in files.items():
        body = "\n".join(lines).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=key, Body=body)

    return s3, bucket


@pytest.fixture
def moto_s3_empty(moto_s3):
    """Empty bucket → for testing no results / no prefix matches."""
    return moto_s3


@pytest.fixture
def moto_s3_fail():
    """Simulates AWS outage."""
    class BrokenS3:
        def list_objects_v2(self, *a, **kw):
            raise Exception("AWS is down")

    return BrokenS3()


# =====================================================================
#   2) FAKE S3 — LIGHTWEIGHT, FOR UNIT TESTS (NO BOTO3)
# =====================================================================

class FakeS3Body:
    def __init__(self, lines):
        self._lines = [l.encode() if isinstance(l, str) else l for l in lines]

    def iter_lines(self):
        for l in self._lines:
            yield l


class FakePaginator:
    def __init__(self, files):
        self.files = files

    def paginate(self, Bucket, Prefix):
        # boto3 returns "Contents": [{"Key": "..."}]
        contents = [
            {"Key": key}
            for key in self.files.keys()
            if key.startswith(Prefix)
        ]
        return [{"Contents": contents}]

class FakeS3:
    """
    Fully mimics the subset of boto3 used by S3Client:
    - get_object
    - list_jsonl_files
    - stream_jsonl_lines
    """

    def __init__(self, files=None):
        # files: dict {"prefix/file.jsonl": [line1, line2, ...]}
        self.files = files or {}
        
    def get_paginator(self, operation):
        if operation != "list_objects_v2":
            raise NotImplementedError("Only list_objects_v2 is supported")
        return FakePaginator(self.files)

    def list_jsonl_files(self, prefix="", ext=".jsonl"):
        return [
            key for key in self.files.keys()
            if key.startswith(prefix) and key.endswith(ext)
        ]

    def get_object(self, Bucket, Key):
        if Key not in self.files:
            raise FileNotFoundError(f"FakeS3 missing file: {Key}")
        return {"Body": FakeS3Body(self.files[Key])}

    def stream_jsonl_lines(self, Key):
        if Key not in self.files:
            raise FileNotFoundError(f"FakeS3 missing file: {Key}")
        for line in self.files[Key]:
            yield line


@pytest.fixture
def fake_s3():
    """
    Lightweight backward-compatible fake matching older tests:
    - has attribute `.lines`
    - stream_jsonl_lines reads from .lines
    """
    class LegacyFakeS3:
        def __init__(self):
            self.lines = []

        def stream_jsonl_lines(self, key):
            for l in self.lines:
                yield l.decode()

        def get_object(self, Bucket, Key):
            return {"Body": FakeS3Body(self.lines)}

    return LegacyFakeS3()


@pytest.fixture
def fake_s3_single():
    """A single InfoClimat test file."""
    files = {
        "raw/infoclimat.jsonl": [
            json.dumps({"_airbyte_data": {"metadata": {"id": "infoclimat"}}})
        ]
    }
    return FakeS3(files)


@pytest.fixture
def fake_s3_multiple():
    """Two files under raw/ for list_jsonl tests."""
    files = {
        "raw/infoclimat.jsonl": ["{}", "{}"],
        "raw/wunderground.jsonl": ["{}", "{}"],
    }
    return FakeS3(files)


@pytest.fixture
def fake_s3_empty():
    """Bucket with no files."""
    return FakeS3(files={})

@pytest.fixture
def fake_s3_fail():
    """
    Fake a boto3 client that raises ClientError for both pagination and get_object.
    Compatible with S3Client.
    """
    class BrokenS3:
        # Needed by S3Client.list_jsonl_files()
        def get_paginator(self, *args, **kwargs):
            raise ClientError(
                error_response={"Error": {"Code": "500", "Message": "Simulated AWS failure"}},
                operation_name="ListObjectsV2",
            )

        # Needed by S3Client.stream_jsonl_lines()
        def get_object(self, *args, **kwargs):
            raise ClientError(
                error_response={"Error": {"Code": "404", "Message": "Not Found"}},
                operation_name="GetObject",
            )

    return BrokenS3()

@pytest.fixture
def fake_s3_with_objects():
    """
    Fake S3 containing two .jsonl files under the correct prefix
    defined in s3_config.yaml: raw_prefix = "sources/".
    """
    files = {
        "sources/a.jsonl": ["line1"],
        "sources/b.jsonl": ["line2"],
    }
    return FakeS3(files)


# =====================================================================
#   3) FAKE S3JSONLReader (inject FakeS3 into real reader)
# =====================================================================

class FakeReader:
    """
    Inject FakeS3 directly into real S3JSONLReader logic.
    Used for testing extract_metadata_from_file & iter_records.
    """
    def __init__(self, fake_s3):
        self.s3 = fake_s3


@pytest.fixture
def fake_reader(fake_s3_single):
    return FakeReader(fake_s3_single)


# =====================================================================
#   4) Mongo DB (your existing mock)
# =====================================================================

class FakeSettings:
    stations_collection = "stations"
    metadata_collection = "metadata"
    staging_collection = "staging"
    ingestion_tracker_collection = "ingestion_tracker"


class FakeMongoWrapper:
    def __init__(self):
        import mongomock
        self.client = mongomock.MongoClient()
        self.db = self.client["weather"]
        self.settings = FakeSettings()

        self.db[self.settings.ingestion_tracker_collection].create_index(
            "s3_key", unique=True
        )

    def get_collection(self, name: str):
        return self.db[name]
        
    def get_database(self):
        return self.db


@pytest.fixture
def fake_mongo():
    return FakeMongoWrapper()


@pytest.fixture
def tracker(fake_mongo):
    from ingest.ingestion_tracker import IngestionTracker
    return IngestionTracker(fake_mongo)

@pytest.fixture
def example_base_date():
    """
    Base date utilisée pour tester convert_time_local_to_utc.
    """
    from datetime import date
    return date(2024, 6, 1)
    
@pytest.fixture(scope="session", autouse=True)
def configure_loguru_for_tests():
    """
    Configure Loguru pour toute la session de tests :
    - JSONL global: tests/logs/test_session.jsonl
    - Logs jolis sur stdout (capturés par pytest)
    """
    os.makedirs("tests/logs", exist_ok=True)
    log_path = "tests/logs/test_session.jsonl"

    # Reset tous les handlers
    logger.remove()

    # 1) Fichier JSONL global
    logger.add(
        log_path,
        level="DEBUG",
        serialize=True,
        enqueue=False,
        backtrace=True,
        diagnose=True,
    )

    # 2) (Optionnel) sortie console lisible
    logger.add(
        sys.stdout,
        level="DEBUG",
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
    )

    logger.debug("Loguru initialized for test session.")
    yield
    logger.debug("Loguru test session finished.")