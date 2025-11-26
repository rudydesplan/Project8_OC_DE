import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from loguru import logger
import yaml
from pathlib import Path
from dotenv import load_dotenv
import os
import hashlib


class S3Client:
    """
    Secure wrapper around boto3 for listing and streaming S3 JSONL files.
    AWS credentials are loaded from environment variables.
    """

    def __init__(self, config_path: str = "config/s3_config.yaml"):

        # --- LOAD ENVIRONMENT VARIABLES ---
        load_dotenv()

        self.config = self._load_config(config_path)
        self.s3 = self._create_client()

        self.bucket = self.config["s3"]["bucket"]
        self.raw_prefix = self.config["s3"]["raw_prefix"]
        self.file_ext = self.config["s3"]["file_extension"]

    # ----------------------------------------------------------------------
    def _load_config(self, path: str):
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"S3 config not found: {path}")

        with open(path, "r") as f:
            return yaml.safe_load(f)

    # ----------------------------------------------------------------------
    def _create_client(self):
        """
        Create boto3 S3 client using env vars, profile or IAM role.
        """

        region = (
            os.getenv("AWS_DEFAULT_REGION")
            or self.config["aws"]["region"]
        )

        use_instance_role = (
            os.getenv("AWS_USE_INSTANCE_ROLE", "false").lower() == "true"
        )

        env_key = os.getenv("AWS_ACCESS_KEY_ID")
        env_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

        profile = os.getenv("AWS_PROFILE_NAME") or self.config["aws"].get("profile_name")

        retry_cfg = Config(
            region_name=region,
            retries={"max_attempts": 5, "mode": "adaptive"}
        )

        try:
            if env_key and env_secret:
                logger.info("Using AWS credentials from environment variables.")
                return boto3.client(
                    "s3",
                    region_name=region,
                    aws_access_key_id=env_key,
                    aws_secret_access_key=env_secret,
                    config=retry_cfg
                )

            if profile:
                logger.info(f"Using AWS profile '{profile}'")
                session = boto3.Session(profile_name=profile, region_name=region)
                return session.client("s3", config=retry_cfg)

            if use_instance_role:
                logger.info("Using IAM Role authentication")
                return boto3.client("s3", region_name=region, config=retry_cfg)

            logger.info("Using default AWS credentials chain")
            return boto3.client("s3", region_name=region, config=retry_cfg)

        except Exception as e:
            logger.error(f"S3 client creation failed: {e}")
            raise

    # ----------------------------------------------------------------------
    def list_jsonl_files(self):
        """List all .jsonl files under raw_prefix."""
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            keys = []

            for page in paginator.paginate(Bucket=self.bucket, Prefix=self.raw_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(self.file_ext):
                        keys.append(key)

            logger.info(f"Found {len(keys)} JSONL file(s) under prefix {self.raw_prefix}")
            return keys

        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            raise

    # ----------------------------------------------------------------------
    def stream_jsonl_lines(self, key: str):
        """Stream a JSONL file from S3."""
        logger.info(f"Streaming from S3: s3://{self.bucket}/{key}")

        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = obj["Body"]

            for raw_line in body.iter_lines():
                if not raw_line:
                    continue
                yield raw_line.decode("utf-8").rstrip("\n")

        except ClientError as e:
            logger.error(f"Error streaming file {key}: {e}")
            raise

    # ======================================================================
    #                          HASH COMPUTATION
    # ======================================================================

    def compute_file_hash(self, key: str) -> str:
        """
        Compute a reproducible SHA256 hash for an S3 object.
        Uses streaming so it works with any file size.

        Returns hex digest string.
        """
        logger.info(f"Computing SHA256 hash for s3://{self.bucket}/{key}")

        sha256 = hashlib.sha256()

        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = obj["Body"]

            # stream in chunks
            for chunk in iter(lambda: body.read(4096), b""):
                sha256.update(chunk)

            digest = sha256.hexdigest()
            logger.info(f"SHA256 hash for {key}: {digest}")

            return digest

        except Exception as e:
            logger.error(f"Error computing SHA256 for {key}: {e}")
            raise

    def get_file_etag(self, key: str) -> str | None:
        """
        Get ETag from S3 object metadata.
        WARNING: ETag == MD5 ONLY for non-multipart uploads.
        """
        try:
            obj = self.s3.head_object(Bucket=self.bucket, Key=key)
            etag = obj["ETag"].strip('"')
            logger.info(f"ETag for {key} = {etag}")
            return etag
        except Exception as e:
            logger.error(f"Could not fetch ETag for {key}: {e}")
            return None

    def get_md5_from_stream(self, key: str) -> str:
        """
        Compute a strict MD5 hash based on content stream.
        Works even on multipart uploads where ETag != MD5.
        """
        logger.info(f"Computing MD5 hash for s3://{self.bucket}/{key}")

        md5 = hashlib.md5()

        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = obj["Body"]

            for chunk in iter(lambda: body.read(4096), b""):
                md5.update(chunk)

            return md5.hexdigest()

        except Exception as e:
            logger.error(f"Error computing MD5 for {key}: {e}")
            raise
