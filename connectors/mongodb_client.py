# connectors/mongodb_client.py

from __future__ import annotations
from typing import Optional
from time import sleep
import os

from pydantic import BaseModel
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError, ConnectionFailure
from loguru import logger


# -------------------------------------------------------------------
# 1) Pydantic Settings: loads .env AND builds Mongo Atlas URI
# -------------------------------------------------------------------

class MongoSettings(BaseModel):
    """
    Pydantic-based environment configuration for MongoDB Atlas.
    """

    user: str
    password: str
    cluster: str
    appname: str
    database: str

    # --- ALL 5 COLLECTIONS USED IN THE PIPELINE ---
    stations_collection: str = "stations"
    metadata_collection: str = "metadata"
    ingestion_tracker_collection: str = "ingestion_tracker"
    staging_collection: str = "hourly_staging"
    final_collection: str = "hourly_measurements"

    def build_uri(self) -> str:
        """Construct a safe MongoDB Atlas SRV URI."""
        return (
            f"mongodb+srv://{self.user}:{self.password}"
            f"@{self.cluster}/?retryWrites=true&w=majority&appName={self.appname}"
        )

    @classmethod
    def from_env(cls):
        """Load MongoDB settings from environment variables."""
        return cls(
            user=os.getenv("MONGODB_USER"),
            password=os.getenv("MONGODB_PASSWORD"),
            cluster=os.getenv("MONGODB_CLUSTER"),
            appname=os.getenv("MONGODB_APPNAME"),
            database=os.getenv("MONGODB_DATABASE"),

            # --- Allow overrides but default to canonical collection names ---
            stations_collection=os.getenv("MONGODB_STATIONS_COLLECTION", "stations"),
            metadata_collection=os.getenv("MONGODB_METADATA_COLLECTION", "metadata"),
            ingestion_tracker_collection=os.getenv("MONGODB_INGESTION_TRACKER", "ingestion_tracker"),
            staging_collection=os.getenv("MONGODB_STAGING_COLLECTION", "hourly_staging"),
            final_collection=os.getenv("MONGODB_FINAL_COLLECTION", "hourly_measurements"),
        )


# -------------------------------------------------------------------
# 2) MongoDBClient: PyMongo with retry + ping + Loguru
# -------------------------------------------------------------------

class MongoDBClient:
    """
    MongoDB Atlas client using PyMongo 4.15.4 with retry logic.
    """

    def __init__(self, settings: MongoSettings):
        self.settings = settings
        self.client: Optional[MongoClient] = None
        self.db = None

    # -------------------------------------------------------------
    def connect(self, retries: int = 5, delay: int = 2):
        """Connect to MongoDB Atlas with retry & ping."""
        uri = self.settings.build_uri()
        logger.info(f"Connecting to MongoDB Atlas cluster: {self.settings.cluster}")

        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Attempt {attempt}/{retries}")

                self.client = MongoClient(uri, server_api=ServerApi("1"))
                self.db = self.client[self.settings.database]

                # Required for Atlas
                self.client.admin.command("ping")

                logger.success("Successfully connected to MongoDB Atlas!")
                return self.client

            except (PyMongoError, ConnectionFailure) as e:
                logger.error(f"MongoDB connection failed: {e}")

                if attempt == retries:
                    logger.critical("Max retries reached. Cannot connect to MongoDB.")
                    raise

                sleep(delay)

    # -------------------------------------------------------------
    def get_database(self):
        if self.db is None:
            self.connect()
        return self.db

    # -------------------------------------------------------------
    def get_collection(self, name: str):
        if self.db is None:
            self.connect()
        return self.db[name]

    # -------------------------------------------------------------
    def close(self):
        if self.client:
            self.client.close()
            logger.info("MongoDB client closed.")
