from __future__ import annotations
from typing import Optional, Dict
from datetime import datetime

from loguru import logger
from pymongo import ReturnDocument

from models.ingestion_tracker_model import (
    IngestionTrackerModel,
    IngestionTrackerUpdate,
)
from connectors.mongodb_client import MongoDBClient


class IngestionTracker:
    """
    Tracks ingestion state for each processed S3 JSONL file.
    """

    def __init__(self, mongo: MongoDBClient):
        self.mongo = mongo
        self.collection = mongo.get_collection(
            mongo.settings.ingestion_tracker_collection
        )

        self.collection.create_index("s3_key", unique=True)
        logger.info("Index ensured on ingestion_tracker.s3_key")

    # ----------------------------------------------------------------------
    # 🔍 LIST ALL KNOWN FILES (S3 keys)
    # ----------------------------------------------------------------------
    def list_known_files(self) -> set[str]:
        docs = self.collection.find({}, {"s3_key": 1})
        known = {d["s3_key"] for d in docs}
        logger.info(f"[TRACKER] Known files: {len(known)}")
        return known

    # ----------------------------------------------------------------------
    # 🔍 GET FILE HASH
    # ----------------------------------------------------------------------
    def get_file_hash(self, s3_key: str) -> Optional[str]:
        doc = self.collection.find_one({"s3_key": s3_key}, {"file_hash": 1})
        return doc.get("file_hash") if doc else None

    # ----------------------------------------------------------------------
    # CHECK IF SUCCESSFUL
    # ----------------------------------------------------------------------
    def was_successful(self, s3_key: str) -> bool:
        doc = self.collection.find_one({"s3_key": s3_key, "success": True})
        if doc:
            logger.info(f"[TRACKER] File already processed successfully: {s3_key}")
        return doc is not None

    # ----------------------------------------------------------------------
    # 🚀 START INGESTION (REGISTER FILE IF NEW)
    # ----------------------------------------------------------------------
    def start_ingestion(self, s3_key: str) -> IngestionTrackerModel:

        record = IngestionTrackerModel(
            s3_key=s3_key,
            success=False,
            error_message=None,
            lines_read=None,
            file_hash=None,
        )

        self.collection.update_one(
            {"s3_key": s3_key},
            {"$setOnInsert": record.model_dump()},
            upsert=True,
        )

        logger.info(f"[TRACKER] Started ingestion for {s3_key}")
        return record

    # ----------------------------------------------------------------------
    # ✨ SAFE UPDATE PAYLOAD (no None → no schema violation)
    # ----------------------------------------------------------------------
    def _safe_payload(self, update: IngestionTrackerUpdate) -> dict:
        return {k: v for k, v in update.model_dump().items() if v is not None}

    # ----------------------------------------------------------------------
    # ✅ MARK SUCCESS
    # ----------------------------------------------------------------------
    def mark_success(
        self,
        s3_key: str,
        lines_read: Optional[int],
        file_hash: Optional[str],
    ) -> Dict:

        update = IngestionTrackerUpdate(
            success=True,
            error_message=None,
            lines_read=lines_read,
            file_hash=file_hash,
        )

        payload = self._safe_payload(update)

        updated = self.collection.find_one_and_update(
            {"s3_key": s3_key},
            {"$set": payload},
            return_document=ReturnDocument.AFTER,
        )

        logger.success(f"[TRACKER] SUCCESS → {s3_key}")
        return updated

    # ----------------------------------------------------------------------
    # ❌ MARK FAILURE
    # ----------------------------------------------------------------------
    def mark_failure(self, s3_key: str, error_message: str) -> Dict:

        update = IngestionTrackerUpdate(
            success=False,
            error_message=error_message,
            lines_read=None,
            file_hash=None,
        )

        payload = self._safe_payload(update)

        updated = self.collection.find_one_and_update(
            {"s3_key": s3_key},
            {"$set": payload},
            return_document=ReturnDocument.AFTER,
        )

        logger.error(f"[TRACKER] FAILURE → {s3_key}: {error_message}")
        return updated

    # ----------------------------------------------------------------------
    # 🔁 RESET A FILE ENTRY
    # ----------------------------------------------------------------------
    def reset_file(self, s3_key: str):
        self.collection.delete_one({"s3_key": s3_key})
        logger.warning(f"[TRACKER] RESET: {s3_key} deleted from ingestion tracking.")

    # ----------------------------------------------------------------------
    # 📌 FILES STILL NOT FULLY PROCESSED
    # ----------------------------------------------------------------------
    def get_pending_or_failed(self):
        docs = list(self.collection.find({"success": False}))
        logger.info(f"[TRACKER] {len(docs)} file(s) pending or failed.")
        return docs
