# database/schemas/ingestion_tracker_schema.py

INGESTION_TRACKER_SCHEMA = {
        "bsonType": "object",
        "required": ["s3_key", "first_seen", "last_processed"],
        "properties": {
            "s3_key": {"bsonType": "string"},
            "first_seen": {"bsonType": "date"},
            "last_processed": {"bsonType": "date"},

            "success": {"bsonType": "bool"},
            "error_message": {"bsonType": ["string", "null"]},

            "lines_read": {"bsonType": ["int", "null"]},
            "file_hash": {"bsonType": ["string", "null"]},
            
            "dq_validated": {"bsonType": "bool"},
            "dq_run_at": {"bsonType": ["date", "null"]},
        },
}