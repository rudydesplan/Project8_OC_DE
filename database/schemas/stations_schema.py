# database/schemas/stations_schema.py

STATIONS_VALIDATOR = {
        "bsonType": "object",
        "required": ["id", "name"],
        "properties": {
            "id": {
                "bsonType": "string",
                "description": "Unique station identifier"
            },
            "name": {
                "bsonType": "string",
                "description": "Station display name"
            },

            # --- Geographic fields ---
            "latitude": {
                "bsonType": ["double", "null"],
                "description": "Latitude in decimal degrees"
            },
            "longitude": {
                "bsonType": ["double", "null"],
                "description": "Longitude in decimal degrees"
            },
            "elevation": {
                "bsonType": ["int", "null"],
                "description": "Elevation in meters"
            },

            # --- Station type and details ---
            "type": {
                "bsonType": ["string", "null"],
                "description": "Station type (official / WU / amateur)"
            },
            "city": {
                "bsonType": ["string", "null"],
                "description": "City where the station is located"
            },
            "state": {
                "bsonType": ["string", "null"],
                "description": "Administrative region / state"
            },

            # --- Hardware information ---
            "hardware": {
                "bsonType": ["string", "null"],
                "description": "Hardware brand/model"
            },
            "software": {
                "bsonType": ["string", "null"],
                "description": "Software used by the station"
            },

            # --- License object ---
            "license": {
                "bsonType": ["object", "null"],
                "properties": {
                    "license": {
                        "bsonType": ["string", "null"]
                    },
                    "url": {
                        "bsonType": ["string", "null"]
                    },
                    "source": {
                        "bsonType": ["string", "null"]
                    },
                    "metadonnees": {
                        "bsonType": ["string", "null"]
                    }
                }
            },
        }
}
