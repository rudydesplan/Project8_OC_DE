# database/schemas/metadata_schema.py

METADATA_VALIDATOR = {
    "bsonType": "object",
    "required": ["id"],
    "properties": {
        "id": {"bsonType": "string"},

        "temperature":  {"bsonType": ["string", "null"]},
        "pression":     {"bsonType": ["string", "null"]},
        "humidite":     {"bsonType": ["string", "null"]},
        "point_de_rosee": {"bsonType": ["string", "null"]},
        "visibilite":   {"bsonType": ["string", "null"]},

        "vent_moyen":       {"bsonType": ["string", "null"]},
        "vent_rafales":     {"bsonType": ["string", "null"]},
        "vent_direction":   {"bsonType": ["string", "null"]},

        "pluie_3h":         {"bsonType": ["string", "null"]},
        "pluie_1h":         {"bsonType": ["string", "null"]},
        "neige_au_sol":     {"bsonType": ["string", "null"]},
        "nebulosite":       {"bsonType": ["string", "null"]},

        "temps_omm":        {"bsonType": ["string", "null"]},
    }
}
