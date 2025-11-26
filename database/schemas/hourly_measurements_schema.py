# database/schemas/hourly_measurements_schema.py

HOURLY_MEASUREMENTS_SCHEMA = {
        "bsonType": "object",
        "required": ["id_station", "dh_utc"],
        "properties": {
            
            "id_station": {"bsonType": "string"},
            "dh_utc": {"bsonType": "date"},
            "s3_key": {"bsonType": "string"},

            "temperature_C": {"bsonType": ["double", "null"]},
            "pression_hPa": {"bsonType": ["double", "null"]},
            "humidite_pct": {"bsonType": ["double", "null"]},
            "point_de_rosee_C": {"bsonType": ["double", "null"]},
            "visibilite_m": {"bsonType": ["double", "null"]},
                
            "vent_moyen_kmh": {"bsonType": ["double", "null"]},
            "vent_rafales_kmh": {"bsonType": ["double", "null"]},
            "vent_direction_deg": {"bsonType": ["double", "null"]},
                
            "pluie_3h_mm": {"bsonType": ["double", "null"]},
            "pluie_1h_mm": {"bsonType": ["double", "null"]},
            "neige_au_sol_cm": {"bsonType": ["double", "null"]},
                
            "precip_rate_mm": {"bsonType": ["double", "null"]},
            "precip_accum_mm": {"bsonType": ["double", "null"]},
                
            "nebulosite_okta": {"bsonType": ["int", "null"]},
            "temps_omm_code": {"bsonType": ["int", "null"]},

            "uv_index": {"bsonType": ["double", "null"]},
            "solar_wm2": {"bsonType": ["double", "null"]},
        },
}
