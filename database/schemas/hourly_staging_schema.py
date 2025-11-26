# database/schemas/hourly_staging_schema.py

HOURLY_STAGING_SCHEMA = {
        "bsonType": "object",
        "required": ["id_station"],
        "properties": {
            "id_station": {"bsonType": "string"},
            "s3_key": {"bsonType": "string"},
            "dq_checked": {"bsonType": "bool"},
            "error" : {"bsonType": ["bool", "null"]},

            "dh_utc": { "bsonType": ["date", "null"] },
            "time_local": {"bsonType": ["string", "null"]},
            
            # --- Température & humidité ---
            "temperature_C": {"bsonType": ["string", "null"]},
            "pressure_hPa": {"bsonType": ["string", "null"]},
            "humidite_pct": {"bsonType": ["string", "null"]},
            "point_de_rosee_C": {"bsonType": ["string", "null"]},
            "visibility_m": {"bsonType": ["string", "null"]},

            # --- Vent ---
            "vent_moyen_kmh": {"bsonType": ["string", "null"]},
            "vent_rafales_kmh": {"bsonType": ["string", "null"]},
            "vent_direction_deg": {"bsonType": ["string", "null"]},

            # --- Précipitations ---
            "precip_3h_mm": {"bsonType": ["string", "null"]},
            "precip_1h_mm": {"bsonType": ["string", "null"]},
            "neige_au_sol_cm": {"bsonType": ["string", "null"]},

            # --- Ciel ---
            "nebulosite_okta": {"bsonType": ["string", "null"]},
            "temps_omm_code": {"bsonType": ["string", "null"]},

            # --- UV & Solar ---
            "uv_index": {"bsonType": ["string", "null"]},
            "solar_wm2": {"bsonType": ["string", "null"]},

            # --- Champs EN (convertis ensuite) ---
            "temperature_F": {"bsonType": ["string", "null"]},
            "dew_point_F": {"bsonType": ["string", "null"]},
            
            "humidity_pct": {"bsonType": ["string", "null"]},

            "wind_direction_text": {"bsonType": ["string", "null"]},
            "wind_speed_mph": {"bsonType": ["string", "null"]},
            "wind_gust_mph": {"bsonType": ["string", "null"]},

            "pressure_inHg": {"bsonType": ["string", "null"]},

            "precip_rate_in": {"bsonType": ["string", "null"]},
            "precip_accum_in": {"bsonType": ["string", "null"]},
        },
}
