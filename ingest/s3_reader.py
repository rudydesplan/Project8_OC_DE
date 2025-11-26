# ingest/s3_reader.py

import re
import json
from loguru import logger
from ingest.s3_client import S3Client


class S3JSONLReader:
    """
    Reads JSONL files from S3 and routes parsing based on file origin.

    Handles:
    - Weather Underground format (Ichtegem, La Madeleine)
    - InfoClimat format (hourly nested per station)
    """

    def __init__(self, config_path: str = "config/s3_config.yaml"):
        self.s3 = S3Client(config_path=config_path)

    # ------------------------------------------------------------------
    def detect_source(self, key: str) -> str:
        """
        Detect the upstream source from the S3 key.
        """
        key_lower = key.lower()

        if "ichtegem" in key_lower or "la_madeleine" in key_lower:
            return "wunderground"

        if "infoclimat" in key_lower:
            return "infoclimat"

        logger.warning(f"Unknown source for key: {key}, fallback to raw _airbyte_data")
        return "unknown"

    # ------------------------------------------------------------------
    @staticmethod
    def clean_value(value):
        """
        Cleans raw strings:
        - removes non-breaking spaces (\xa0)
        - strips leading/trailing spaces
        - collapses multiple spaces
        """
        if value is None:
            return None

        if isinstance(value, str):
            value = value.replace("\xa0", " ")
            value = re.sub(r"\s+", " ", value)
            return value.strip()

        return value

    # ------------------------------------------------------------------
    def parse_wunderground(self, data: dict) -> dict:
        """
        Normalize Weather Underground row → staging shape (strings only).
        """
        c = self.clean_value

        return {
            "time_local": c(data.get("Time")),
            "temperature_F": c(data.get("Temperature")),
            "dew_point_F": c(data.get("Dew Point")),
            "humidity_pct": c(data.get("Humidity")),

            "wind_direction_text": c(data.get("Wind")),
            "wind_speed_mph": c(data.get("Speed")),
            "wind_gust_mph": c(data.get("Gust")),

            "pressure_inHg": c(data.get("Pressure")),

            "precip_rate_in": c(data.get("Precip. Rate.")),
            "precip_accum_in": c(data.get("Precip. Accum.")),

            "uv_index": c(data.get("UV")),
            "solar_wm2": c(data.get("Solar")),
        }

    # ------------------------------------------------------------------
    def parse_infoclimat(self, row: dict) -> dict:
        """
        Normalize ONE InfoClimat hourly row → staging shape.

        row example:
        {
          "id_station": "07015",
          "dh_utc": "2024-10-05 00:00:00",
          "temperature": 11.2,
          "pression": 1013.7,
          ...
        }
        """
        c = self.clean_value

        return {
            # 🔹 CRITICAL: keep id_station from the row
            "id_station": c(row.get("id_station")),
            "dh_utc": c(row.get("dh_utc")),

            "temperature_C": c(row.get("temperature")),
            "pression_hPa": c(row.get("pression")),
            "humidite_pct": c(row.get("humidite")),
            "point_de_rosee_C": c(row.get("point_de_rosee")),
            "visibilite_m": c(row.get("visibilite")),

            "vent_moyen_kmh": c(row.get("vent_moyen")),
            "vent_rafales_kmh": c(row.get("vent_rafales")),
            "vent_direction_deg": c(row.get("vent_direction")),

            "pluie_3h_mm": c(row.get("pluie_3h")),
            "pluie_1h_mm": c(row.get("pluie_1h")),
            "neige_au_sol_cm": c(row.get("neige_au_sol")),

            # okta can be int-like or float-like; staging keeps it as string
            "nebulosite_okta": c(row.get("nebulosite")),
            # WMO present-weather code
            "temps_omm_code": c(row.get("temps_omm")),
        }

    # ------------------------------------------------------------------
    def iter_records(self, key: str):
        """
        Streams JSONL lines from S3, detects the source,
        extracts _airbyte_data and yields normalized staging dicts.
        """
        source = self.detect_source(key)
        logger.info(f"Detected source '{source}' for file: {key}")

        for line in self.s3.stream_jsonl_lines(key):
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON line in {key}: {line[:200]}")
                continue

            if "_airbyte_data" not in raw:
                logger.warning(f"Missing _airbyte_data in line for {key}")
                continue

            data = raw["_airbyte_data"]

            if source == "wunderground":
                # each line is already one hourly row
                yield self.parse_wunderground(data)

            elif source == "infoclimat":
                hourly = data.get("hourly", {})

                if not hourly:
                    logger.warning(f"No 'hourly' block in InfoClimat payload for {key}")
                    continue

                # hourly is a dict: { '07015': [rows...], 'STATIC0010': [rows...], '_params': {...} }
                for station_code, rows in hourly.items():
                    if station_code == "_params":
                        continue
                    if not isinstance(rows, list):
                        logger.warning(
                            f"Hourly[{station_code}] for {key} is not a list, skipping."
                        )
                        continue

                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        # row already has id_station + dh_utc + measurements
                        yield self.parse_infoclimat(row)

            else:
                # Fallback: just yield raw _airbyte_data
                yield data
