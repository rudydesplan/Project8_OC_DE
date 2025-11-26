# quality/wunderground_schema.py

import pandera.pandas as pa
from pandera.pandas import Column, Check
import re


# ===============================================================
# Helper: Extract numeric part from unit-based strings
# Example: "56.8 °F" → 56.8
#          "87 %" → 87
#          "5 mph" → 5
# ===============================================================
def to_float(series):
    """Extract numeric part from values like '11 mph', '85 %', '29.47 in'."""
    numeric = series.str.extract(r"(-?\d+(\.\d+)?)")[0]
    return numeric.astype(float)


# ===============================================================
# Regex rules EXACTLY from your Great Expectations YAML
# ===============================================================
REGEX_TIME_LOCAL = r"^[0-9]{1,2}:[0-9]{2} [AP]M$"
REGEX_DATE_UTC = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\+|\-)[0-9]{2}:[0-9]{2}$"
REGEX_TEMPERATURE_F = r"^-?\d+(\.\d+)?\s*[°º˚░]F$"
REGEX_HUMIDITY_PCT = r"^[0-9]+ %$"
REGEX_PRESSURE_IN = r"^[0-9]+(\.[0-9]+)? in$"
REGEX_WIND_MPH = r"^[0-9]+(\.[0-9]+)? mph$"
REGEX_PRECIP_IN = r"^[0-9]+(\.[0-9]+)? in$"
REGEX_SOLAR_WM2 = r"^[0-9]+(\.[0-9]+)? w/m²$"
REGEX_UV_INDEX = r"^[0-9]+(\.[0-9]+)?( UV)?$"

WIND_DIR_ALLOWED = [
    "N","NNE","NE","ENE","E","ESE","SE","SSE",
    "S","SSW","SW","WSW","W","WNW","NW","NNW" , "North" , "South" , "East" , "West"
]


# ===============================================================
# FINAL PANDERA SCHEMA (complete GE → Pandera translation)
# ===============================================================
wunderground_schema = pa.DataFrameSchema(
    {
        # -----------------------------------------------------------
        # STEP 1 — Column existence (done implicitly by declaring them)
        # STEP 2 — Only id_station must NOT be null
        # -----------------------------------------------------------
        "id_station": Column(str, nullable=False),

        # GE expects string "12:04 AM"
        "time_local": Column(
            str,
            nullable=True,
            regex=REGEX_TIME_LOCAL,
        ),

        # -----------------------------------------------------------
        # STEP 3 — Regex validation (exact match from GE)
        # -----------------------------------------------------------
        "temperature_F": Column(
            str,
            nullable=True,
            regex=REGEX_TEMPERATURE_F,
            checks=Check(lambda s: (to_float(s) >= -100) & (to_float(s) <= 140)),
        ),
        
        "dew_point_F": Column(
            str,
            nullable=True,
            regex=REGEX_TEMPERATURE_F,
            checks=Check(lambda s: (to_float(s) >= -95) & (to_float(s) <= 95)),
        ),

        "humidite_pct": Column(
            str,
            nullable=True,
            regex=REGEX_HUMIDITY_PCT,
            checks=Check(lambda s: (to_float(s) >= 0) & (to_float(s) <= 100)),
        ),

        "pressure_inHg": Column(
            str,
            nullable=True,
            regex=REGEX_PRESSURE_IN,
            checks=Check(lambda s: (to_float(s) >= 850/33.864) & (to_float(s) <= 1100/33.864)),
            # GE expects pressure 850–1100 hPa; 1 inHg = 33.864 hPa
        ),

        "wind_speed_mph": Column(
            str,
            nullable=True,
            regex=REGEX_WIND_MPH,
        ),

        "wind_gust_mph": Column(
            str,
            nullable=True,
            regex=REGEX_WIND_MPH,
        ),

        "precip_rate_in": Column(
            str,
            nullable=True,
            regex=REGEX_PRECIP_IN,
        ),
        
        "precip_accum_in": Column(
            str,
            nullable=True,
            regex=REGEX_PRECIP_IN,
        ),

        "solar_wm2": Column(
            str,
            nullable=True,
            regex=REGEX_SOLAR_WM2,
        ),

        "uv_index": Column(
            str,
            nullable=True,
            regex=REGEX_UV_INDEX,
        ),

        # -----------------------------------------------------------
        # STEP 4 — Categorical / Enumeration
        # -----------------------------------------------------------
        "wind_direction_text": Column(
            str,
            nullable=True,
            checks=Check.isin(WIND_DIR_ALLOWED),
        ),

        # -----------------------------------------------------------
        # Technical / ingestion fields
        # -----------------------------------------------------------
        "s3_key": Column(str, nullable=False),
    },

    # ---------------------------------------------------------------
    # STEP 5 — Compound uniqueness (id_station, time_local, date_utc)
    # Pandera can do this via DataFrame-level checks
    # ---------------------------------------------------------------
    checks=[
        # ----------------------------------------------
        # Unicité GE: (id_station, time_local)
        # ----------------------------------------------
        Check(
            lambda df: df[["id_station", "time_local"]]
            .drop_duplicates()
            .shape[0]
            == df.shape[0],
            error="Rows must be unique for (id_station, time_local)",
        ),

        # ----------------------------------------------
        # wind_gust_mph ≥ wind_speed_mph  (ROW-LEVEL)
        # ----------------------------------------------
        Check(
            lambda df: to_float(df["wind_gust_mph"]) >= to_float(df["wind_speed_mph"]),
            error="wind_gust_mph must be >= wind_speed_mph",
        ),
    ],

    strict=False,
)
