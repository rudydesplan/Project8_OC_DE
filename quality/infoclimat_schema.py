# quality/infoclimat_schema.py

import pandera.pandas as pa
from pandera.pandas import Column, Check


# ===============================================================
# Helpers
# ===============================================================
def to_float(series):
    """Extract numeric part and convert to float (no unit expected here)."""
    return series.str.extract(r"(-?\d+(\.\d+)?)")[0].astype(float)


# ===============================================================
# Regex formats
# ===============================================================
REGEX_DATETIME = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
REGEX_NUMBER = r"^-?\d+(\.\d+)?$"    # pure numeric (no units in Infoclimat)


# ===============================================================
# FINAL PANDERA SCHEMA FOR INFOCLIMAT
# ===============================================================
infoclimat_schema = pa.DataFrameSchema(
    {

        "dh_utc": Column(pa.DateTime, nullable=False),
        
        # ----------------------------------------------------------
        # Numeric meteorological parameters (strings but numeric only)
        # ----------------------------------------------------------
        "temperature_C": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
            checks=Check(lambda s: (to_float(s) >= -60) & (to_float(s) <= 60)),
        ),

        "pression_hPa": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
            checks=Check(lambda s: (to_float(s) >= 850) & (to_float(s) <= 1100)),
        ),

        "humidite_pct": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
            checks=Check(lambda s: (to_float(s) >= 0) & (to_float(s) <= 100)),
        ),

        "point_de_rosee_C": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
            checks=Check(lambda s: (to_float(s) >= -60) & (to_float(s) <= 60)),
        ),

        # ----------------------------------------------------------
        # Wind validation: **strict row-level rule**
        # ----------------------------------------------------------
        "vent_moyen_kmh": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
        ),

        "vent_rafales_kmh": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
        ),
        
        "visibilite_m": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
            checks=Check(lambda s: s.isna() | (to_float(s) >= 0)),
        ),
        
        
        "neige_au_sol_cm": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
            checks=Check(lambda s: s.isna() | (to_float(s) >= 0)),
        ),
        
        "nebulosite_okta": Column(
            str,
            nullable=True,
            checks=Check(
                lambda s: s[s.notna() & (s != "")]
                .astype(int)
                .between(0, 8)
                .all(),
                error="nebulosite_okta must be an integer between 0 and 8 when not null",
            ),
        ),

        # Numeric angle
        "vent_direction_deg": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
            checks=Check(lambda s: (to_float(s) >= 0) & (to_float(s) <= 360)),
        ),

        # ----------------------------------------------------------
        # Rainfall (null allowed)
        # ----------------------------------------------------------
        "pluie_3h_mm": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
            checks=Check(lambda s: s.isna() | (to_float(s) >= 0)),
        ),

        "pluie_1h_mm": Column(
            str,
            nullable=True,
            regex=REGEX_NUMBER,
            checks=Check(lambda s: s.isna() | (to_float(s) >= 0)),
        ),
        
        "temps_omm_code": Column(
            str,
            nullable=True,
        ), 

    },
    
    checks=[
        Check(
            lambda df: (
                df["vent_rafales_kmh"].isna()
                | df["vent_moyen_kmh"].isna()
                | (to_float(df["vent_rafales_kmh"]) >= to_float(df["vent_moyen_kmh"]))
            ),
            error="vent_rafales_kmh must be >= vent_moyen_kmh when both present",
        ),
        
        Check(
            lambda df: df[["id_station", "dh_utc"]]
            .drop_duplicates()
            .shape[0]
            == df.shape[0],
            error="Rows must be unique for (id_station, dh_utc)",
        ),
    ],    

    strict=False,   # allow additional fields (id_station, s3_key, etc.)
)
