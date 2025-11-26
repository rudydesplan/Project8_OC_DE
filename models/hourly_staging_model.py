# models/hourly_staging_model.py
from datetime import datetime, date
from pydantic import BaseModel, Field
from typing import Optional


class HourlyStagingModel(BaseModel):
    id_station: str
    s3_key: str = Field(...)
    dq_checked: bool = False
    
    dh_utc: Optional[datetime] = None
    time_local: Optional[str] = None

    temperature_C: Optional[str] = None
    pression_hPa: Optional[str] = None
    humidite_pct: Optional[str] = None
    
    point_de_rosee_C: Optional[str] = None
    visibilite_m: Optional[str] = None

    vent_moyen_kmh: Optional[str] = None
    vent_rafales_kmh: Optional[str] = None
    vent_direction_deg: Optional[str] = None

    pluie_3h_mm: Optional[str] = None
    pluie_1h_mm: Optional[str] = None
    neige_au_sol_cm: Optional[str] = None
    nebulosite_okta: Optional[str] = None
    temps_omm_code: Optional[str] = None
    
    uv_index: Optional[str] = None
    solar_wm2: Optional[str] = None

    temperature_F: Optional[str] = None
    dew_point_F: Optional[str] = None
    humidity_pct: Optional[str] = None

    wind_direction_text: Optional[str] = None
    wind_speed_mph: Optional[str] = None
    wind_gust_mph: Optional[str] = None

    pressure_inHg: Optional[str] = None

    precip_rate_in: Optional[str] = None
    precip_accum_in: Optional[str] = None

