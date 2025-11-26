# models/hourly_measurements_model.py
from pydantic import BaseModel, field_validator, Field
from datetime import datetime, timezone
from typing import Optional


class HourlyMeasurementsModel(BaseModel):
    id_station: str
    dh_utc: datetime
    s3_key: str = Field(...)

    temperature_C: Optional[float] = None
    pression_hPa: Optional[float] = None
    humidite_pct: Optional[float] = None
    point_de_rosee_C: Optional[float] = None
    visibilite_m: Optional[float] = None

    vent_moyen_kmh: Optional[float] = None
    vent_rafales_kmh: Optional[float] = None
    vent_direction_deg: Optional[float] = None

    pluie_3h_mm: Optional[float] = None
    pluie_1h_mm: Optional[float] = None
    neige_au_sol_cm: Optional[float] = None
    nebulosite_okta: Optional[int] = None
    temps_omm_code: Optional[int] = None

    precip_rate_mm: Optional[float] = None
    precip_accum_mm: Optional[float] = None

    uv_index: Optional[float] = None
    solar_wm2: Optional[float] = None
