# models/metadata_model.py
from pydantic import BaseModel, Field
from typing import Optional


class MetadataModel(BaseModel):
    id: str = "infoclimat"
    
    temperature: Optional[str] = None
    pression: Optional[str] = None
    humidite: Optional[str] = None
    point_de_rosee: Optional[str] = None
    visibilite: Optional[str] = None

    vent_moyen: Optional[str] = None
    vent_rafales: Optional[str] = None
    vent_direction: Optional[str] = None

    pluie_3h: Optional[str] = None
    pluie_1h: Optional[str] = None
    neige_au_sol: Optional[str] = None
    nebulosite: Optional[str] = None
    temps_omm: Optional[str] = None
