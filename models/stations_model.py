# models/stations_model.py
from pydantic import BaseModel, Field
from typing import Optional


class LicenseInfo(BaseModel):
    license: Optional[str] = Field(default=None)
    url: Optional[str] = Field(default=None)
    source: Optional[str] = Field(default=None)
    metadonnees: Optional[str] = Field(default=None)


class StationModel(BaseModel):
    id: str = Field(..., description="Station identifier")
    name: str = Field(..., description="Station name")

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    elevation: Optional[int] = None
    type: Optional[str] = None

    city: Optional[str] = None
    state: Optional[str] = None

    hardware: Optional[str] = None
    software: Optional[str] = None

    license: Optional[LicenseInfo] = None
