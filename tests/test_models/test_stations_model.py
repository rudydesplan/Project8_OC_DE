import pytest
from models.stations_model import StationModel, LicenseInfo

def test_station_minimal():
    s = StationModel(id="ST", name="Station Test")
    assert s.id == "ST"
    assert s.name == "Station Test"

def test_station_missing_required():
    with pytest.raises(Exception):
        StationModel(id="ST")  # name manquant

def test_license_submodel():
    lic = LicenseInfo(license="CC-BY", url="http://example.com")
    s = StationModel(
        id="ST",
        name="Station",
        license=lic
    )
    assert s.license.license == "CC-BY"
    assert s.license.url == "http://example.com"

def test_station_types():
    s = StationModel(id="ST", name="X", latitude=45.2, elevation=100)
    assert s.latitude == 45.2
    assert s.elevation == 100
