from models.metadata_model import MetadataModel

def test_metadata_default_id():
    m = MetadataModel()
    assert m.id == "infoclimat"

def test_metadata_accepts_strings():
    m = MetadataModel(temperature="12.5", vent_moyen="10")
    assert m.temperature == "12.5"
    assert m.vent_moyen == "10"
