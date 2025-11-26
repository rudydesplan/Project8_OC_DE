# database/stations_seed.py

STATIONS_SEED_DATA = [
    {
        "id": "ILAMAD25",
        "name": "La Madeleine",

        # Geographic coordinates
        "latitude": 50.659,       # decimal degrees
        "longitude": 3.070,
        "elevation": 23,          # int

        # InfoClimat metadata (missing → None)
        "type": None,             # personal stations are often not typed
        "city": "La Madeleine",
        "state": None,

        "hardware": "other",
        "software": "EasyWeatherPro_V5.1.6",

        # License metadata block
        "license": {
            "license": None,
            "url": None,
            "source": None,
            "metadonnees": None
        },
    },
    {
        "id": "IICHTE19",
        "name": "WeerstationBS",

        # Geographic coordinates
        "latitude": 51.092,
        "longitude": 2.999,
        "elevation": 15,           # int

        "type": None,
        "city": "Ichtegem",
        "state": None,

        "hardware": "other",
        "software": "EasyWeatherV1.6.6",

        "license": {
            "license": None,
            "url": None,
            "source": None,
            "metadonnees": None
        },
    }
]
