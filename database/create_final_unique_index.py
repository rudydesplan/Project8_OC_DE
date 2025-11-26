# create_final_unique_index.py

from connectors.mongodb_client import MongoSettings, MongoDBClient
from dotenv import load_dotenv

def create_unique_index() :

    load_dotenv()
    settings = MongoSettings.from_env()
    client = MongoDBClient(settings)
    db = client.get_database()

    final = db[settings.final_collection]

    final.create_index(
        [
            ("id_station", 1),
            ("dh_utc", 1),
            ("s3_key", 1),
        ],
        unique=True,
        name="unique_measurement_key"
    )
    
    mongo.close()
    print("Index unique créé !")

if __name__ == "__main__":
    create_unique_index()
