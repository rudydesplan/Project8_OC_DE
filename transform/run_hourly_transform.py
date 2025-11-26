# run_hourly_transform.py

from loguru import logger
from datetime import datetime, UTC
from connectors.mongodb_client import MongoSettings, MongoDBClient
from transform.transformations import transform_infoclimat , transform_document
from models.hourly_measurements_model import HourlyMeasurementsModel

from dotenv import load_dotenv
load_dotenv()

def run_hourly_transform():
    start_time = datetime.now(UTC)

    logger.info("🚀 Starting HOURLY transformation job")

    # ------------------------------------------------------
    # 1) Chargement configuration + connexion Mongo
    # ------------------------------------------------------
    settings = MongoSettings.from_env()
    client = MongoDBClient(settings)

    try:
        db = client.get_database()
    except Exception as e:
        logger.exception("❌ Failed to connect to MongoDB")
        raise

    staging = db[settings.staging_collection]
    final = db[settings.final_collection]

    logger.info(
        f"Connected to DB='{settings.database}', "
        f"staging='{settings.staging_collection}', "
        f"final='{settings.final_collection}'"
    )

    # ------------------------------------------------------
    # 2) Récupération des documents à traiter
    # ------------------------------------------------------
    query = {
        "dq_checked": True,
        "$or": [{"error": None}, {"error": False}]
    }

    total_to_process = staging.count_documents(query)
    logger.info(f"📥 Documents matching query: {total_to_process}")

    cursor = staging.find(query)

    count_info = 0
    count_transformed = 0
    count_errors = 0

    # ------------------------------------------------------
    # 3) Boucle de traitement
    # ------------------------------------------------------
    for doc in cursor:
        try:
            s3_key = doc.get("s3_key", "UNKNOWN")

            # Logger enrichi avec contexte
            doc_logger = logger.bind(
                id_station=doc.get("id_station"),
                s3_key=s3_key
            )
            
            # ----- CAS 1 : InfoClimat  
            if "InfoClimat" in s3_key:
                transformed = transform_infoclimat(doc)
                
                try:
                    record = HourlyMeasurementsModel(**transformed)
                    final.insert_one(record.model_dump())
                    #doc_logger.debug("Inserted validated Pydantic record.")
                except Exception as e:
                    doc_logger.exception("❌ Pydantic validation failed")

                count_info += 1

                doc_logger.debug("📄 Copied InfoClimat document to final collection.")
                continue

            # ----- CAS 2 : Ichtegem / Madeleine → transformation -----
            if any(x in s3_key for x in ["Ichtegem", "Madeleine"]):
                transformed = transform_document(doc)

                try:
                    record = HourlyMeasurementsModel(**transformed)
                    final.insert_one(record.model_dump())
                    #doc_logger.debug("Inserted validated Pydantic record.")
                except Exception as e:
                    doc_logger.exception("❌ Pydantic validation failed")
                

                count_transformed += 1

                doc_logger.debug("🔄 Transformed & inserted Ichtegem/Madeleine document.")
                continue

        except Exception as e:
            count_errors += 1
            doc_logger.exception("❌ Error processing document.")

    # ------------------------------------------------------
    # 4) Résumé final
    # ------------------------------------------------------
    duration = (datetime.now(UTC) - start_time).total_seconds()

    logger.success(
        f"🏁 HOURLY transform finished in {duration:.2f}s — "
        f"{count_info} copied (InfoClimat), "
        f"{count_transformed} transformed (Ichtegem/Madeleine), "
        f"{count_errors} errors."
    )

    client.close()


if __name__ == "__main__":
    run_hourly_transform()
