from prefect import flow, task
from loguru import logger

# -------------------------------------------------------------------
#                         DATABASE TASKS
# -------------------------------------------------------------------
from database.init_db import initialize_database
from database.stations_seed import seed_stations
from database.create_final_unique_index import create_unique_index

# -------------------------------------------------------------------
#                         LOADERS
# -------------------------------------------------------------------
from loaders.load_stations import load_stations
from loaders.load_metadata import load_metadata
from loaders.load_staging import load_staging

# -------------------------------------------------------------------
#                         QUALITY (STAGING)
# -------------------------------------------------------------------
from quality.dq_validator import run_all_dq_tests

# -------------------------------------------------------------------
#                         TRANSFORM
# -------------------------------------------------------------------
from transform.run_hourly_transform import run_hourly_transform

# -------------------------------------------------------------------
#                         FINAL DATA TESTS
# -------------------------------------------------------------------
from quality.dq_consistency_test import dq_consistency_test
from quality.null_rates_test import null_rates_test
from quality.latency_test import latency_test
from quality.schema_validity_test import schema_validity_test
from quality.uniqueness_test import uniqueness_test
from quality.volume_test_v2 import volume_test_v2


# ===================================================================
#                              TASKS
# ===================================================================

@task(log_prints=True)
def task_init_db():
    logger.info("🏗 Initializing MongoDB database…")
    initialize_database()
    seed_stations()
    create_unique_index()
    logger.success("🏗 Database initialization DONE.")


@task(log_prints=True)
def task_load_stations():
    load_stations()
    logger.success("📥 Stations loaded.")


@task(log_prints=True)
def task_load_metadata():
    load_metadata()
    logger.success("📥 Metadata loaded.")


@task(log_prints=True)
def task_load_staging():
    load_staging()
    logger.success("📥 Staging loaded.")


@task(log_prints=True)
def task_dq_staging():
    run_all_dq_tests()
    logger.success("🔍 Staging DQ passed.")


@task(log_prints=True)
def task_transform():
    run_hourly_transform()
    logger.success("⚙️ Hourly transformation completed.")


@task(log_prints=True)
def task_final_tests():
    dq_consistency_test()
    null_rates_test()
    latency_test()
    schema_validity_test()
    uniqueness_test()
    volume_test_v2()

    logger.success("🧪 All final validation tests successful.")


# ===================================================================
#                       SUB-FLOW STRUCTURE (v3 BEST PRACTICE)
# ===================================================================

@flow(name="init_and_seed", log_prints=True)
def subflow_initialize():
    """Runs initialization tasks.  
    Visible as a separate subflow run in Prefect UI."""
    task_init_db()


@flow(name="loaders_parallel", log_prints=True)
def subflow_loaders():
    """Runs all loaders concurrently."""
    f1 = task_load_stations.submit()
    f2 = task_load_metadata.submit()
    f3 = task_load_staging.submit()

    # block until staging is done
    f3.result()


@flow(name="staging_quality", log_prints=True)
def subflow_dq():
    task_dq_staging()


@flow(name="transform_flow", log_prints=True)
def subflow_transform():
    task_transform()


@flow(name="final_data_tests", log_prints=True)
def subflow_tests():
    task_final_tests()


# ===================================================================
#                           MAIN PIPELINE FLOW
# ===================================================================

@flow(
    name="weather_full_pipeline",
    log_prints=True,
    description="Full end-to-end weather ETL pipeline orchestrated with Prefect 3."
)
def weather_pipeline():
    logger.info("🚀 Starting full ETL pipeline")

    # 1. DB INIT
    subflow_initialize()

    # 2. LOADERS (parallel)
    subflow_loaders()

    # 3. STAGING DQ
    subflow_dq()

    # 4. TRANSFORMATION
    subflow_transform()

    # 5. FINAL TESTS
    subflow_tests()

    logger.success("🌤 Weather ETL pipeline completed successfully.")
    return "SUCCESS"


# ===================================================================
#                       ENTRY POINT FOR ECS CONTAINER
# ===================================================================
if __name__ == "__main__":
    weather_pipeline()
