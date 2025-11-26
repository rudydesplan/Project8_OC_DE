import argparse
import time
import os
import traceback
import threading
import boto3
from loguru import logger

# ---------------------------------------------------------------------
# IMPORT PIPELINE FUNCTIONS
# ---------------------------------------------------------------------
from config.logging_setup import setup_logging

from database.init_db import init_database
from database.create_final_unique_index import create_unique_index

from loaders.load_stations import load_all_stations
from loaders.load_metadata import load_all_metadata
from loaders.load_staging import ingest_all_staging

from quality.dq_validator import run_all_dq_tests

from transform.run_hourly_transform import run_hourly_transform

from quality.dq_consistency_test import run_dq_consistency_test
from quality.null_rates_test import run_null_rates_test
from quality.latency_test import run_latency_test
from quality.schema_validity_test import run_schema_validity_test
from quality.uniqueness_test import test_staging_uniqueness
from quality.volume_test_v2 import run_volume_test_v2


# ---------------------------------------------------------------------
# AWS CLIENTS
# ---------------------------------------------------------------------

sf = boto3.client("stepfunctions", region_name=os.getenv("AWS_REGION", "eu-north-1"))


# ---------------------------------------------------------------------
# HEARTBEAT THREAD
# ---------------------------------------------------------------------

def start_heartbeat_thread(token, interval):
    """
    Sends heartbeats while the Step Function waits.
    """
    def loop():
        while heartbeat_active["run"] and token:
            try:
                sf.send_task_heartbeat(taskToken=token)
                logger.debug("💓 Heartbeat sent")
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")
            time.sleep(interval)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t

heartbeat_active = {"run": False}


# ---------------------------------------------------------------------
# CALLBACK HELPERS
# ---------------------------------------------------------------------

def send_success(token, output):
    sf.send_task_success(
        taskToken=token,
        output=str(output)
    )

def send_failure(token, error):
    sf.send_task_failure(
        taskToken=token,
        error="PipelineError",
        cause=str(error)
    )


# ---------------------------------------------------------------------
# PIPELINE TASKS
# ---------------------------------------------------------------------

def task_pipeline_full():
    logger.info("🚀 Running FULL PIPELINE")

    init_database()
    create_unique_index()

    load_all_stations()
    load_all_metadata()
    ingest_all_staging()

    run_all_dq_tests()

    run_hourly_transform()

    run_dq_consistency_test()
    run_null_rates_test()
    run_latency_test()
    run_schema_validity_test()
    test_staging_uniqueness()
    run_volume_test_v2()

    logger.success("🌤 FULL PIPELINE SUCCESS")


def task_load_all_stations(): load_all_stations()
def task_load_all_metadata(): load_all_metadata()
def task_load_staging(): ingest_all_staging()
def task_dq_staging(): run_all_dq_tests()
def task_transform(): run_hourly_transform()

def task_final_tests():
    run_dq_consistency_test()
    run_null_rates_test()
    run_latency_test()
    run_schema_validity_test()
    test_staging_uniqueness()
    run_volume_test_v2()


TASKS = {
    "pipeline_full": task_pipeline_full,
    "load_all_stations": task_load_all_stations,
    "load_all_metadata": task_load_all_metadata,
    "ingest_all_staging": task_load_staging,
    "dq_staging": task_dq_staging,
    "transform": task_transform,
    "final_tests": task_final_tests,
}


# ---------------------------------------------------------------------
# AUTO-MODE (NO TOKEN)
# ---------------------------------------------------------------------

SFN_PIPELINE_ARN = "arn:aws:states:eu-north-1:217522443977:stateMachine:weather-machine"

def auto_mode(task_name):
    """
    ECS Service launched container (no token)
    → Start a Step Functions execution for monitoring only.
    """
    logger.info("📡 AUTO-MODE → starting monitoring execution on Step Functions…")

    try:
        execution = sf.start_execution(
            stateMachineArn=SFN_PIPELINE_ARN,
            input={"auto": True, "task": task_name}
        )

        logger.info(f"▶ Monitoring execution started: {execution['executionArn']}")

    except Exception as e:
        logger.error(f"⚠ Failed to start Step Functions monitoring execution: {e}")
        logger.warning("Running pipeline WITHOUT monitoring.")
        TASKS[task_name]()
        return

    # Run task normally (no callback in auto-mode)
    TASKS[task_name]()

    logger.success("✔ AUTO-MODE task completed.")


# ---------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------

def main():
    setup_logging()
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=str, default="pipeline_full")
    parser.add_argument("--task-token", type=str, default=None)
    parser.add_argument("--heartbeat-interval", type=int, default=20)
    args = parser.parse_args()

    task_name = args.task
    token = args.task_token

    logger.info(f"▶ Starting task: {task_name}")

    if task_name not in TASKS:
        raise ValueError(f"Unknown task: {task_name}")

    # ------------------------------------------------------
    # HYBRID BEHAVIOR STARTS HERE
    # ------------------------------------------------------

    if not token:
        # AUTO MODE (ECS startup)
        return auto_mode(task_name)

    # CALLBACK MODE (Step Functions launched this container)
    logger.info("🔗 Callback mode detected (Step Functions)")
    heartbeat_active["run"] = True
    start_heartbeat_thread(token, args.heartbeat_interval)

    try:
        TASKS[task_name]()
        send_success(token, {"task": task_name, "status": "success"})
        logger.success(f"✔ Task {task_name} completed.")

    except Exception as e:
        traceback.print_exc()
        logger.error(f"❌ Task failed: {e}")
        send_failure(token, str(e))
        raise

    finally:
        heartbeat_active["run"] = False


if __name__ == "__main__":
    main()
