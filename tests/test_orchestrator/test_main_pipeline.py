import pytest
from unittest.mock import patch, MagicMock
from moto import mock_aws
import boto3
from main import main, TASKS, start_heartbeat_thread
import main as orchestrator


# --------------------------------------------------------------------
# Helper: simulate command-line execution
# --------------------------------------------------------------------

def run_main(args):
    with patch("sys.argv", ["main.py"] + args):
        return main()


# --------------------------------------------------------------------
# Fixture: Mock all pipeline task functions
# --------------------------------------------------------------------

@pytest.fixture
def mock_tasks():
    """
    Replace every pipeline task (loaders, DQ, transform...) with MagicMock.
    """
    with patch.dict(TASKS, {
        "pipeline_full": MagicMock(),
        "load_stations": MagicMock(),
        "load_metadata": MagicMock(),
        "load_staging": MagicMock(),
        "dq_staging": MagicMock(),
        "transform": MagicMock(),
        "final_tests": MagicMock(),
    }) as mocks:
        yield mocks


# --------------------------------------------------------------------
# 1) AUTO-MODE TESTS (NO TOKEN)
# --------------------------------------------------------------------

@mock_aws
def test_auto_mode_starts_stepfunctions_execution(mock_tasks):
    """
    When no token is provided, auto-mode must:
    - call sf.start_execution(...)
    - then call TASKS[task_name]()
    """
    sf = boto3.client("stepfunctions", region_name="eu-north-1")

    # Create fake state machine (Moto requirement)
    sf.create_state_machine(
        name="weather-machine",
        definition="{}",
        roleArn="arn:aws:iam::123456789012:role/Dummy"
    )

    run_main(["--task", "pipeline_full"])

    # Task has been called
    assert TASKS["pipeline_full"].called


@mock_aws
def test_auto_mode_fallback_if_sf_fails(mock_tasks):
    """
    If Step Functions cannot start an execution,
    auto-mode should still run the task locally.
    """
    # Patch start_execution to force failure
    with patch.object(orchestrator.sf, "start_execution", side_effect=Exception("SF down")):
        run_main(["--task", "pipeline_full"])

    assert TASKS["pipeline_full"].called


# --------------------------------------------------------------------
# 2) CALLBACK MODE — success flow
# --------------------------------------------------------------------

@mock_aws
def test_callback_mode_success(mock_tasks):
    """
    In callback mode, main():
        - starts heartbeat
        - calls task
        - sends task success
    """

    sf = boto3.client("stepfunctions", region_name="eu-north-1")

    # Required by StepFunctions mocking
    sf.create_state_machine(
        name="weather-machine",
        definition="{}",
        roleArn="arn:aws:iam::123456789012:role/Dummy"
    )

    with (
        patch.object(orchestrator.sf, "send_task_success") as mock_success,
        patch.object(orchestrator.sf, "send_task_heartbeat") as mock_hb,
        patch("main.start_heartbeat_thread", return_value=None),
    ):

        run_main(["--task", "pipeline_full", "--task-token", "TOKEN123"])

        assert TASKS["pipeline_full"].called
        mock_success.assert_called_once()


# --------------------------------------------------------------------
# 3) CALLBACK MODE — failure propagation
# --------------------------------------------------------------------

@mock_aws
def test_callback_mode_failure_is_reported(mock_tasks):
    """
    If TASKS[task] raises, we must call send_task_failure(token).
    """

    sf = boto3.client("stepfunctions", region_name="eu-north-1")
    sf.create_state_machine(
        name="weather-machine",
        definition="{}",
        roleArn="arn:aws:iam::123456789012:role/Dummy"
    )

    # Force pipeline failure
    TASKS["pipeline_full"].side_effect = Exception("BOOM")

    with (
        patch.object(orchestrator.sf, "send_task_failure") as mock_fail,
        patch("main.start_heartbeat_thread", return_value=None),
    ):
        with pytest.raises(Exception):
            run_main(["--task", "pipeline_full", "--task-token", "AAA"])

        mock_fail.assert_called_once()


# --------------------------------------------------------------------
# 4) HEARTBEAT THREAD TEST (unit)
# --------------------------------------------------------------------

@mock_aws
def test_heartbeat_thread_sends_heartbeats():
    """
    start_heartbeat_thread() must call sf.send_task_heartbeat repeatedly.
    """
    sf = boto3.client("stepfunctions", region_name="eu-north-1")

    with (
        patch.object(orchestrator.sf, "send_task_heartbeat") as mock_hb,
    ):
        # Enable heartbeat
        orchestrator.heartbeat_active["run"] = True

        t = start_heartbeat_thread("TOKEN123", 0.01)

        # Let a few iterations run
        import time
        time.sleep(0.05)

        orchestrator.heartbeat_active["run"] = False

        t.join(timeout=0.1)

        assert mock_hb.called


# --------------------------------------------------------------------
# 5) Specific pipelines
# --------------------------------------------------------------------

def test_pipeline_load_only(mock_tasks):
    run_main(["--task", "load_stations"])
    assert TASKS["load_stations"].called


def test_pipeline_transform_only(mock_tasks):
    run_main(["--task", "transform"])
    assert TASKS["transform"].called


def test_pipeline_load_and_metadata(mock_tasks):
    run_main(["--task", "load_metadata"])
    assert TASKS["load_metadata"].called
