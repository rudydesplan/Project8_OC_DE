# config/settings.py

from dotenv import load_dotenv
from pathlib import Path
from loguru import logger


def load_env():
    """
    Loads the .env file located at the project root directory.
    Safe to call multiple times (load_dotenv is idempotent).
    """
    env_path = Path(__file__).resolve().parents[1] / ".env"

    if env_path.exists():
        load_dotenv(env_path)
    else:
        logger.info(f"No .env file found at {env_path}, using environment variables from ECS.")
