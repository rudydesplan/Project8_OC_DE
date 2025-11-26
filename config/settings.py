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

    if not env_path.exists():
        raise FileNotFoundError(f".env file not found at: {env_path}")

    load_dotenv(env_path)
    logger.info(f".env successfully loaded from: {env_path}")
