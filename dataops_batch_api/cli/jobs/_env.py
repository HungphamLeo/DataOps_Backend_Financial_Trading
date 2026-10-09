"""Load repository-level environment settings for manually launched jobs."""
import os
from pathlib import Path

from dotenv import load_dotenv


_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def load_project_env() -> None:
    load_dotenv(_PROJECT_ROOT / ".env", override=False)
    os.environ.setdefault(
        "AWS_ENDPOINT_URL",
        os.getenv("MINIO_HOST_ENDPOINT", "http://localhost:9010"),
    )
