import importlib.util
from pathlib import Path

import pytest

JOBS = Path(__file__).resolve().parents[1] / "jobs"


def load_job(name: str):
    """Import jobs/<name>.py without Spark (the jobs import pyspark only inside main())."""
    spec = importlib.util.spec_from_file_location(f"job_{name}", JOBS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def silver():
    return load_job("silver")


@pytest.fixture(scope="session")
def bronze():
    return load_job("bronze")
