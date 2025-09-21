import os
import pytest


@pytest.fixture(scope="session", autouse=True)
def _envs():
    os.environ.setdefault("ENV", "test")
    yield

