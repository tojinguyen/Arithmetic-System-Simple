import pytest
from fastapi.testclient import TestClient
from app.main import app as fastapi_app
from app.celery import app as celery_app

@pytest.fixture(scope="session", autouse=True)
def setup_celery_for_testing():
    celery_app.conf.update(task_always_eager=True)

@pytest.fixture(scope="module")
def client():
    with TestClient(fastapi_app) as c:
        yield c