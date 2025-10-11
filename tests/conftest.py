import pytest

from src.database.connection import sync_engine
from src.database.models import BaseModel


@pytest.fixture(scope="session", autouse=True)
def setup_db():
    print("Before DB drop")
    BaseModel.metadata.drop_all(sync_engine)
    print("Before DB creation")
    BaseModel.metadata.create_all(sync_engine)
    yield
    print("Before DB drop")


#    BaseModel.metadata.drop_all(sync_engine)
