import pytest
from random import randint, choice
from faker import Faker
from httpx import ASGITransport, AsyncClient
from src.database.models import SpimexTradingResults
from src.database.config import DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME
from main import app

fake = Faker()

ASYNC_DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

EXCHANGE_PRODUCT_IDS = ["EX1", "EX2"]
OIL_IDS = ["OIL1", "OIL2"]
DELIVERY_BASIS_IDS = [
    "DB1",
    "DB2",
]
DELIVERY_BASIS_NAMES = ["ABC", "DEF"]
DELIVERY_TYPE_IDS = ["A", "B"]
ROWS = 100


@pytest.fixture
async def fake_spimex_rows(async_session):
    rows = []
    for _ in range(ROWS):
        row = SpimexTradingResults(
            exchange_product_id=choice(EXCHANGE_PRODUCT_IDS),
            oil_id=choice(OIL_IDS),
            delivery_basis_id=choice(DELIVERY_BASIS_IDS),
            delivery_basis_name=choice(DELIVERY_BASIS_NAMES),
            delivery_type_id=choice(DELIVERY_TYPE_IDS),
            volume=randint(1000, 100000),
            total=randint(10000, 1000000),
            count=randint(1, 100),
            date=fake.date_between(start_date="-1w", end_date="today"),
        )
        async_session.add(row)
        rows.append(row)
    await async_session.commit()
    yield rows


@pytest.fixture()
async def ac():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


from unittest.mock import AsyncMock


@pytest.fixture
def mock_cache(mocker):
    get_cache = mocker.patch("src.api.routes.get_from_cache", new_callable=AsyncMock, return_value=None)
    set_cache = mocker.patch("src.api.routes.set_cache", new_callable=AsyncMock)
    return get_cache, set_cache


@pytest.fixture
def override_db(async_session):
    from src.api.routes import get_async_db

    app.dependency_overrides[get_async_db] = lambda: async_session
    yield
    app.dependency_overrides.clear()
