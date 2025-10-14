from random import choice, randint
from unittest.mock import AsyncMock

import pytest
from faker import Faker
from httpx import ASGITransport, AsyncClient

from main import app
from src.database.models import SpimexTradingResults

fake = Faker()
ROWS_COUNT = 100
EXCHANGE_PRODUCT_IDS = ["EX1", "EX2"]
OIL_IDS = ["OIL1", "OIL2"]
DELIVERY_BASIS_IDS = ["DB1", "DB2"]
DELIVERY_BASIS_NAMES = ["ABC", "DEF"]
DELIVERY_TYPE_IDS = ["A", "B"]


@pytest.fixture
async def fake_spimex_rows(async_session):
    rows = []
    for _ in range(ROWS_COUNT):
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


@pytest.fixture
async def ac():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


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


@pytest.mark.asyncio
async def test_ping(ac):
    url = app.url_path_for("ping")
    response = await ac.get(url)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


VALID_DAYS = [1, 2, 3, 101]
INVALID_DAYS = [-5, 0, 2.5, "not_a_num"]


@pytest.mark.asyncio
@pytest.mark.parametrize("days", VALID_DAYS)
async def test_get_dates_valid(ac, fake_spimex_rows, mock_cache, override_db, days):
    url = app.url_path_for("get_dates")
    response = await ac.get(url, params={"days": days})
    assert response.status_code == 200
    data = response.json()
    assert "dates" in data
    if days != 101:
        assert len(data["dates"]) == days


@pytest.mark.asyncio
@pytest.mark.parametrize("days", INVALID_DAYS)
async def test_get_dates_invalid(ac, fake_spimex_rows, days):
    url = app.url_path_for("get_dates")
    params = {"days": days}
    if days in (-5, 0):
        from pydantic_core import ValidationError

        with pytest.raises(ValidationError):
            await ac.get(url, params=params)
        return
    response = await ac.get(url, params=params)
    if days in (2.5, "not_a_num"):
        assert response.status_code == 422


VALID_DYNAMICS_PARAMS = [
    {"start_date": "2025-01-01", "end_date": "2025-10-10", "oil_id": "OIL1"},
    {"start_date": "1970-01-01", "end_date": "2069-12-31", "oil_id": "OIL1"},
]
INVALID_DYNAMICS_PARAMS = [
    {"start_date": "2023-13-32", "end_date": "2023-10-10"},
    {"start_date": "10-10-2023", "end_date": "2023-10-10"},
    {"start_date": "2023-10-10", "end_date": "not_a_date"},
    {"start_date": "not_a_date", "end_date": "also_bad"},
]


@pytest.mark.asyncio
@pytest.mark.parametrize("params", VALID_DYNAMICS_PARAMS)
async def test_get_dynamics_valid(ac, fake_spimex_rows, mock_cache, override_db, params):
    url = app.url_path_for("get_dynamics")
    response = await ac.get(url, params=params)
    assert response.status_code == 200
    data = response.json()
    assert "delivery_type_id" in data[0]
    assert "OIL1" in data[0].values()
    assert "OIL2" not in data[0].values()


@pytest.mark.asyncio
@pytest.mark.parametrize("params", INVALID_DYNAMICS_PARAMS)
async def test_get_dynamics_invalid(ac, fake_spimex_rows, params):
    url = app.url_path_for("get_dynamics")
    response = await ac.get(url, params=params)
    assert response.status_code == 422


VALID_RESULTS_PARAMS = [{"oil_id": "OIL2"}, {"delivery_type_id": "B"}, {"delivery_basis_id": "DB2"}, {}]
INVALID_RESULTS_PARAMS = [{"oil_id": "not_an_id"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("params", VALID_RESULTS_PARAMS)
async def test_get_results_valid(ac, fake_spimex_rows, mock_cache, override_db, params):
    url = app.url_path_for("get_results")
    response = await ac.get(url, params=params)
    assert response.status_code == 200
    data = response.json()
    assert len(data[0].keys()) == 9
    if "OIL2" in params.values():
        assert "OIL2" in data[0].values()
    if "B" in params.values():
        assert "A" not in data[0].values()
    if not params:
        assert len(data[0].values()) > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("params", INVALID_RESULTS_PARAMS)
async def test_get_results_invalid(ac, fake_spimex_rows, params):
    url = app.url_path_for("get_results")
    response = await ac.get(url, params=params)
    assert response.status_code == 422
