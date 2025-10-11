import pytest
from httpx import ASGITransport, AsyncClient

from main import app

# from src.api.dependencies import get_async_db


@pytest.fixture
async def ac():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_ping(ac):
    url = app.url_path_for("ping")
    response = await ac.get(url)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_get_dates(ac):
    url = app.url_path_for("get_dates")
    params = {"days": 10}
    response = await ac.get(url, params=params)
    assert response.status_code == 200
    data = response.json()
    print(data)
