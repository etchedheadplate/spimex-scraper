import pytest
from pydantic_core import ValidationError

from main import app


class TestEndpoints:
    @pytest.mark.asyncio
    async def test_ping(self, ac):
        url = app.url_path_for("ping")
        response = await ac.get(url)
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("valid_days", [1, 2, 3, 101])
    async def test_get_dates_valid_query(self, ac, fake_spimex_rows, mock_cache, override_db, valid_days):
        url = app.url_path_for("get_dates")
        params = {"days": valid_days}
        response = await ac.get(url, params=params)
        assert response.status_code == 200
        data = response.json()
        assert "dates" in data
        if valid_days != 101:
            assert len(data["dates"]) == valid_days

    @pytest.mark.asyncio
    @pytest.mark.parametrize("invalid_days", [-5, 0, 2.5, "not_a_num"])
    async def test_get_dates_invalid_query(self, ac, fake_spimex_rows, invalid_days):
        url = app.url_path_for("get_dates")
        params = {"days": invalid_days}
        if invalid_days in (-5, 0):
            with pytest.raises(ValidationError):
                await ac.get(url, params=params)
            return

        response = await ac.get(url, params=params)

        if invalid_days in (2.5, "not_a_num"):
            assert response.status_code == 422

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "valid_params",
        [
            {"start_date": "2025-01-01", "end_date": "2025-10-10", "oil_id": "OIL1"},
            {"start_date": "1970-01-01", "end_date": "2069-12-31", "oil_id": "OIL1"},
        ],
    )
    async def test_get_dynamics_valid_query(self, ac, fake_spimex_rows, mock_cache, override_db, valid_params):
        url = app.url_path_for("get_dynamics")
        response = await ac.get(url, params=valid_params)
        assert response.status_code == 200
        data = response.json()
        assert "delivery_type_id" in data[0].keys()
        assert "OIL1" in data[0].values()
        assert "OIL2" not in data[0].values()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "invalid_params",
        [
            {"start_date": "2023-13-32", "end_date": "2023-10-10"},
            {"start_date": "10-10-2023", "end_date": "2023-10-10"},
            {"start_date": "2023-10-10", "end_date": "not_a_date"},
            {"start_date": "not_a_date", "end_date": "also_bad"},
        ],
    )
    async def test_get_dynamics_invalid_query(self, ac, fake_spimex_rows, invalid_params):
        url = app.url_path_for("get_dynamics")
        response = await ac.get(url, params=invalid_params)
        assert response.status_code == 422

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "valid_params", [{"oil_id": "OIL2"}, {"delivery_type_id": "B"}, {"delivery_basis_id": "DB2"}, {}]
    )
    async def test_get_trading_results_valid_query(self, ac, fake_spimex_rows, mock_cache, override_db, valid_params):
        url = app.url_path_for("get_results")
        response = await ac.get(url, params=valid_params)
        assert response.status_code == 200
        data = response.json()
        assert len(data[0].keys()) == 9
        if "OIL2" in valid_params.values():
            assert "OIL2" in data[0].values()
        if "B" in valid_params.values():
            assert "A" not in data[0].values()
        if not valid_params:
            assert len(data[0].values()) > 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "invalid_params",
        [
            {"oil_id": "not_an_id"},
        ],
    )
    async def test_get_trading_results_invalid_query(self, ac, fake_spimex_rows, invalid_params):
        url = app.url_path_for("get_results")
        response = await ac.get(url, params=invalid_params)
        assert response.status_code == 422
