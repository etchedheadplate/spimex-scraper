import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.mark.parametrize("route_name", ["ping", "get_dates", "get_dynamics", "get_results"])
@pytest.mark.parametrize("http_method", ["post", "put", "delete", "patch"])
def test_route_not_supported_methods(client, route_name, http_method):
    url = app.url_path_for(route_name)
    response = getattr(client, http_method)(url)
    assert response.status_code == 405


def test_nonexistent_route(client):
    response = client.get("/no-such-route")
    assert response.status_code == 404
