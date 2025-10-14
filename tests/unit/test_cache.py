from unittest.mock import AsyncMock, patch

import pytest

from src.cache import get_cache_key, get_from_cache, set_cache


def test_get_cache_key():
    class MockURL:
        path = "/test-path"
        query = "param=1"

    class MockRequest:
        url = MockURL()

    key = get_cache_key(MockRequest())
    assert key == "cache:/test-path?param=1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "redis_value, expected",
    [
        ('{"foo": 123}', {"foo": 123}),
        (None, None),
    ],
)
async def test_get_from_cache(redis_value, expected):
    class MockURL:
        path = "/path"
        query = ""

    class MockRequest:
        url = MockURL()

    with patch("src.cache.async_redis_client.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = redis_value
        result = await get_from_cache(MockRequest())
        assert result == expected


@pytest.mark.asyncio
async def test_set_cache_calls_redis_set():
    class MockURL:
        path = "/path"
        query = ""

    class MockRequest:
        url = MockURL()

    data = {"foo": 123}

    with patch("src.cache.async_redis_client.set", new_callable=AsyncMock) as mock_set:
        await set_cache(MockRequest(), data)
        mock_set.assert_called_once()
        called_key, called_value = mock_set.call_args[0]
        assert called_key == "cache:/path?"
        import json

        assert json.loads(called_value) == data
