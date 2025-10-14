from unittest.mock import patch

from src.worker.tasks import clear_cache


def test_clear_cache_calls_flushdb():
    with patch("src.worker.tasks.sync_redis_client.flushdb") as mock_flush:
        result = clear_cache()
        mock_flush.assert_called_once()
        assert result == "Кэш очищен"
