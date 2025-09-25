import redis

from src.worker.app import celery_app

sync_redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0)


@celery_app.task  # type: ignore[reportUnknownMemberType]
def clear_cache():
    sync_redis_client.flushdb()  # type: ignore[reportUnknownMemberType]
    return "Кэш очищен"
