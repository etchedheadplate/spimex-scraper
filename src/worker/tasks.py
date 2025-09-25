import redis

from src.worker.app import celery_app

r = redis.Redis(host="127.0.0.1", port=6379, db=0)


@celery_app.task
def clear_cache():
    r.flushdb()
    return "Cache cleared"
