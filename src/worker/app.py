from celery import Celery
from celery.schedules import crontab

celery_app = Celery(
    "spimex_worker",
    broker="redis://127.0.0.1:6379/0",
    backend="redis://127.0.0.1:6379/0",
    broker_connection_retry_on_startup=True,
)

celery_app.conf.update(  # type: ignore[reportUnknownMemberType]
    timezone="Europe/Moscow",
    enable_utc=True,
)

celery_app.conf.beat_schedule = {  # type: ignore[reportUnknownMemberType]
    "clear-cache-every-day-1411": {
        "task": "src.worker.tasks.clear_cache",
        "schedule": crontab(hour=23, minute=55),
    },
}

import src.worker.tasks as _  # noqa: F401, E402
