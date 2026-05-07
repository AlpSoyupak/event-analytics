from celery import Celery
from celery.schedules import crontab

from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "event_analytics",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.workers.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    # Prevent tasks from being lost if the worker crashes mid-execution
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_routes={
        "app.workers.tasks.refresh_materialized_views": {"queue": "maintenance"},
        "app.workers.tasks.cleanup_old_events": {"queue": "maintenance"},
        "app.workers.tasks.precompute_tenant_analytics": {"queue": "analytics"},
        "app.workers.tasks.run_agent_evaluation": {"queue": "analytics"},
        "app.workers.tasks.detect_anomalies": {"queue": "analytics"},
        "app.workers.tasks.replay_events": {"queue": "analytics"},
    },
    beat_schedule={
        "refresh-views-every-5-minutes": {
            "task": "app.workers.tasks.refresh_materialized_views",
            "schedule": crontab(minute="*/5"),
        },
        "detect-anomalies-every-5-minutes": {
            "task": "app.workers.tasks.detect_anomalies",
            "schedule": crontab(minute="*/5"),
        },
        "cleanup-old-events-nightly": {
            "task": "app.workers.tasks.cleanup_old_events",
            "schedule": crontab(hour=2, minute=0),
        },
    },
)
