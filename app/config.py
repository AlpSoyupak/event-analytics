from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@postgres:5432/event_analytics"
    sync_database_url: str = "postgresql://postgres:postgres@postgres:5432/event_analytics"

    # Redis
    redis_url: str = "redis://redis:6379/0"

    # Celery
    celery_broker_url: str = "redis://redis:6379/1"
    celery_result_backend: str = "redis://redis:6379/2"

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_events_topic: str = "events"
    kafka_consumer_group: str = "event-analytics-consumer"
    kafka_batch_size: int = 100
    kafka_batch_timeout_ms: int = 5000

    # App
    secret_key: str = "dev-secret-key"
    environment: str = "development"

    # Rate limiting (requests per minute)
    default_rate_limit: int = 100

    # Cache TTLs in seconds
    analytics_cache_ttl: int = 300
    summary_cache_ttl: int = 60

    # Data retention
    event_retention_days: int = 90


@lru_cache
def get_settings() -> Settings:
    return Settings()
