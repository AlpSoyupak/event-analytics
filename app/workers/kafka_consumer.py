"""Kafka consumer — ingests events published by external producers.

Events that were already persisted via the HTTP API carry `persisted=True`
and are skipped to avoid duplicate inserts.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

_engine = create_async_engine(settings.database_url, pool_size=10, max_overflow=5)
_session_factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)


async def _persist_batch(messages: list[dict]) -> None:
    external = [m for m in messages if not m.get("persisted")]
    if not external:
        return

    payload = json.dumps(external, default=str)

    async with _session_factory() as session:
        await session.execute(
            text("""
                INSERT INTO events (
                    id, tenant_id, event_type, user_id, session_id,
                    source, properties, meta, received_at, processed_at
                )
                SELECT
                    gen_random_uuid(),
                    (m->>'tenant_id')::uuid,
                    m->>'event_type',
                    m->>'user_id',
                    m->>'session_id',
                    'kafka',
                    COALESCE((m->>'properties')::jsonb, '{}'),
                    '{}',
                    COALESCE((m->>'received_at')::timestamptz, NOW()),
                    NOW()
                FROM jsonb_array_elements(:payload::jsonb) AS m
                ON CONFLICT DO NOTHING
            """),
            {"payload": payload},
        )
        await session.commit()
    logger.info(f"Persisted {len(external)} external events from Kafka")


async def consume() -> None:
    consumer = AIOKafkaConsumer(
        settings.kafka_events_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode()),
        max_poll_records=settings.kafka_batch_size,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )

    await consumer.start()
    logger.info(f"Consumer started | topic={settings.kafka_events_topic} | group={settings.kafka_consumer_group}")

    batch: list[dict] = []
    last_flush = asyncio.get_event_loop().time()

    try:
        async for msg in consumer:
            batch.append(msg.value)
            elapsed_ms = (asyncio.get_event_loop().time() - last_flush) * 1000

            if len(batch) >= settings.kafka_batch_size or elapsed_ms >= settings.kafka_batch_timeout_ms:
                await _persist_batch(batch)
                await consumer.commit()
                batch = []
                last_flush = asyncio.get_event_loop().time()
    except asyncio.CancelledError:
        if batch:
            await _persist_batch(batch)
            await consumer.commit()
    except Exception:
        logger.exception("Consumer error")
    finally:
        await consumer.stop()
        await _engine.dispose()
        logger.info("Consumer shut down")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(consume())
