import json
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models.event import Event
from app.models.tenant import Tenant
from app.schemas.event import EventCreate
from app.services.cache_service import cache_service

settings = get_settings()

_producer: AIOKafkaProducer | None = None


async def get_kafka_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
            compression_type="gzip",
            acks="all",
            enable_idempotence=True,
            request_timeout_ms=10_000,
        )
        await _producer.start()
    return _producer


async def close_kafka_producer() -> None:
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None


def _build_event(tenant: Tenant, data: EventCreate, now: datetime) -> Event:
    return Event(
        id=uuid.uuid4(),
        tenant_id=tenant.id,
        event_type=data.event_type,
        user_id=data.user_id,
        session_id=data.session_id,
        source=data.source,
        properties=data.properties,
        meta=data.meta,
        received_at=data.received_at or now,
    )


async def _publish_to_websocket(event: Event) -> None:
    try:
        r = await cache_service.client()
        payload = json.dumps(
            {
                "event_id": str(event.id),
                "event_type": event.event_type,
                "user_id": event.user_id,
                "session_id": event.session_id,
                "properties": event.properties,
                "received_at": event.received_at.isoformat(),
                "source": event.source,
            },
            default=str,
        )
        await r.publish(f"ws:events:{event.tenant_id}", payload)
    except Exception:
        pass


async def _publish_to_kafka(event: Event) -> None:
    try:
        producer = await get_kafka_producer()
        await producer.send(
            settings.kafka_events_topic,
            value={
                "event_id": str(event.id),
                "tenant_id": str(event.tenant_id),
                "event_type": event.event_type,
                "user_id": event.user_id,
                "session_id": event.session_id,
                "properties": event.properties,
                "received_at": event.received_at.isoformat(),
                "persisted": True,  # signals consumer to skip re-insert
            },
            key=str(event.tenant_id).encode(),
        )
    except Exception:
        pass  # Kafka is best-effort; DB is the source of truth


class EventService:
    async def ingest(
        self,
        tenant: Tenant,
        data: EventCreate,
        db: AsyncSession,
    ) -> Event:
        now = datetime.now(timezone.utc)
        event = _build_event(tenant, data, now)
        db.add(event)
        await db.flush()
        await _publish_to_kafka(event)
        await _publish_to_websocket(event)
        return event

    async def ingest_batch(
        self,
        tenant: Tenant,
        items: list[EventCreate],
        db: AsyncSession,
    ) -> list[Event]:
        now = datetime.now(timezone.utc)
        events = [_build_event(tenant, item, now) for item in items]
        db.add_all(events)
        await db.flush()
        for event in events:
            await _publish_to_websocket(event)
        return events

    async def list_events(
        self,
        tenant_id: uuid.UUID,
        db: AsyncSession,
        *,
        event_type: str | None = None,
        user_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Event]:
        q = select(Event).where(Event.tenant_id == tenant_id)
        if event_type:
            q = q.where(Event.event_type == event_type)
        if user_id:
            q = q.where(Event.user_id == user_id)
        q = q.order_by(Event.received_at.desc()).limit(limit).offset(offset)
        result = await db.execute(q)
        return list(result.scalars().all())


event_service = EventService()
