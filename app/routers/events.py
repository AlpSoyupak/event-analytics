from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.rate_limit import require_tenant
from app.models.tenant import Tenant
from app.schemas.event import BatchIngestResponse, EventBatchCreate, EventCreate, EventIngestResponse, EventResponse, ReplayJobResponse, ReplayRequest
from app.services.cache_service import cache_service
from app.services.event_service import event_service

router = APIRouter(prefix="/events", tags=["events"])


@router.post("", response_model=EventIngestResponse, status_code=202)
async def ingest_event(
    payload: EventCreate,
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Ingest a single event. Writes to PostgreSQL and publishes to Kafka."""
    daily = await cache_service.increment_daily_counter(str(tenant.id))
    if daily > tenant.max_events_per_day:
        raise HTTPException(status_code=429, detail="Daily event quota exceeded")

    event = await event_service.ingest(tenant, payload, db)
    return EventIngestResponse(event_id=event.id, queued_at=event.received_at)


@router.post("/batch", response_model=BatchIngestResponse, status_code=202)
async def ingest_batch(
    payload: EventBatchCreate,
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Ingest up to 1 000 events in a single request."""
    daily = await cache_service.increment_daily_counter(str(tenant.id))
    if daily + len(payload.events) > tenant.max_events_per_day:
        raise HTTPException(status_code=429, detail="Batch would exceed daily quota")

    events = await event_service.ingest_batch(tenant, payload.events, db)
    return BatchIngestResponse(accepted=len(events))


@router.post("/replay", response_model=ReplayJobResponse, status_code=202)
async def start_replay(
    payload: ReplayRequest,
    tenant: Tenant = Depends(require_tenant),
):
    """Submit an async job to re-publish historical events to Kafka (max 10 000 events)."""
    from app.workers.tasks import replay_events

    job = replay_events.delay(
        str(tenant.id),
        payload.start.isoformat(),
        payload.end.isoformat(),
        payload.event_type,
    )
    return ReplayJobResponse(job_id=job.id)


@router.get("/replay/{job_id}")
async def get_replay_status(job_id: str, tenant: Tenant = Depends(require_tenant)):
    """Poll the status of a replay job."""
    from app.workers.celery_app import celery_app

    result = celery_app.AsyncResult(job_id)
    return {
        "job_id": job_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
    }


@router.get("", response_model=list[EventResponse])
async def list_events(
    event_type: str | None = None,
    user_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Query raw events for the authenticated tenant."""
    if limit > 1000:
        raise HTTPException(status_code=400, detail="limit cannot exceed 1000")
    return await event_service.list_events(tenant.id, db, event_type=event_type, user_id=user_id, limit=limit, offset=offset)
