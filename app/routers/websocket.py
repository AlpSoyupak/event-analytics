import asyncio
import json

import redis.asyncio as aioredis
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from sqlalchemy import select

from app.config import get_settings
from app.database import async_session_factory
from app.models.tenant import Tenant

settings = get_settings()
router = APIRouter(tags=["realtime"])


@router.websocket("/ws/events")
async def websocket_events(websocket: WebSocket, api_key: str = Query(...)):
    async with async_session_factory() as db:
        result = await db.execute(
            select(Tenant).where(Tenant.api_key == api_key, Tenant.is_active.is_(True))
        )
        tenant = result.scalars().first()

    if not tenant:
        await websocket.close(code=4001)
        return

    await websocket.accept()

    # Dedicated connection — pub/sub changes connection state so we can't share the main client
    redis_conn = aioredis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
    pubsub = redis_conn.pubsub()
    await pubsub.subscribe(f"ws:events:{tenant.id}")

    await websocket.send_text(
        json.dumps({"type": "connected", "tenant_id": str(tenant.id)})
    )

    async def _forward():
        async for msg in pubsub.listen():
            if msg["type"] == "message":
                await websocket.send_text(msg["data"])

    async def _wait_disconnect():
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass

    forward_task = asyncio.create_task(_forward())
    disconnect_task = asyncio.create_task(_wait_disconnect())

    try:
        await asyncio.wait(
            [forward_task, disconnect_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        for task in [forward_task, disconnect_task]:
            task.cancel()
        await pubsub.unsubscribe(f"ws:events:{tenant.id}")
        await pubsub.aclose()
        await redis_conn.aclose()
