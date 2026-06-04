import json
import time
from typing import Any

import redis.asyncio as aioredis

from app.config import get_settings

settings = get_settings()


class CacheService:
    def __init__(self) -> None:
        self._client: aioredis.Redis | None = None

    async def client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = aioredis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
        return self._client

    async def get(self, key: str) -> Any | None:
        try:
            r = await self.client()
            raw = await r.get(key)
            return json.loads(raw) if raw else None
        except Exception:
            return None

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        try:
            r = await self.client()
            await r.setex(key, ttl, json.dumps(value, default=str))
        except Exception:
            pass

    async def delete(self, key: str) -> None:
        try:
            r = await self.client()
            await r.delete(key)
        except Exception:
            pass

    async def ttl(self, key: str) -> int:
        """Return remaining TTL in seconds for *key*. -2 = not found, -1 = no expiry."""
        try:
            r = await self.client()
            return await r.ttl(key)
        except Exception:
            return -2

    async def delete_pattern(self, pattern: str) -> None:
        try:
            r = await self.client()
            keys = await r.keys(pattern)
            if keys:
                await r.delete(*keys)
        except Exception:
            pass

    async def check_rate_limit(
        self,
        tenant_id: str,
        limit: int,
        window_seconds: int = 60,
    ) -> tuple[bool, int, int]:
        """Sliding window rate limiter using a Redis sorted set.

        Returns (allowed, current_count, remaining).
        Fails open if Redis is unavailable.
        """
        try:
            r = await self.client()
            now = time.time()
            window_start = now - window_seconds
            key = f"rl:{tenant_id}"

            async with r.pipeline(transaction=True) as pipe:
                pipe.zremrangebyscore(key, 0, window_start)
                pipe.zadd(key, {str(now): now})
                pipe.zcard(key)
                pipe.expire(key, window_seconds + 1)
                results = await pipe.execute()

            current = results[2]
            remaining = max(0, limit - current)
            return current <= limit, current, remaining
        except Exception:
            return True, 0, limit  # fail open

    async def increment_daily_counter(self, tenant_id: str) -> int:
        """Track daily event volume for quota enforcement."""
        from datetime import date

        try:
            r = await self.client()
            key = f"quota:{tenant_id}:{date.today().isoformat()}"
            count = await r.incr(key)
            await r.expire(key, 172_800)  # 2 days
            return count
        except Exception:
            return 0

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None


cache_service = CacheService()
