from fastapi import Depends, HTTPException, Response

from app.dependencies.auth import get_current_tenant
from app.models.tenant import Tenant
from app.services.cache_service import cache_service


async def require_tenant(
    response: Response,
    tenant: Tenant = Depends(get_current_tenant),
) -> Tenant:
    """Auth + sliding-window rate limit. Attaches RateLimit headers to every response."""
    allowed, current, remaining = await cache_service.check_rate_limit(
        tenant_id=str(tenant.id),
        limit=tenant.rate_limit_per_minute,
        window_seconds=60,
    )
    response.headers["X-RateLimit-Limit"] = str(tenant.rate_limit_per_minute)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Reset"] = "60"

    if not allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Try again in 60 seconds.",
            headers={
                "Retry-After": "60",
                "X-RateLimit-Limit": str(tenant.rate_limit_per_minute),
                "X-RateLimit-Remaining": "0",
            },
        )
    return tenant
