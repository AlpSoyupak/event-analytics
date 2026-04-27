from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.tenant import Tenant

from fastapi import Depends

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_current_tenant(
    api_key: str | None = Security(api_key_header),
    db: AsyncSession = Depends(get_db),
) -> Tenant:
    if not api_key:
        raise HTTPException(status_code=401, detail="X-API-Key header required")

    result = await db.execute(
        select(Tenant).where(
            Tenant.api_key == api_key,
            Tenant.is_active.is_(True),
        )
    )
    tenant = result.scalar_one_or_none()
    if not tenant:
        raise HTTPException(status_code=401, detail="Invalid or inactive API key")

    return tenant
