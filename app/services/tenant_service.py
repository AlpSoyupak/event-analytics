import secrets
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tenant import Tenant
from app.schemas.tenant import PLAN_LIMITS, TenantCreate, TenantUpdate


class TenantService:
    async def create(self, data: TenantCreate, db: AsyncSession) -> Tenant:
        limits = PLAN_LIMITS[data.plan]
        tenant = Tenant(
            name=data.name,
            api_key=secrets.token_urlsafe(32),
            plan=data.plan,
            rate_limit_per_minute=limits["rate_limit_per_minute"],
            max_events_per_day=limits["max_events_per_day"],
        )
        db.add(tenant)
        await db.flush()
        return tenant

    async def get(self, tenant_id: uuid.UUID, db: AsyncSession) -> Tenant | None:
        result = await db.execute(select(Tenant).where(Tenant.id == tenant_id))
        return result.scalar_one_or_none()

    async def update(self, tenant: Tenant, data: TenantUpdate, db: AsyncSession) -> Tenant:
        if data.name is not None:
            tenant.name = data.name
        if data.is_active is not None:
            tenant.is_active = data.is_active
        if data.plan is not None:
            limits = PLAN_LIMITS[data.plan]
            tenant.plan = data.plan
            tenant.rate_limit_per_minute = limits["rate_limit_per_minute"]
            tenant.max_events_per_day = limits["max_events_per_day"]
        await db.flush()
        return tenant

    async def rotate_api_key(self, tenant: Tenant, db: AsyncSession) -> Tenant:
        tenant.api_key = secrets.token_urlsafe(32)
        await db.flush()
        return tenant


tenant_service = TenantService()
