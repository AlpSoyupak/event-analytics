import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.rate_limit import require_tenant
from app.models.tenant import Tenant
from app.schemas.tenant import TenantCreate, TenantResponse, TenantUpdate
from app.services.tenant_service import tenant_service

router = APIRouter(prefix="/tenants", tags=["tenants"])


@router.post("", response_model=TenantResponse, status_code=201)
async def create_tenant(
    payload: TenantCreate,
    db: AsyncSession = Depends(get_db),
):
    """Provision a new tenant. Returns the API key — store it securely, it is shown only once."""
    tenant = await tenant_service.create(payload, db)
    return tenant


@router.get("/me", response_model=TenantResponse)
async def get_current_tenant(tenant: Tenant = Depends(require_tenant)):
    """Return the tenant associated with the provided API key."""
    return tenant


@router.get("/{tenant_id}", response_model=TenantResponse)
async def get_tenant(
    tenant_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    tenant = await tenant_service.get(tenant_id, db)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return tenant


@router.patch("/{tenant_id}", response_model=TenantResponse)
async def update_tenant(
    tenant_id: uuid.UUID,
    payload: TenantUpdate,
    db: AsyncSession = Depends(get_db),
):
    tenant = await tenant_service.get(tenant_id, db)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return await tenant_service.update(tenant, payload, db)


@router.post("/{tenant_id}/rotate-key", response_model=TenantResponse)
async def rotate_api_key(
    tenant_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """Issue a new API key and invalidate the existing one."""
    tenant = await tenant_service.get(tenant_id, db)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return await tenant_service.rotate_api_key(tenant, db)
