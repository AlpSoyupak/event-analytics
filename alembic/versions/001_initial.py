"""Initial schema with tenants, events, indexes, and materialized views

Revision ID: 001
Revises:
Create Date: 2026-04-25 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("api_key", sa.String(64), nullable=False, unique=True),
        sa.Column("plan", sa.String(50), server_default="free", nullable=False),
        sa.Column("rate_limit_per_minute", sa.Integer, server_default="100", nullable=False),
        sa.Column("max_events_per_day", sa.BigInteger, server_default="10000", nullable=False),
        sa.Column("is_active", sa.Boolean, server_default="true", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_tenants_api_key", "tenants", ["api_key"])

    op.create_table(
        "events",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("user_id", sa.String(255), nullable=True),
        sa.Column("session_id", sa.String(255), nullable=True),
        sa.Column("source", sa.String(100), server_default="api", nullable=False),
        sa.Column("properties", JSONB, server_default="{}", nullable=False),
        sa.Column("meta", JSONB, server_default="{}", nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Composite B-tree indexes for the most common query patterns
    op.create_index("ix_events_tenant_time", "events", ["tenant_id", "received_at"])
    op.create_index("ix_events_tenant_type", "events", ["tenant_id", "event_type"])
    op.create_index("ix_events_tenant_user", "events", ["tenant_id", "user_id"])

    # BRIN index: minimal overhead, ideal for append-only time-ordered data at scale
    op.execute("CREATE INDEX ix_events_received_brin ON events USING BRIN (received_at)")

    # GIN index for arbitrary JSONB property queries
    op.execute("CREATE INDEX ix_events_properties_gin ON events USING GIN (properties)")

    # Hourly materialized view — drives real-time dashboards
    op.execute("""
        CREATE MATERIALIZED VIEW mv_event_counts_hourly AS
        SELECT
            tenant_id,
            event_type,
            date_trunc('hour', received_at) AS hour,
            COUNT(*)                         AS event_count,
            COUNT(DISTINCT user_id)          AS unique_users,
            COUNT(DISTINCT session_id)       AS unique_sessions
        FROM events
        GROUP BY tenant_id, event_type, date_trunc('hour', received_at)
        WITH DATA
    """)
    op.execute(
        "CREATE UNIQUE INDEX uix_mv_hourly ON mv_event_counts_hourly (tenant_id, event_type, hour)"
    )

    # Daily materialized view — drives historical trend queries
    op.execute("""
        CREATE MATERIALIZED VIEW mv_event_counts_daily AS
        SELECT
            tenant_id,
            event_type,
            date_trunc('day', received_at) AS day,
            COUNT(*)                        AS event_count,
            COUNT(DISTINCT user_id)         AS unique_users,
            COUNT(DISTINCT session_id)      AS unique_sessions
        FROM events
        GROUP BY tenant_id, event_type, date_trunc('day', received_at)
        WITH DATA
    """)
    op.execute(
        "CREATE UNIQUE INDEX uix_mv_daily ON mv_event_counts_daily (tenant_id, event_type, day)"
    )


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_event_counts_daily")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_event_counts_hourly")
    op.drop_table("events")
    op.drop_table("tenants")
