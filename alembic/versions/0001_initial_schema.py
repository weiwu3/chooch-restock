"""Initial schema: observations, inventory_state, replenishment_requests, outbound_attempts.

Revision ID: 0001
Revises:
Create Date: 2026-10-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

BIG_ID = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")
EVENT_ID = sa.String(128).with_variant(postgresql.VARCHAR(128, collation="C"), "postgresql")
TS = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "observations",
        sa.Column("id", BIG_ID, primary_key=True),
        sa.Column("event_id", EVENT_ID, nullable=False),
        sa.Column("facility_id", sa.String(64), nullable=False),
        sa.Column("room_id", sa.String(64), nullable=False),
        sa.Column("sku", sa.String(64), nullable=False),
        sa.Column("observed_at", TS, nullable=False),
        sa.Column("observed_quantity", sa.Integer(), nullable=False),
        sa.Column("par_level", sa.Integer(), nullable=False),
        sa.Column("reorder_point", sa.Integer(), nullable=False),
        sa.Column("confidence", sa.Double(), nullable=False),
        sa.Column("source", sa.String(128)),
        sa.Column("camera_id", sa.String(128)),
        sa.Column("metadata", JSON),
        sa.Column("received_at", TS, nullable=False),
        sa.Column("correlation_id", sa.String(128), nullable=False),
        sa.Column("applied", sa.Boolean(), nullable=False),
        sa.Column("decision", sa.String(32), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_observations"),
        sa.UniqueConstraint("event_id", name="uq_observations_event_id"),
        sa.CheckConstraint(
            "observed_quantity >= 0", name=op.f("ck_observations_quantity_non_negative")
        ),
        sa.CheckConstraint(
            "par_level >= reorder_point AND reorder_point >= 0",
            name=op.f("ck_observations_thresholds"),
        ),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 1", name=op.f("ck_observations_confidence_range")
        ),
        sa.CheckConstraint(
            "decision IN ('none', 'recovered', 'request_created', 'blocked_open_request', "
            "'blocked_not_armed', 'stale')",
            name=op.f("ck_observations_decision_valid"),
        ),
    )
    op.create_index(
        "ix_observations_item_order",
        "observations",
        ["facility_id", "room_id", "sku", "observed_at", "event_id"],
    )
    # History by room and SKU without facility_id (facility_id is optional on that endpoint).
    # Both indexes are read backwards for the newest-first order.
    op.create_index(
        "ix_observations_room_sku_order",
        "observations",
        ["room_id", "sku", "observed_at", "event_id"],
    )

    op.create_table(
        "inventory_state",
        sa.Column("facility_id", sa.String(64), nullable=False),
        sa.Column("room_id", sa.String(64), nullable=False),
        sa.Column("sku", sa.String(64), nullable=False),
        sa.Column("current_quantity", sa.Integer(), nullable=False),
        sa.Column("par_level", sa.Integer(), nullable=False),
        sa.Column("reorder_point", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(8), nullable=False),
        sa.Column("last_event_id", EVENT_ID, nullable=False),
        sa.Column("last_observed_at", TS, nullable=False),
        sa.Column("armed", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("updated_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("facility_id", "room_id", "sku", name="pk_inventory_state"),
        sa.ForeignKeyConstraint(
            ["last_event_id"],
            ["observations.event_id"],
            name="fk_inventory_state_last_event_id_observations",
        ),
        sa.CheckConstraint("status IN ('ok', 'low')", name=op.f("ck_inventory_state_status_valid")),
        sa.CheckConstraint(
            "current_quantity >= 0", name=op.f("ck_inventory_state_quantity_non_negative")
        ),
        sa.CheckConstraint(
            "par_level >= reorder_point AND reorder_point >= 0",
            name=op.f("ck_inventory_state_thresholds"),
        ),
    )

    op.create_table(
        "replenishment_requests",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("facility_id", sa.String(64), nullable=False),
        sa.Column("room_id", sa.String(64), nullable=False),
        sa.Column("sku", sa.String(64), nullable=False),
        sa.Column("triggering_event_id", EVENT_ID, nullable=False),
        sa.Column("requested_quantity", sa.Integer(), nullable=False),
        sa.Column("observed_quantity", sa.Integer(), nullable=False),
        sa.Column("par_level", sa.Integer(), nullable=False),
        sa.Column("reorder_point", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("idempotency_key", sa.String(64), nullable=False),
        sa.Column("erp_order_id", sa.String(128)),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("next_attempt_at", TS),
        sa.Column("last_error", sa.Text()),
        sa.Column("manual_retry_at", TS),
        sa.Column("manual_retry_correlation_id", sa.String(128)),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_replenishment_requests"),
        sa.UniqueConstraint(
            "triggering_event_id", name="uq_replenishment_requests_triggering_event_id"
        ),
        sa.UniqueConstraint("idempotency_key", name="uq_replenishment_requests_idempotency_key"),
        sa.ForeignKeyConstraint(
            ["triggering_event_id"],
            ["observations.event_id"],
            name="fk_replenishment_requests_triggering_event_id_observations",
        ),
        sa.CheckConstraint(
            "requested_quantity > 0", name=op.f("ck_replenishment_requests_quantity_positive")
        ),
        sa.CheckConstraint(
            "observed_quantity >= 0",
            name=op.f("ck_replenishment_requests_observed_quantity_non_negative"),
        ),
        sa.CheckConstraint(
            "par_level >= reorder_point AND reorder_point >= 0",
            name=op.f("ck_replenishment_requests_thresholds"),
        ),
        sa.CheckConstraint(
            "attempt_count >= 0", name=op.f("ck_replenishment_requests_attempt_count_non_negative")
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'in_flight', 'delivered', 'failed')",
            name=op.f("ck_replenishment_requests_status_valid"),
        ),
    )
    # At most one open request (pending or in flight) per facility + room + SKU; a delivered or
    # failed one does not count. Partial unique indexes exist in both PostgreSQL and SQLite
    # (>= 3.8), so the invariant is the database's.
    op.create_index(
        "uq_replenishment_requests_open_item",
        "replenishment_requests",
        ["facility_id", "room_id", "sku"],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'in_flight')"),
        sqlite_where=sa.text("status IN ('pending', 'in_flight')"),
    )
    op.create_index(
        "ix_replenishment_requests_status_next_attempt_at",
        "replenishment_requests",
        ["status", "next_attempt_at"],
    )
    # An item's requests in creation order: the search for the request that supersedes a
    # failed one (restock/api.py, newer_request_id).
    op.create_index(
        "ix_replenishment_requests_facility_id_room_id_sku_created_at",
        "replenishment_requests",
        ["facility_id", "room_id", "sku", "created_at"],
    )

    op.create_table(
        "outbound_attempts",
        sa.Column("id", BIG_ID, primary_key=True),
        sa.Column("request_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(64), nullable=False),
        sa.Column("request_body", JSON, nullable=False),
        sa.Column("http_status", sa.Integer()),
        sa.Column("response_body", JSON),
        sa.Column("classification", sa.String(16), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("triggered_by", sa.String(8), nullable=False),
        sa.Column("correlation_id", sa.String(128)),
        sa.Column("manual_retry_correlation_id", sa.String(128)),
        sa.Column("started_at", TS, nullable=False),
        sa.Column("finished_at", TS, nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_outbound_attempts"),
        sa.ForeignKeyConstraint(
            ["request_id"],
            ["replenishment_requests.id"],
            name="fk_outbound_attempts_request_id_replenishment_requests",
        ),
        sa.CheckConstraint(
            "classification IN ('success', 'retryable', 'non_retryable', 'ambiguous', "
            "'in_progress', 'lost')",
            name=op.f("ck_outbound_attempts_classification_valid"),
        ),
        sa.CheckConstraint(
            "triggered_by IN ('worker', 'manual')",
            name=op.f("ck_outbound_attempts_triggered_by_valid"),
        ),
    )
    op.create_index(
        "ix_outbound_attempts_request_id_attempt_no",
        "outbound_attempts",
        ["request_id", "attempt_no"],
    )


def downgrade() -> None:
    op.drop_table("outbound_attempts")
    op.drop_table("replenishment_requests")
    op.drop_table("inventory_state")
    op.drop_table("observations")
