"""Keep the hot outbox lookup index limited to unpublished work."""
from alembic import op
import sqlalchemy as sa

revision = "0003"
down_revision = "0002"
branch_labels = depends_on = None


def upgrade():
    op.create_index(
        "ix_outbox_pending_route_time", "outbox",
        ["routing_key", "available_at"],
        postgresql_where=sa.text("published_at IS NULL"),
        sqlite_where=sa.text("published_at IS NULL"),
    )


def downgrade():
    op.drop_index("ix_outbox_pending_route_time", table_name="outbox")
