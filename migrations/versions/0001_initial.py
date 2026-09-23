from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = depends_on = None


def state_columns():
    return [
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error_code", sa.String(80), nullable=True),
    ]


def upgrade():
    op.create_table("templates",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("template_id", sa.String(100), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("subject", sa.Text(), nullable=False),
        sa.Column("html_body", sa.Text(), nullable=False),
        sa.Column("text_body", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("template_id", "version"))
    op.create_table("requests",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("service_id", sa.String(50), nullable=False),
        sa.Column("endpoint_kind", sa.String(20), nullable=False),
        sa.Column("external_id", sa.Uuid(), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=True),
        sa.Column("template_version_id", sa.Uuid(), sa.ForeignKey("templates.id"), nullable=False),
        sa.Column("audience", sa.JSON(), nullable=False),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("audience_cutoff", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cursor", sa.Text(), nullable=True),
        *state_columns(), sa.UniqueConstraint("service_id", "endpoint_kind", "external_id"))
    op.create_table("deliveries",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("request_id", sa.Uuid(), sa.ForeignKey("requests.id"), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        *state_columns(), sa.UniqueConstraint("request_id", "user_id"))
    op.create_table("outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("routing_key", sa.String(20), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("dedup_key", sa.String(200), nullable=False, unique=True),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_requests_lease_until", "requests", ["lease_until"])
    op.create_index("ix_deliveries_lease_until", "deliveries", ["lease_until"])
    op.create_index("ix_delivery_status", "deliveries", ["request_id", "status"])
    op.create_index("ix_outbox_available_at", "outbox", ["available_at"])


def downgrade():
    for table in ("outbox", "deliveries", "requests", "templates"):
        op.drop_table(table)

