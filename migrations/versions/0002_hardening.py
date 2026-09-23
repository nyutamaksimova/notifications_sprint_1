from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = depends_on = None


def upgrade():
    for table in ("requests", "deliveries"):
        op.add_column(table, sa.Column("channel", sa.String(20), nullable=False, server_default="email"))
        op.add_column(table, sa.Column("lane", sa.String(20), nullable=False, server_default="bulk"))
        op.create_index(f"ix_{table}_lane", table, ["lane"])
    op.add_column("requests", sa.Column("category", sa.String(20), nullable=False, server_default="marketing"))
    op.execute("UPDATE requests SET category='account' WHERE event_type='user.registered'")
    op.execute("UPDATE requests SET category='catalog' WHERE event_type='movie.published'")
    op.create_table("preferences", sa.Column("user_id", sa.Uuid(), primary_key=True),
                    sa.Column("settings", sa.JSON(), nullable=False))
    op.create_table("retry_commands", sa.Column("kind", sa.String(20), primary_key=True),
                    sa.Column("entity_id", sa.Uuid(), primary_key=True),
                    sa.Column("retry_id", sa.Uuid(), primary_key=True),
                    sa.Column("generation", sa.Integer(), nullable=False),
                    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))


def downgrade():
    op.drop_table("retry_commands")
    op.drop_table("preferences")
    op.drop_column("requests", "category")
    for table in ("deliveries", "requests"):
        op.drop_index(f"ix_{table}_lane", table_name=table)
        op.drop_column(table, "lane")
        op.drop_column(table, "channel")
