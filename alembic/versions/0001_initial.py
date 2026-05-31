"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-05-31
"""
from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pipelines",
        sa.Column("id", sa.String, primary_key=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("source_type", sa.String, nullable=False),
        sa.Column("sink_type", sa.String, nullable=False),
        sa.Column("status", sa.String, default="idle"),
        sa.Column("last_run_at", sa.DateTime, nullable=True),
        sa.Column("last_watermark", sa.DateTime, nullable=True),
        sa.Column("is_running", sa.Boolean, default=False),
        sa.Column("created_at", sa.DateTime),
    )
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.String, primary_key=True),
        sa.Column("pipeline_id", sa.String, nullable=False),
        sa.Column("started_at", sa.DateTime),
        sa.Column("finished_at", sa.DateTime, nullable=True),
        sa.Column("status", sa.String, default="running"),
        sa.Column("records_processed", sa.Integer, default=0),
        sa.Column("error_message", sa.Text, nullable=True),
    )
    op.create_table(
        "source_credentials",
        sa.Column("id", sa.String, primary_key=True),
        sa.Column("pipeline_id", sa.String, nullable=False),
        sa.Column("credential_type", sa.String, nullable=False),
        sa.Column("credentials", sa.JSON, nullable=False),
        sa.Column("created_at", sa.DateTime),
    )
    op.create_table(
        "transactions",
        sa.Column("transaction_id", sa.String, primary_key=True),
        sa.Column("merchant_id", sa.String, nullable=False),
        sa.Column("customer_id", sa.String, nullable=False),
        sa.Column("amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("currency", sa.String(3), default="USD"),
        sa.Column("email", sa.String, nullable=True),
        sa.Column("phone", sa.String, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_table(
        "merchant_categories",
        sa.Column("merchant_id", sa.String, primary_key=True),
        sa.Column("category", sa.String, nullable=False),
        sa.Column("name", sa.String, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("merchant_categories")
    op.drop_table("transactions")
    op.drop_table("source_credentials")
    op.drop_table("pipeline_runs")
    op.drop_table("pipelines")
