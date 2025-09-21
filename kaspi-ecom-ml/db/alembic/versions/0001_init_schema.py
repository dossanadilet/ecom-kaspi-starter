"""init schema

Revision ID: 0001_init
Revises: 
Create Date: 2025-09-20
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # categories
    op.create_table(
        "categories",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(200), nullable=False, comment="Category name"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )

    # products
    op.create_table(
        "products",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("sku", sa.String(64), nullable=False, unique=True, comment="Internal SKU"),
        sa.Column("product_id", sa.String(64), nullable=True, comment="Kaspi product id"),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("category_id", sa.Integer, sa.ForeignKey("categories.id")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_products_sku", "products", ["sku"], unique=True)

    # merchants
    op.create_table(
        "merchants",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )

    # offers snapshots
    op.create_table(
        "offers",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("product_id", sa.String(64), nullable=False),
        sa.Column("sku", sa.String(64), nullable=True),
        sa.Column("merchant_id", sa.Integer, sa.ForeignKey("merchants.id")),
        sa.Column("price_list", sa.Numeric, nullable=True),
        sa.Column("price_min", sa.Numeric, nullable=True),
        sa.Column("price_default", sa.Numeric, nullable=True),
        sa.Column("available", sa.Boolean, nullable=True),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_offers_pid_ts", "offers", ["product_id", "ts"], unique=False)

    # price history
    op.create_table(
        "price_history",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("product_id", sa.String(64), nullable=False),
        sa.Column("sku", sa.String(64), nullable=True),
        sa.Column("price", sa.Numeric, nullable=False),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_price_hist_pid_ts", "price_history", ["product_id", "ts"], unique=False)

    # reviews
    op.create_table(
        "reviews",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("product_id", sa.String(64), nullable=False),
        sa.Column("rating", sa.Numeric, nullable=True),
        sa.Column("review_count", sa.Integer, nullable=True),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_reviews_pid_ts", "reviews", ["product_id", "ts"], unique=False)

    # my inventory / strategy
    op.create_table(
        "my_inventory",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("sku", sa.String(64), nullable=False),
        sa.Column("min_price", sa.Numeric, nullable=True, comment="User min price"),
        sa.Column("max_price", sa.Numeric, nullable=True, comment="User max price"),
        sa.Column("target_margin", sa.Numeric, nullable=True),
        sa.Column("sensitivity", sa.Numeric, nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_my_inventory_sku", "my_inventory", ["sku"], unique=False)

    # features daily
    op.create_table(
        "features_daily",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("sku", sa.String(64), nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("competitor_min_price", sa.Numeric, nullable=True),
        sa.Column("competitor_avg_price", sa.Numeric, nullable=True),
        sa.Column("own_price", sa.Numeric, nullable=True),
        sa.Column("sales_units", sa.Numeric, nullable=True),
        sa.Column("stock_on_hand", sa.Numeric, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_features_daily_sku_date", "features_daily", ["sku", "date"], unique=True)

    # demand forecast
    op.create_table(
        "demand_forecast",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("sku", sa.String(64), nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("q", sa.Numeric, nullable=False),
        sa.Column("model_ver", sa.String(50), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_demand_forecast_sku_date", "demand_forecast", ["sku", "date"], unique=True)

    # price reco
    op.create_table(
        "price_reco",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("sku", sa.String(64), nullable=False),
        sa.Column("price", sa.Numeric, nullable=False),
        sa.Column("expected_qty", sa.Numeric, nullable=True),
        sa.Column("expected_profit", sa.Numeric, nullable=True),
        sa.Column("explain", sa.Text, nullable=True),
        sa.Column("model_ver", sa.String(50), nullable=True),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_price_reco_sku_ts", "price_reco", ["sku", "ts"], unique=False)

    # alerts
    op.create_table(
        "alerts",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("type", sa.String(50), nullable=False),
        sa.Column("sku", sa.String(64), nullable=True),
        sa.Column("payload", sa.Text, nullable=True),
        sa.Column("ack", sa.Boolean, server_default=sa.text("false")),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("ix_alerts_ts", "alerts", ["ts"], unique=False)

    # ab tests
    op.create_table(
        "ab_tests",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("variant", sa.String(50), nullable=False),
        sa.Column("active", sa.Boolean, server_default=sa.text("true")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()")),
    )


def downgrade() -> None:
    op.drop_table("ab_tests")
    op.drop_index("ix_alerts_ts", table_name="alerts")
    op.drop_table("alerts")
    op.drop_index("ix_price_reco_sku_ts", table_name="price_reco")
    op.drop_table("price_reco")
    op.drop_index("ix_demand_forecast_sku_date", table_name="demand_forecast")
    op.drop_table("demand_forecast")
    op.drop_index("ix_features_daily_sku_date", table_name="features_daily")
    op.drop_table("features_daily")
    op.drop_index("ix_my_inventory_sku", table_name="my_inventory")
    op.drop_table("my_inventory")
    op.drop_index("ix_reviews_pid_ts", table_name="reviews")
    op.drop_table("reviews")
    op.drop_index("ix_price_hist_pid_ts", table_name="price_history")
    op.drop_table("price_history")
    op.drop_index("ix_offers_pid_ts", table_name="offers")
    op.drop_table("offers")
    op.drop_table("merchants")
    op.drop_index("ix_products_sku", table_name="products")
    op.drop_table("products")
    op.drop_table("categories")

