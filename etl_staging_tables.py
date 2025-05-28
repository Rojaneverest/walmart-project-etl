#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Staging Tables Setup
This script creates the staging layer tables using SQLAlchemy and PostgreSQL.
"""

from etl_tables_setup import get_engine, metadata
from sqlalchemy import (
    Table, Column, Integer, String, Float, Date, Boolean, 
    ForeignKey, Numeric, CheckConstraint, MetaData
)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.sql import func

# Define staging layer tables
def create_staging_tables(metadata):
    """Create staging layer tables."""
    
    # Staging Date dimension
    stg_date = Table(
        'stg_date', metadata,
        Column('date_key', Integer, primary_key=True),
        Column('full_date', Date, nullable=False),
        Column('day_of_week', String(10)),
        Column('day_name', String(10)),
        Column('day_of_month', Integer),
        Column('day_of_year', Integer),
        Column('week_of_year', Integer),
        Column('month', Integer),
        Column('month_name', String(10)),
        Column('quarter', Integer),
        Column('year', Integer),
        Column('is_weekend', Boolean),
        Column('is_holiday', Boolean),
        Column('holiday_name', String(50)),
        Column('fiscal_year', Integer),
        Column('fiscal_quarter', Integer),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Customer dimension
    stg_customer = Table(
        'stg_customer', metadata,
        Column('customer_key', Integer, primary_key=True),
        Column('customer_id', String(20), nullable=False),
        Column('customer_name', String(100)),
        Column('customer_age', String(10)),
        Column('customer_segment', String(50)),
        Column('email', String(100)),
        Column('phone', String(20)),
        Column('address', String(200)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('region', String(50)),
        Column('country', String(50)),
        Column('customer_since', Date),
        Column('customer_type', String(20)),
        Column('loyalty_segment', String(20)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Supplier dimension
    stg_supplier = Table(
        'stg_supplier', metadata,
        Column('supplier_key', Integer, primary_key=True),
        Column('supplier_id', String(20), nullable=False),
        Column('supplier_name', String(100)),
        Column('contact_person', String(100)),
        Column('email', String(100)),
        Column('phone', String(20)),
        Column('address', String(200)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('country', String(50)),
        Column('contract_start_date', Date),
        Column('contract_end_date', Date),
        Column('supplier_rating', Integer),
        Column('supplier_status', String(20)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Return Reason dimension
    stg_return_reason = Table(
        'stg_return_reason', metadata,
        Column('return_reason_key', Integer, primary_key=True),
        Column('reason_code', String(20), nullable=False),
        Column('reason_description', String(200)),
        Column('category', String(50)),
        Column('impact_level', String(20)),
        Column('is_controllable', Boolean),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Product dimension
    stg_product = Table(
        'stg_product', metadata,
        Column('product_key', Integer, primary_key=True),
        Column('product_id', String(20), nullable=False),
        Column('product_name', String(200)),
        Column('category', String(50)),
        Column('subcategory', String(50)),
        Column('department', String(50)),
        Column('brand', String(50)),
        Column('price', Numeric(10, 2)),
        Column('cost', Numeric(10, 2)),
        Column('supplier_key', Integer),
        Column('is_active', Boolean),
        Column('effective_date', Date, nullable=False),
        Column('expiry_date', Date),
        Column('current_flag', String(1)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Store dimension
    stg_store = Table(
        'stg_store', metadata,
        Column('store_key', Integer, primary_key=True),
        Column('store_id', String(50), nullable=False),  # Increased from 20 to 50 characters
        Column('store_name', String(100)),
        Column('store_type', String(50)),
        Column('location', String(200)),
        Column('address', String(200)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('region', String(50)),
        Column('country', String(50)),
        Column('store_size_sqft', Integer),
        Column('opening_date', Date),
        Column('remodeling_date', Date),
        Column('effective_date', Date, nullable=False),
        Column('expiry_date', Date),
        Column('current_flag', String(1)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Sales fact
    stg_sales = Table(
        'stg_sales', metadata,
        Column('sale_key', Integer, primary_key=True),
        Column('sale_id', String(20), nullable=False),
        Column('order_id', String(20)),
        Column('row_id', Integer),
        Column('date_key', Integer, nullable=False),
        Column('product_key', Integer, nullable=False),
        Column('store_key', Integer, nullable=False),
        Column('customer_key', Integer, nullable=False),
        Column('sales_amount', Numeric(12, 2), nullable=False),
        Column('quantity_sold', Integer, nullable=False),
        Column('unit_price', Numeric(10, 2)),
        Column('total_cost', Numeric(12, 2)),
        Column('profit_margin', Numeric(10, 2)),
        Column('discount_amount', Numeric(10, 2)),
        Column('net_sales_amount', Numeric(12, 2)),
        Column('sales_channel', String(20)),
        Column('promotion_key', Integer),
        Column('order_priority', String(20)),
        Column('ship_date_key', Integer),
        Column('ship_mode', String(50)),
        Column('shipping_cost', Numeric(10, 2)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Inventory fact
    stg_inventory = Table(
        'stg_inventory', metadata,
        Column('inventory_key', Integer, primary_key=True),
        Column('inventory_id', String(50), nullable=False),  # Increased from 20 to 50 characters
        Column('date_key', Integer, nullable=False),
        Column('product_key', Integer, nullable=False),
        Column('store_key', Integer, nullable=False),
        Column('stock_level', Integer, nullable=False),
        Column('min_stock_level', Integer),
        Column('max_stock_level', Integer),
        Column('reorder_point', Integer),
        Column('last_restock_date_key', Integer),
        Column('days_of_supply', Integer),
        Column('stock_status', String(20)),
        Column('is_in_stock', Boolean),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Returns fact
    stg_returns = Table(
        'stg_returns', metadata,
        Column('return_key', Integer, primary_key=True),
        Column('return_id', String(50), nullable=False),  # Increased from 20 to 50 characters
        Column('date_key', Integer, nullable=False),
        Column('product_key', Integer, nullable=False),
        Column('store_key', Integer, nullable=False),
        Column('return_reason_key', Integer, nullable=False),
        Column('return_amount', Numeric(12, 2), nullable=False),
        Column('quantity_returned', Integer, nullable=False),
        Column('original_sale_id', String(50)),  # Increased from 20 to 50 characters
        Column('original_sale_date_key', Integer),
        Column('return_condition', String(50)),
        Column('days_since_purchase', Integer),
        Column('refund_type', String(20)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    return {
        'stg_date': stg_date,
        'stg_customer': stg_customer,
        'stg_supplier': stg_supplier,
        'stg_return_reason': stg_return_reason,
        'stg_product': stg_product,
        'stg_store': stg_store,
        'stg_sales': stg_sales,
        'stg_inventory': stg_inventory,
        'stg_returns': stg_returns
    }

# Main function to create staging tables
def main():
    """Main function to create staging tables."""
    engine = get_engine()
    
    # Create staging tables
    staging_tables = create_staging_tables(metadata)
    
    # Create all tables in the database
    metadata.create_all(engine)
    
    print("Staging tables created successfully!")

if __name__ == "__main__":
    main()
