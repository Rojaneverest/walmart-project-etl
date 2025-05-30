#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Staging Tables Setup
This script creates the staging layer tables using SQLAlchemy and PostgreSQL.
"""

from sqlalchemy import (
    MetaData, Table, Column, Integer, String, Float, Date, 
    Boolean, ForeignKey, Numeric, CheckConstraint, func
)
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.sql import text

# Import configuration
from config import get_connection_string

# Create metadata object
metadata = MetaData()

def get_engine():
    """Get SQLAlchemy engine with connection string from config."""
    connection_string = get_connection_string()
    return create_engine(connection_string)

# Define staging layer tables
def create_staging_tables(metadata):
    """Create staging layer tables."""
    
    # Staging Date dimension
    stg_date = Table(
        'stg_date', metadata,
        Column('date_key', Integer, primary_key=True, autoincrement=True),
        Column('date_id', Integer, nullable=False, unique=True),  # Original ID from ODS
        Column('full_date', Date, nullable=False),
        Column('day_of_week', String(10)),
        Column('day_of_month', Integer),
        Column('month', Integer),
        Column('month_name', String(10)),
        Column('quarter', Integer),
        Column('year', Integer),
        Column('is_weekend', Boolean),  # Derived in staging
        Column('is_holiday', Boolean),
        Column('fiscal_year', Integer),  # Derived in staging
        Column('fiscal_quarter', Integer),  # Derived in staging
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Customer dimension
    stg_customer = Table(
        'stg_customer', metadata,
        Column('customer_key', Integer, primary_key=True, autoincrement=True),
        Column('customer_id', String(20), nullable=False),
        Column('customer_name', String(100)),
        Column('customer_age', Integer),  # Converted to integer in staging
        Column('age_group', String(20)),  # Derived in staging
        Column('customer_segment', String(50)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('region', String(50)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Product dimension
    stg_product = Table(
        'stg_product', metadata,
        Column('product_key', Integer, primary_key=True, autoincrement=True),
        Column('product_id', String(20), nullable=False),
        Column('product_name', String(200)),
        Column('product_category', String(50)),
        Column('product_sub_category', String(50)),
        Column('product_container', String(50)),
        Column('unit_price', Numeric(10, 2)),
        Column('price_tier', String(20)),  # Derived in staging
        Column('product_base_margin', Float),
        Column('margin_percentage', Float),  # Derived in staging
        Column('is_high_margin', Boolean),  # Derived in staging
        Column('supplier_id', String(20)),  # Original ID from ODS
        Column('supplier_name', String(100)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Store dimension
    stg_store = Table(
        'stg_store', metadata,
        Column('store_key', Integer, primary_key=True, autoincrement=True),
        Column('store_id', String(20), nullable=False),
        Column('store_name', String(100)),
        Column('location', String(200)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('region', String(50)),
        Column('market', String(50)),  # Derived in staging
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Supplier dimension
    stg_supplier = Table(
        'stg_supplier', metadata,
        Column('supplier_key', Integer, primary_key=True, autoincrement=True),
        Column('supplier_id', String(20), nullable=False),
        Column('supplier_name', String(100)),
        Column('supplier_type', String(50)),  # Derived in staging
        Column('contact_name', String(100)),
        Column('contact_phone', String(20)),
        Column('contact_email', String(100)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Return Reason dimension
    stg_return_reason = Table(
        'stg_return_reason', metadata,
        Column('reason_key', Integer, primary_key=True, autoincrement=True),
        Column('reason_code', String(20), nullable=False),
        Column('reason_description', String(200)),
        Column('reason_category', String(50)),  # Derived in staging
        Column('impact_level', String(20)),  # Derived in staging
        Column('is_controllable', Boolean),  # Derived in staging
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Sales fact
    stg_sales = Table(
        'stg_sales', metadata,
        Column('sales_key', Integer, primary_key=True, autoincrement=True),
        Column('sale_id', String(20), nullable=False),
        Column('order_id', String(20)),
        Column('row_id', Integer),
        Column('transaction_date_key', Integer, ForeignKey('stg_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('stg_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('stg_store.store_key'), nullable=False),
        Column('customer_key', Integer, ForeignKey('stg_customer.customer_key'), nullable=False),
        Column('order_priority', String(20)),
        Column('order_quantity', Integer),
        Column('sales_amount', Numeric(12, 2)),
        Column('discount', Float),
        Column('discount_amount', Numeric(10, 2)),  # Derived in staging
        Column('shipping_cost', Numeric(10, 2)),
        Column('gross_revenue', Numeric(12, 2)),  # Derived in staging
        Column('net_revenue', Numeric(12, 2)),  # Derived in staging
        Column('profit', Numeric(12, 2)),
        Column('profit_margin', Float),  # Derived in staging
        Column('is_profitable', Boolean),  # Derived in staging
        Column('ship_date_key', Integer, ForeignKey('stg_date.date_key')),
        Column('ship_mode', String(50)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Inventory fact
    stg_inventory = Table(
        'stg_inventory', metadata,
        Column('inventory_key', Integer, primary_key=True, autoincrement=True),
        Column('inventory_id', String(50), nullable=False),
        Column('date_key', Integer, ForeignKey('stg_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('stg_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('stg_store.store_key'), nullable=False),
        Column('stock_level', Integer),
        Column('min_stock_level', Integer),
        Column('max_stock_level', Integer),
        Column('reorder_point', Integer),
        Column('last_restock_date_key', Integer, ForeignKey('stg_date.date_key')),
        Column('days_of_supply', Integer),  # Derived in staging
        Column('stock_status', String(20)),  # Derived in staging
        Column('is_in_stock', Boolean),  # Derived in staging
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    # Staging Returns fact
    stg_returns = Table(
        'stg_returns', metadata,
        Column('return_key', Integer, primary_key=True, autoincrement=True),
        Column('return_id', String(50), nullable=False),
        Column('return_date_key', Integer, ForeignKey('stg_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('stg_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('stg_store.store_key'), nullable=False),
        Column('reason_key', Integer, ForeignKey('stg_return_reason.reason_key')),
        Column('reason_code', String(20)),  # Original code from ODS
        Column('return_amount', Numeric(12, 2)),
        Column('quantity_returned', Integer),
        Column('avg_return_price', Numeric(10, 2)),  # Derived in staging
        Column('original_sale_id', String(20)),
        Column('original_sale_date_key', Integer, ForeignKey('stg_date.date_key')),
        Column('days_since_sale', Integer),  # Derived in staging
        Column('is_within_30_days', Boolean),  # Derived in staging
        Column('return_condition', String(50)),
        Column('etl_batch_id', String(50)),
        Column('etl_timestamp', TIMESTAMP, default=func.now())
    )
    
    return {
        'stg_date': stg_date,
        'stg_customer': stg_customer,
        'stg_product': stg_product,
        'stg_store': stg_store,
        'stg_supplier': stg_supplier,
        'stg_return_reason': stg_return_reason,
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