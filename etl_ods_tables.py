#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Database Tables Setup
This script creates the tables for the ODS, staging, and target layers
using SQLAlchemy and Snowflake.
"""

import os
import pandas as pd
from datetime import datetime
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, 
    Float, Date, DateTime, Boolean, Text, ForeignKey, Numeric, 
    CheckConstraint, func
)
from sqlalchemy.sql import text

# Snowflake connection parameters
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER', 'ROJAN')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD', 'e!Mv5ashy5aVdNb')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT', 'GEBNTIK-YU16043')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')

# Database-specific parameters
SNOWFLAKE_DB_ODS = os.environ.get('SNOWFLAKE_DB_ODS', 'ODS_DB')
SNOWFLAKE_DB_STAGING = os.environ.get('SNOWFLAKE_DB_STAGING', 'STAGING_DB')
SNOWFLAKE_DB_TARGET = os.environ.get('SNOWFLAKE_DB_TARGET', 'TARGET_DB')

# Create Snowflake engine for ODS database
def get_snowflake_ods_engine():
    """Create and return a SQLAlchemy engine for Snowflake ODS database."""
    connection_string = (
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
        f"{SNOWFLAKE_DB_ODS}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )
    return create_engine(connection_string)

# For backward compatibility
def get_engine():
    """Create and return a SQLAlchemy engine for Snowflake ODS database (for backward compatibility)."""
    return get_snowflake_ods_engine()

# Create metadata object
metadata = MetaData()

# Define ODS layer tables
def create_ods_tables(metadata):
    """Create ODS layer tables."""
    
    # ODS Date dimension
    ods_date = Table(
        'ods_date', metadata,
        Column('date_id', Integer, primary_key=True),
        Column('full_date', Date, nullable=False),
        Column('day_of_week', String(10)),
        Column('day_of_month', Integer),
        Column('month', Integer),
        Column('month_name', String(10)),
        Column('quarter', Integer),
        Column('year', Integer),
        Column('is_holiday', Boolean),
        # Removed holiday_name as specified
        Column('source_system', String(50)),
        Column('load_timestamp', DateTime, default=func.now())
    )
    
    # ODS Customer dimension
    ods_customer = Table(
        'ods_customer', metadata,
        Column('customer_id', String(20), primary_key=True),
        Column('customer_name', String(100)),
        Column('customer_age', String(10)),  # Using string as it might have missing values
        Column('customer_segment', String(50)),
        # Removed email as specified
        # Removed phone as specified
        # Removed address as specified
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('region', String(50)),
        # Removed country as specified
        Column('source_system', String(50)),
        Column('load_timestamp', DateTime, default=func.now())
    )
    
    # ODS Product dimension
    ods_product = Table(
        'ods_product', metadata,
        Column('product_id', String(20), primary_key=True),
        Column('product_name', String(200)),
        Column('product_category', String(50)),
        Column('product_sub_category', String(50)),
        Column('product_container', String(50)),
        Column('product_base_margin', Float),
        Column('unit_price', Numeric(10, 2)),
        Column('supplier_id', String(20), ForeignKey('ods_supplier.supplier_id')),
        Column('source_system', String(50)),
        Column('load_timestamp', DateTime, default=func.now())
    )
    
    # ODS Store dimension
    ods_store = Table(
        'ods_store', metadata,
        Column('store_id', String(20), primary_key=True),  # Using 20 characters for consistency
        Column('store_name', String(100)),
        Column('location', String(200)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('region', String(50)),
        # Removed store_size_sqft as specified
        # Removed opening_date as specified
        # Removed remodeling_date as specified
        Column('source_system', String(50)),
        Column('load_timestamp', DateTime, default=func.now())
    )
    
    # ODS Supplier dimension
    ods_supplier = Table(
        'ods_supplier', metadata,
        Column('supplier_id', String(20), primary_key=True),
        Column('supplier_name', String(100)),
        Column('contact_person', String(100)),
        Column('email', String(100)),
        Column('phone', String(20)),
        Column('address', String(200)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('contract_start_date', Date),
        # Removed supplier_rating as specified
        Column('source_system', String(50)),
        Column('load_timestamp', DateTime, default=func.now())
    )
    
    # ODS Return Reason dimension
    ods_return_reason = Table(
        'ods_return_reason', metadata,
        Column('reason_code', String(20), primary_key=True),
        Column('reason_description', String(200)),
        Column('category', String(50)),
        Column('source_system', String(50)),
        Column('load_timestamp', DateTime, default=func.now())
    )
    
    # ODS Sales fact
    ods_sales = Table(
        'ods_sales', metadata,
        Column('sale_id', String(20), primary_key=True),
        Column('order_id', String(20)),
        Column('row_id', Integer),
        Column('transaction_date', Date),
        Column('product_id', String(20), ForeignKey('ods_product.product_id')),
        Column('store_id', String(20), ForeignKey('ods_store.store_id')),
        Column('customer_id', String(20), ForeignKey('ods_customer.customer_id')),
        Column('order_priority', String(20)),
        Column('order_quantity', Integer),
        Column('sales_amount', Numeric(12, 2)),
        Column('discount', Float),
        Column('profit', Numeric(12, 2)),
        Column('shipping_cost', Numeric(10, 2)),
        Column('ship_date', Date),
        Column('ship_mode', String(50)),
        # Transaction-specific location fields
        Column('transaction_city', String(50)),
        Column('transaction_state', String(50)),
        Column('transaction_zip', String(20)),
        Column('product_base_margin', Float),
        Column('source_system', String(50)),
        Column('load_timestamp', DateTime, default=func.now())
    )
    
    # ODS Inventory fact
    ods_inventory = Table(
        'ods_inventory', metadata,
        Column('inventory_id', String(50), primary_key=True),
        Column('inventory_date', Date),
        Column('product_id', String(20), ForeignKey('ods_product.product_id')),  # Match product_id size
        Column('store_id', String(20), ForeignKey('ods_store.store_id')),  # Match store_id size
        Column('stock_level', Integer),
        Column('min_stock_level', Integer),
        Column('max_stock_level', Integer),
        Column('reorder_point', Integer),
        Column('last_restock_date', Date),
        Column('source_system', String(50)),
        Column('load_timestamp', DateTime, default=func.now())
    )
    
    # ODS Returns fact
    ods_returns = Table(
        'ods_returns', metadata,
        Column('return_id', String(50), primary_key=True),
        Column('return_date', Date),
        Column('product_id', String(20), ForeignKey('ods_product.product_id')),  # Match product_id size
        Column('store_id', String(20), ForeignKey('ods_store.store_id')),  # Match store_id size
        Column('reason_code', String(20), ForeignKey('ods_return_reason.reason_code')),
        Column('return_amount', Numeric(12, 2)),
        Column('quantity_returned', Integer),
        Column('original_sale_id', String(20), ForeignKey('ods_sales.sale_id')),
        Column('original_sale_date', Date),  # Added missing column
        Column('return_condition', String(50)),
        Column('source_system', String(50)),
        Column('load_timestamp', DateTime, default=func.now())
    )
    
    return {
        'ods_date': ods_date,
        'ods_customer': ods_customer,
        'ods_product': ods_product,
        'ods_store': ods_store,
        'ods_supplier': ods_supplier,
        'ods_return_reason': ods_return_reason,
        'ods_sales': ods_sales,
        'ods_inventory': ods_inventory,
        'ods_returns': ods_returns
    }

# Main function to create all tables
def main():
    """Main function to create all tables."""
    # Get the Snowflake ODS engine
    engine = get_snowflake_ods_engine()
    
    # Create ODS tables
    ods_tables = create_ods_tables(metadata)
    
    # Create all tables in the database
    metadata.create_all(engine)
    
    print("All ODS tables created successfully in Snowflake ODS database!")

if __name__ == "__main__":
    main()
