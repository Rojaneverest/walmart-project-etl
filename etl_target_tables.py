#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Target Tables Setup
This script creates the target layer tables using SQLAlchemy and Snowflake.
These tables implement the final dimensional model with SCD Type 2 for appropriate dimensions.
"""

import os
import sys
from datetime import datetime, timedelta
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

# Create Snowflake engine for TARGET database
def get_snowflake_target_engine():
    """Create and return a SQLAlchemy engine for Snowflake TARGET database."""
    connection_string = (
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
        f"{SNOWFLAKE_DB_TARGET}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )
    return create_engine(connection_string)

# For backward compatibility
def get_engine():
    """Create and return a SQLAlchemy engine for Snowflake TARGET database (for backward compatibility)."""
    return get_snowflake_target_engine()

# Create metadata object
metadata = MetaData()

# Define target layer tables
def create_target_tables(metadata):
    """Create target layer tables with SCD Type 2 implementation for product and store dimensions."""
    
    # Target Date dimension
    tgt_dim_date = Table(
        'tgt_dim_date', metadata,
        Column('date_key', Integer, primary_key=True, autoincrement=True),
        Column('date_id', Integer, nullable=False, unique=True),
        Column('full_date', Date, nullable=False),
        Column('day_of_week', String(10), nullable=False, default='Unknown'),
        Column('day_of_month', Integer, nullable=False, default=1),
        Column('month', Integer, nullable=False, default=1),
        Column('month_name', String(10), nullable=False, default='Unknown'),
        Column('quarter', Integer, nullable=False, default=1),
        Column('year', Integer, nullable=False, default=2025),
        Column('is_weekend', Boolean, nullable=False, default=False),
        Column('is_holiday', Boolean, nullable=False, default=False),
        Column('fiscal_year', Integer, nullable=False, default=2025),
        Column('fiscal_quarter', Integer, nullable=False, default=1),
        Column('insertion_date', DateTime, nullable=False, default=func.now()),
        Column('modification_date', DateTime, nullable=False, default=func.now())
    )
    
    # Target Customer dimension
    tgt_dim_customer = Table(
        'tgt_dim_customer', metadata,
        Column('customer_key', Integer, primary_key=True, autoincrement=True),
        Column('customer_id', String(20), nullable=False),
        Column('customer_name', String(100), nullable=False, default='Unknown'),
        Column('customer_age', Integer),
        Column('age_group', String(20)),
        Column('customer_segment', String(50), nullable=False, default='Unknown'),
        Column('city', String(50), nullable=False, default='Unknown'),
        Column('state', String(50), nullable=False, default='Unknown'),
        Column('zip_code', String(20), nullable=False, default='00000'),
        Column('region', String(50), nullable=False, default='Unknown'),
        Column('insertion_date', DateTime, nullable=False, default=func.now()),
        Column('modification_date', DateTime, nullable=False, default=func.now())
    )
    
    # Target Supplier dimension
    tgt_dim_supplier = Table(
        'tgt_dim_supplier', metadata,
        Column('supplier_key', Integer, primary_key=True, autoincrement=True),
        Column('supplier_id', String(20), nullable=False, unique=True),
        Column('supplier_name', String(100), nullable=False, default='Unknown'),
        Column('supplier_type', String(50), nullable=False, default='Standard'),
        Column('contact_name', String(100), nullable=False, default='Unknown'),
        Column('contact_phone', String(20), nullable=False, default='000-000-0000'),
        Column('contact_email', String(100), nullable=False, default='unknown@example.com'),
        Column('insertion_date', DateTime, nullable=False, default=func.now()),
        Column('modification_date', DateTime, nullable=False, default=func.now())
    )
    
    # Target Return Reason dimension
    tgt_dim_return_reason = Table(
        'tgt_dim_return_reason', metadata,
        Column('reason_key', Integer, primary_key=True, autoincrement=True),
        Column('reason_code', String(20), nullable=False),
        Column('reason_description', String(200), nullable=False, default='Unknown'),
        Column('reason_category', String(50), nullable=False, default='Uncategorized'),
        Column('impact_level', String(20), nullable=False, default='Medium'),
        Column('is_controllable', Boolean, nullable=False, default=False),
        Column('insertion_date', DateTime, nullable=False, default=func.now()),
        Column('modification_date', DateTime, nullable=False, default=func.now())
    )
    
    # Target Product dimension (SCD Type 2)
    tgt_dim_product = Table(
        'tgt_dim_product', metadata,
        Column('product_key', Integer, primary_key=True, autoincrement=True),
        Column('product_id', String(20), nullable=False),  # Natural key
        Column('product_name', String(200), nullable=False, default='Unknown'),
        Column('product_category', String(50), nullable=False, default='Uncategorized'),
        Column('product_sub_category', String(50), nullable=False, default='Uncategorized'),
        Column('product_container', String(50), nullable=False, default='Unknown'),
        Column('unit_price', Numeric(10, 2), nullable=False, default=0.00),
        Column('price_tier', String(20), nullable=False, default='Standard'),
        Column('product_base_margin', Numeric(10, 2), nullable=False, default=0.00),
        Column('margin_percentage', Numeric(10, 2), nullable=False, default=0.00),
        Column('is_high_margin', Boolean, nullable=False, default=False),
        Column('supplier_id', String(20), ForeignKey('tgt_dim_supplier.supplier_id'), nullable=False),
        Column('supplier_name', String(100)),
        # SCD Type 2 specific columns
        Column('effective_date', Date, nullable=False),  # When this version became effective
        Column('expiry_date', Date, nullable=False),     # When this version expires (9999-12-31 for current)
        Column('is_current', Boolean, nullable=False, default=True),  # Flag for current version
        Column('version', Integer, nullable=False, default=1),  # Version number
        Column('insertion_date', DateTime, nullable=False, default=func.now()),
        Column('modification_date', DateTime, nullable=False, default=func.now())
    )
    
    # Target Store dimension (SCD Type 2)
    tgt_dim_store = Table(
        'tgt_dim_store', metadata,
        Column('store_key', Integer, primary_key=True, autoincrement=True),
        Column('store_id', String(20), nullable=False),  # Natural key
        Column('store_name', String(100), nullable=False, default='Unknown'),
        Column('location', String(200), nullable=False, default='Unknown'),
        Column('city', String(50), nullable=False, default='Unknown'),
        Column('state', String(50), nullable=False, default='Unknown'),
        Column('zip_code', String(20), nullable=False, default='00000'),
        Column('region', String(50), nullable=False, default='Unknown'),
        Column('market', String(50), nullable=False, default='Unknown'),
        # SCD Type 2 specific columns
        Column('effective_date', Date, nullable=False),  # When this version became effective
        Column('expiry_date', Date, nullable=False),     # When this version expires (9999-12-31 for current)
        Column('is_current', Boolean, nullable=False, default=True),  # Flag for current version
        Column('version', Integer, nullable=False, default=1),  # Version number
        Column('insertion_date', DateTime, nullable=False, default=func.now()),
        Column('modification_date', DateTime, nullable=False, default=func.now())
    )
    
    # Target Sales fact
    tgt_fact_sales = Table(
        'tgt_fact_sales', metadata,
        Column('sales_key', Integer, primary_key=True, autoincrement=True),
        Column('sale_id', String(20), nullable=False),
        Column('order_id', String(20), nullable=False),
        Column('row_id', Integer, nullable=False),
        Column('transaction_date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('tgt_dim_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('tgt_dim_store.store_key'), nullable=False),
        Column('customer_key', Integer, ForeignKey('tgt_dim_customer.customer_key'), nullable=False),
        Column('order_priority', String(20), nullable=False, default='Standard'),
        Column('order_quantity', Integer, nullable=False, default=0),
        Column('sales_amount', Numeric(12, 2), nullable=False, default=0.00),
        Column('discount', Float, nullable=False, default=0.00),
        Column('discount_amount', Numeric(10, 2), nullable=False, default=0.00),
        Column('shipping_cost', Numeric(10, 2), nullable=False, default=0.00),
        Column('gross_revenue', Numeric(12, 2), nullable=False, default=0.00),
        Column('net_revenue', Numeric(12, 2), nullable=False, default=0.00),
        Column('profit', Numeric(12, 2), nullable=False, default=0.00),
        Column('profit_margin', Numeric(10, 2), nullable=False, default=0.00),
        Column('is_profitable', Boolean, nullable=False, default=True),
        Column('ship_date_key', Integer, ForeignKey('tgt_dim_date.date_key')),
        Column('ship_mode', String(50), nullable=False, default='Standard'),
        Column('insertion_date', DateTime, nullable=False, default=func.now()),
        Column('modification_date', DateTime, nullable=False, default=func.now())
    )
    
    # Target Inventory fact
    tgt_fact_inventory = Table(
        'tgt_fact_inventory', metadata,
        Column('inventory_key', Integer, primary_key=True, autoincrement=True),
        Column('inventory_id', String(50), nullable=False),  # Match ODS table size
        Column('date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('tgt_dim_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('tgt_dim_store.store_key'), nullable=False),
        Column('stock_level', Integer, nullable=False, default=0),
        Column('min_stock_level', Integer, nullable=False, default=0),
        Column('max_stock_level', Integer, nullable=False, default=0),
        Column('reorder_point', Integer, nullable=False, default=0),
        Column('last_restock_date_key', Integer, ForeignKey('tgt_dim_date.date_key')),
        Column('days_of_supply', Integer),
        Column('stock_status', String(20), nullable=False, default='In Stock'),
        Column('is_in_stock', Boolean, nullable=False, default=True),
        Column('insertion_date', DateTime, nullable=False, default=func.now()),
        Column('modification_date', DateTime, nullable=False, default=func.now())
    )
    
    # Target Returns fact
    tgt_fact_returns = Table(
        'tgt_fact_returns', metadata,
        Column('return_key', Integer, primary_key=True, autoincrement=True),
        Column('return_id', String(50), nullable=False),  # Match ODS table size
        Column('return_date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('tgt_dim_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('tgt_dim_store.store_key'), nullable=False),
        Column('reason_key', Integer, ForeignKey('tgt_dim_return_reason.reason_key')),
        Column('reason_code', String(20)),
        Column('return_amount', Numeric(12, 2), nullable=False, default=0.00),
        Column('quantity_returned', Integer, nullable=False, default=0),
        Column('avg_return_price', Numeric(10, 2), nullable=False, default=0.00),
        Column('original_sale_id', String(20)),
        Column('original_sale_date_key', Integer, ForeignKey('tgt_dim_date.date_key')),
        Column('days_since_sale', Integer),
        Column('is_within_30_days', Boolean, nullable=False, default=False),
        Column('return_condition', String(50), nullable=False, default='Unknown'),
        Column('insertion_date', DateTime, nullable=False, default=func.now()),
        Column('modification_date', DateTime, nullable=False, default=func.now())
    )
    
    return {
        'tgt_dim_date': tgt_dim_date,
        'tgt_dim_customer': tgt_dim_customer,
        'tgt_dim_supplier': tgt_dim_supplier,
        'tgt_dim_return_reason': tgt_dim_return_reason,
        'tgt_dim_product': tgt_dim_product,
        'tgt_dim_store': tgt_dim_store,
        'tgt_fact_sales': tgt_fact_sales,
        'tgt_fact_inventory': tgt_fact_inventory,
        'tgt_fact_returns': tgt_fact_returns
    }

# Function to create tables
def create_tables(engine):
    """Create target tables."""
    create_target_tables(metadata)
    metadata.create_all(engine)
    print("Target tables created successfully!")

# Function to clean up existing tables
def clean_database(engine):
    """Clean up target tables before loading."""
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS tgt_fact_sales CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_fact_inventory CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_fact_returns CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_product CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_customer CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_store CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_supplier CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_return_reason CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_date CASCADE"))
    print("Target tables cleaned successfully!")

# Main function
def main():
    """Main function to create target tables."""
    # Get the Snowflake TARGET engine
    engine = get_snowflake_target_engine()
        
    # Create target tables
    create_tables(engine)
    
    print("All target tables created successfully in Snowflake TARGET database!")

if __name__ == "__main__":
    main()