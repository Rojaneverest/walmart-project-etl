#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Database Tables Setup
This script creates the tables for the ODS, staging, and target layers
using SQLAlchemy and PostgreSQL.
"""

import os
import pandas as pd
from datetime import datetime
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, 
    Float, Date, DateTime, Boolean, Text, ForeignKey, Numeric, 
    CheckConstraint, func
)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.sql import text

# Import configuration
from config import get_connection_string

# Create SQLAlchemy engine
def get_engine():
    """Create and return a SQLAlchemy engine for PostgreSQL."""
    connection_string = get_connection_string()
    return create_engine(connection_string)

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
        Column('source_system', String(50)),
        Column('load_timestamp', TIMESTAMP, default=func.now())
    )
    
    # ODS Customer dimension
    ods_customer = Table(
        'ods_customer', metadata,
        Column('customer_id', String(20), primary_key=True),
        Column('customer_name', String(100)),
        Column('customer_age', String(10)),  # Using string as it might have missing values
        Column('customer_segment', String(50)),
        Column('email', String(100)),
        Column('phone', String(20)),
        Column('address', String(200)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('region', String(50)),
        Column('source_system', String(50)),
        Column('load_timestamp', TIMESTAMP, default=func.now())
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
        Column('supplier_id', String(20)),
        Column('effective_date', Date),
        Column('source_system', String(50)),
        Column('load_timestamp', TIMESTAMP, default=func.now())
    )
    
    # ODS Store dimension
    ods_store = Table(
        'ods_store', metadata,
        Column('store_id', String(50), primary_key=True),  # Increased from 20 to 50 characters
        Column('store_name', String(100)),
        Column('location', String(200)),
        Column('city', String(50)),
        Column('state', String(50)),
        Column('zip_code', String(20)),
        Column('region', String(50)),
        Column('store_size_sqft', Integer),
        Column('effective_date', Date),
        Column('source_system', String(50)),
        Column('load_timestamp', TIMESTAMP, default=func.now())
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
        Column('source_system', String(50)),
        Column('load_timestamp', TIMESTAMP, default=func.now())
    )
    
    # ODS Return Reason dimension
    ods_return_reason = Table(
        'ods_return_reason', metadata,
        Column('reason_code', String(20), primary_key=True),
        Column('reason_description', String(200)),
        Column('category', String(50)),
        Column('source_system', String(50)),
        Column('load_timestamp', TIMESTAMP, default=func.now())
    )
    
    # ODS Sales fact
    ods_sales = Table(
        'ods_sales', metadata,
        Column('sale_id', String(20), primary_key=True),
        Column('order_id', String(20)),
        Column('row_id', Integer),
        Column('transaction_date', Date),
        Column('product_id', String(20)),
        Column('store_id', String(20)),
        Column('customer_id', String(20)),
        Column('order_priority', String(20)),
        Column('order_quantity', Integer),
        Column('sales_amount', Numeric(12, 2)),
        Column('discount', Float),
        Column('profit', Numeric(12, 2)),
        Column('shipping_cost', Numeric(10, 2)),
        Column('ship_date', Date),
        Column('ship_mode', String(50)),
        Column('source_system', String(50)),
        Column('load_timestamp', TIMESTAMP, default=func.now())
    )
    
    # ODS Inventory fact
    ods_inventory = Table(
        'ods_inventory', metadata,
        Column('inventory_id', String(50), primary_key=True),  # Increased from 20 to 50 characters
        Column('inventory_date', Date),
        Column('product_id', String(50)),  # Increased from 20 to 50 characters
        Column('store_id', String(50)),  # Increased from 20 to 50 characters
        Column('stock_level', Integer),
        Column('min_stock_level', Integer),
        Column('max_stock_level', Integer),
        Column('reorder_point', Integer),
        Column('last_restock_date', Date),
        Column('source_system', String(50)),
        Column('load_timestamp', TIMESTAMP, default=func.now())
    )
    
    # ODS Returns fact
    ods_returns = Table(
        'ods_returns', metadata,
        Column('return_id', String(50), primary_key=True),  # Increased from 20 to 50 characters
        Column('return_date', Date),
        Column('product_id', String(50)),  # Increased from 20 to 50 characters
        Column('store_id', String(50)),  # Increased from 20 to 50 characters
        Column('reason_code', String(20)),
        Column('return_amount', Numeric(12, 2)),
        Column('quantity_returned', Integer),
        Column('original_sale_id', String(20)),
        Column('return_condition', String(50)),
        Column('source_system', String(50)),
        Column('load_timestamp', TIMESTAMP, default=func.now())
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
    engine = get_engine()
    
    # Create ODS tables
    ods_tables = create_ods_tables(metadata)
    
    # Create all tables in the database
    metadata.create_all(engine)
    
    print("All tables created successfully!")

if __name__ == "__main__":
    main()
