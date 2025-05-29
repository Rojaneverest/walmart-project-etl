#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Target Tables Setup
This script creates the target layer tables using SQLAlchemy and PostgreSQL.
These tables implement the final dimensional model with SCD Type 2 for appropriate dimensions.
"""

from etl_tables_setup import get_engine, metadata
from sqlalchemy import (
    Table, Column, Integer, String, Float, Date, Boolean, 
    ForeignKey, Numeric, CheckConstraint, MetaData
)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.sql import func

# Define target layer tables
def create_target_tables(metadata):
    """Create target layer tables with SCD Type 2 implementation."""
    
    # Target Date dimension
    tgt_dim_date = Table(
        'tgt_dim_date', metadata,
        Column('date_key', Integer, primary_key=True),
        Column('full_date', Date, nullable=False),
        Column('day_of_week', String(10), nullable=False, default='Unknown'),
        Column('day_name', String(10), nullable=False, default='Unknown'),
        Column('day_of_month', Integer, nullable=False, default=1),
        Column('day_of_year', Integer, nullable=False, default=1),
        Column('week_of_year', Integer, nullable=False, default=1),
        Column('month', Integer, nullable=False, default=1),
        Column('month_name', String(10), nullable=False, default='Unknown'),
        Column('quarter', Integer, nullable=False, default=1),
        Column('year', Integer, nullable=False, default=2025),
        Column('is_weekend', Boolean, nullable=False, default=False),
        Column('is_holiday', Boolean, nullable=False, default=False),
        # Removed holiday_name as specified
        Column('fiscal_year', Integer, nullable=False, default=2025),
        Column('fiscal_quarter', Integer, nullable=False, default=1),
        Column('dw_created_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_modified_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_version_number', Integer, nullable=False, default=1)
    )
    
    # Target Customer dimension
    tgt_dim_customer = Table(
        'tgt_dim_customer', metadata,
        Column('customer_key', Integer, primary_key=True, autoincrement=True),
        Column('customer_id', String(20), nullable=False, unique=True),
        Column('customer_name', String(100), nullable=False, default='Unknown'),
        # Removed customer_age as specified
        Column('customer_segment', String(50), nullable=False, default='Unknown'),
        # Removed email as specified
        # Removed phone as specified
        # Removed address as specified
        Column('city', String(50), nullable=False, default='Unknown'),
        Column('state', String(50), nullable=False, default='Unknown'),
        Column('zip_code', String(20), nullable=False, default='00000'),
        Column('region', String(50), nullable=False, default='Unknown'),
        Column('country', String(50), nullable=False, default='Unknown'),
        # Removed customer_since as specified
        Column('customer_type', String(20), nullable=False, default='Regular'),
        Column('loyalty_segment', String(20), nullable=False, default='Standard'),
        Column('dw_created_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_modified_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_version_number', Integer, nullable=False, default=1)
    )
    
    # Target Supplier dimension
    tgt_dim_supplier = Table(
        'tgt_dim_supplier', metadata,
        Column('supplier_key', Integer, primary_key=True, autoincrement=True),
        Column('supplier_id', String(20), nullable=False, unique=True),
        Column('supplier_name', String(100), nullable=False, default='Unknown'),
        Column('contact_person', String(100), nullable=False, default='Unknown'),
        Column('email', String(100), nullable=False, default='unknown@example.com'),
        Column('phone', String(20), nullable=False, default='000-000-0000'),
        Column('address', String(200), nullable=False, default='Unknown'),
        Column('city', String(50), nullable=False, default='Unknown'),
        Column('state', String(50), nullable=False, default='Unknown'),
        Column('zip_code', String(20), nullable=False, default='00000'),
        # Removed country as specified
        Column('contract_start_date', Date, nullable=False, default=func.current_date()),
        Column('contract_end_date', Date, nullable=False, default=func.current_date() + 365),
        # Removed supplier_rating as specified
        Column('supplier_status', String(20), nullable=False, default='Active'),
        Column('dw_created_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_modified_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_version_number', Integer, nullable=False, default=1)
    )
    
    # Target Return Reason dimension
    tgt_dim_return_reason = Table(
        'tgt_dim_return_reason', metadata,
        Column('return_reason_key', Integer, primary_key=True, autoincrement=True),
        Column('reason_code', String(20), nullable=False, unique=True),
        Column('reason_description', String(200), nullable=False, default='Unknown'),
        Column('category', String(50), nullable=False, default='Uncategorized'),
        Column('impact_level', String(20), nullable=False, default='Medium'),
        Column('is_controllable', Boolean, nullable=False, default=False),
        Column('dw_created_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_modified_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_version_number', Integer, nullable=False, default=1)
    )
    
    # Target Product dimension (SCD Type 2)
    tgt_dim_product = Table(
        'tgt_dim_product', metadata,
        Column('product_key', Integer, primary_key=True, autoincrement=True),
        Column('product_id', String(20), nullable=False),
        Column('product_name', String(200), nullable=False, default='Unknown'),
        Column('category', String(50), nullable=False, default='Uncategorized'),
        Column('subcategory', String(50), nullable=False, default='Uncategorized'),
        Column('department', String(50), nullable=False, default='Unknown'),
        Column('brand', String(50), nullable=False, default='Unknown'),
        Column('price', Numeric(10, 2), nullable=False, default=0.00),
        Column('cost', Numeric(10, 2), nullable=False, default=0.00),
        Column('supplier_key', Integer, ForeignKey('tgt_dim_supplier.supplier_key'), nullable=False),
        Column('is_active', Boolean, nullable=False, default=True),
        Column('effective_date', Date, nullable=False),
        Column('expiry_date', Date, nullable=False, default='9999-12-31'),
        Column('current_indicator', Boolean, nullable=False, default=True),
        Column('dw_created_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_modified_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_version_number', Integer, nullable=False, default=1)
    )
    
    # Target Store dimension (SCD Type 2)
    tgt_dim_store = Table(
        'tgt_dim_store', metadata,
        Column('store_key', Integer, primary_key=True, autoincrement=True),
        Column('store_id', String(50), nullable=False),  # Increased from 20 to 50 characters
        Column('store_name', String(100), nullable=False, default='Unknown'),
        Column('store_type', String(50), nullable=False, default='Standard'),
        Column('location', String(200), nullable=False, default='Unknown'),
        Column('address', String(200), nullable=False, default='Unknown'),
        Column('city', String(50), nullable=False, default='Unknown'),
        Column('state', String(50), nullable=False, default='Unknown'),
        Column('zip_code', String(20), nullable=False, default='00000'),
        Column('region', String(50), nullable=False, default='Unknown'),
        Column('country', String(50), nullable=False, default='Unknown'),
        # Removed store_size_sqft as specified
        # Removed opening_date as specified
        # Removed remodeling_date as specified
        Column('effective_date', Date, nullable=False),
        Column('expiry_date', Date, nullable=False, default='9999-12-31'),
        Column('current_indicator', Boolean, nullable=False, default=True),
        Column('dw_created_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_modified_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_version_number', Integer, nullable=False, default=1)
    )
    
    # Target Sales fact
    tgt_fact_sales = Table(
        'tgt_fact_sales', metadata,
        Column('sale_key', Integer, primary_key=True, autoincrement=True),
        Column('sale_id', String(20), nullable=False, unique=True),
        Column('order_id', String(20), nullable=False, default='Unknown'),
        Column('date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('tgt_dim_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('tgt_dim_store.store_key'), nullable=False),
        Column('customer_key', Integer, ForeignKey('tgt_dim_customer.customer_key'), nullable=False),
        Column('sales_amount', Numeric(12, 2), nullable=False),
        Column('quantity_sold', Integer, nullable=False),
        Column('unit_price', Numeric(10, 2), nullable=False, default=0.00),
        Column('total_cost', Numeric(12, 2), nullable=False, default=0.00),
        Column('profit_margin', Numeric(10, 2), nullable=False, default=0.00),
        Column('discount_amount', Numeric(10, 2), nullable=False, default=0.00),
        Column('net_sales_amount', Numeric(12, 2), nullable=False, default=0.00),
        Column('sales_channel', String(20), nullable=False, default='Unknown'),
        Column('promotion_key', Integer, nullable=False, default=0),
        Column('order_priority', String(20), nullable=False, default='Medium'),
        Column('ship_date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('ship_mode', String(50), nullable=False, default='Standard'),
        Column('shipping_cost', Numeric(10, 2), nullable=False, default=0.00),
        Column('dw_created_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_modified_date', TIMESTAMP, nullable=False, default=func.now())
    )
    
    # Target Inventory fact
    tgt_fact_inventory = Table(
        'tgt_fact_inventory', metadata,
        Column('inventory_key', Integer, primary_key=True, autoincrement=True),
        Column('inventory_id', String(50), nullable=False, unique=True),  # Increased from 20 to 50 characters
        Column('date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('tgt_dim_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('tgt_dim_store.store_key'), nullable=False),
        Column('stock_level', Integer, nullable=False),
        Column('min_stock_level', Integer, nullable=False, default=0),
        Column('max_stock_level', Integer, nullable=False, default=1000),
        Column('reorder_point', Integer, nullable=False, default=10),
        Column('last_restock_date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('days_of_supply', Integer, nullable=False, default=30),
        Column('stock_status', String(20), nullable=False, default='In Stock'),
        Column('is_in_stock', Boolean, nullable=False, default=True),
        Column('dw_created_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_modified_date', TIMESTAMP, nullable=False, default=func.now())
    )
    
    # Target Returns fact
    tgt_fact_returns = Table(
        'tgt_fact_returns', metadata,
        Column('return_key', Integer, primary_key=True, autoincrement=True),
        Column('return_id', String(50), nullable=False, unique=True),  # Increased from 20 to 50 characters
        Column('date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('tgt_dim_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('tgt_dim_store.store_key'), nullable=False),
        Column('return_reason_key', Integer, ForeignKey('tgt_dim_return_reason.return_reason_key'), nullable=False),
        Column('return_amount', Numeric(12, 2), nullable=False),
        Column('quantity_returned', Integer, nullable=False),
        Column('original_sale_id', String(50), nullable=False, default='Unknown'),  # Increased from 20 to 50 characters
        Column('original_sale_date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('return_condition', String(50), nullable=False, default='Unknown'),
        Column('days_since_purchase', Integer, nullable=False, default=0),
        Column('refund_type', String(20), nullable=False, default='Full'),
        Column('dw_created_date', TIMESTAMP, nullable=False, default=func.now()),
        Column('dw_modified_date', TIMESTAMP, nullable=False, default=func.now())
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

# Main function to create target tables
def main():
    """Main function to create target tables."""
    engine = get_engine()
    
    # Create target tables
    target_tables = create_target_tables(metadata)
    
    # Create all tables in the database
    metadata.create_all(engine)
    
    print("Target tables created successfully!")

if __name__ == "__main__":
    main()
