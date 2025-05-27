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
        Column('dw_created_date', TIMESTAMP, default=func.now()),
        Column('dw_modified_date', TIMESTAMP),
        Column('dw_version_number', Integer, default=1)
    )
    
    # Target Customer dimension
    tgt_dim_customer = Table(
        'tgt_dim_customer', metadata,
        Column('customer_key', Integer, primary_key=True, autoincrement=True),
        Column('customer_id', String(20), nullable=False, unique=True),
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
        Column('dw_created_date', TIMESTAMP, default=func.now()),
        Column('dw_modified_date', TIMESTAMP),
        Column('dw_version_number', Integer, default=1)
    )
    
    # Target Supplier dimension
    tgt_dim_supplier = Table(
        'tgt_dim_supplier', metadata,
        Column('supplier_key', Integer, primary_key=True, autoincrement=True),
        Column('supplier_id', String(20), nullable=False, unique=True),
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
        Column('dw_created_date', TIMESTAMP, default=func.now()),
        Column('dw_modified_date', TIMESTAMP),
        Column('dw_version_number', Integer, default=1)
    )
    
    # Target Return Reason dimension
    tgt_dim_return_reason = Table(
        'tgt_dim_return_reason', metadata,
        Column('return_reason_key', Integer, primary_key=True, autoincrement=True),
        Column('reason_code', String(20), nullable=False, unique=True),
        Column('reason_description', String(200)),
        Column('category', String(50)),
        Column('impact_level', String(20)),
        Column('is_controllable', Boolean),
        Column('dw_created_date', TIMESTAMP, default=func.now()),
        Column('dw_modified_date', TIMESTAMP),
        Column('dw_version_number', Integer, default=1)
    )
    
    # Target Product dimension (SCD Type 2)
    tgt_dim_product = Table(
        'tgt_dim_product', metadata,
        Column('product_key', Integer, primary_key=True, autoincrement=True),
        Column('product_id', String(20), nullable=False),
        Column('product_name', String(200)),
        Column('category', String(50)),
        Column('subcategory', String(50)),
        Column('department', String(50)),
        Column('brand', String(50)),
        Column('price', Numeric(10, 2)),
        Column('cost', Numeric(10, 2)),
        Column('supplier_key', Integer, ForeignKey('tgt_dim_supplier.supplier_key')),
        Column('is_active', Boolean),
        Column('introduction_date', Date),
        Column('discontinuation_date', Date),
        Column('effective_date', Date, nullable=False),
        Column('expiry_date', Date),
        Column('current_indicator', Boolean, default=True),
        Column('dw_created_date', TIMESTAMP, default=func.now()),
        Column('dw_modified_date', TIMESTAMP),
        Column('dw_version_number', Integer, default=1)
    )
    
    # Target Store dimension (SCD Type 2)
    tgt_dim_store = Table(
        'tgt_dim_store', metadata,
        Column('store_key', Integer, primary_key=True, autoincrement=True),
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
        Column('current_indicator', Boolean, default=True),
        Column('dw_created_date', TIMESTAMP, default=func.now()),
        Column('dw_modified_date', TIMESTAMP),
        Column('dw_version_number', Integer, default=1)
    )
    
    # Target Sales fact
    tgt_fact_sales = Table(
        'tgt_fact_sales', metadata,
        Column('sale_key', Integer, primary_key=True, autoincrement=True),
        Column('sale_id', String(20), nullable=False, unique=True),
        Column('order_id', String(20)),
        Column('date_key', Integer, ForeignKey('tgt_dim_date.date_key'), nullable=False),
        Column('product_key', Integer, ForeignKey('tgt_dim_product.product_key'), nullable=False),
        Column('store_key', Integer, ForeignKey('tgt_dim_store.store_key'), nullable=False),
        Column('customer_key', Integer, ForeignKey('tgt_dim_customer.customer_key'), nullable=False),
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
        Column('ship_date_key', Integer, ForeignKey('tgt_dim_date.date_key')),
        Column('ship_mode', String(50)),
        Column('shipping_cost', Numeric(10, 2)),
        Column('dw_created_date', TIMESTAMP, default=func.now()),
        Column('dw_modified_date', TIMESTAMP)
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
        Column('min_stock_level', Integer),
        Column('max_stock_level', Integer),
        Column('reorder_point', Integer),
        Column('last_restock_date_key', Integer, ForeignKey('tgt_dim_date.date_key')),
        Column('days_of_supply', Integer),
        Column('stock_status', String(20)),
        Column('is_in_stock', Boolean),
        Column('dw_created_date', TIMESTAMP, default=func.now()),
        Column('dw_modified_date', TIMESTAMP)
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
        Column('original_sale_id', String(50)),  # Increased from 20 to 50 characters
        Column('original_sale_date_key', Integer, ForeignKey('tgt_dim_date.date_key')),
        Column('return_condition', String(50)),
        Column('days_since_purchase', Integer),
        Column('refund_type', String(20)),
        Column('dw_created_date', TIMESTAMP, default=func.now()),
        Column('dw_modified_date', TIMESTAMP)
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
