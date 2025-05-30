#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL Target Loader Module

This module handles the ETL process from staging to target layer, focusing on:
1. SCD Type 2 implementation for product and store dimensions
2. Loading fact tables with appropriate surrogate keys
3. Maintaining data integrity and historical tracking
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text, MetaData, inspect
from sqlalchemy.sql import text
from config import get_connection_string

# Import the target tables module
from etl_target_tables import create_target_tables, metadata

# Generate a unique ETL batch ID for this run
ETL_BATCH_ID = f"BATCH_{datetime.now().strftime('%Y%m%d%H%M%S')}"

# Define the far future date for SCD Type 2 expiry
FAR_FUTURE_DATE = date(9999, 12, 31)

# Helper functions
def get_engine():
    """Get SQLAlchemy engine with connection string from config."""
    connection_string = get_connection_string()
    return create_engine(connection_string)

def create_tables(engine):
    """Create target tables if they don't exist."""
    # Check if target tables already exist
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    if 'tgt_dim_date' not in existing_tables:
        create_target_tables(metadata)
        metadata.create_all(engine)
        print("Target tables created successfully!")
    else:
        print("Target tables already exist, skipping creation.")

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

def generate_date_id(date_obj):
    """Generate a date ID from a date object in format YYYYMMDD."""
    if date_obj is None:
        return None
    
    if isinstance(date_obj, datetime):
        return int(date_obj.strftime('%Y%m%d'))
    elif isinstance(date_obj, str):
        try:
            date_obj = datetime.strptime(date_obj, '%Y-%m-%d')
            return int(date_obj.strftime('%Y%m%d'))
        except ValueError:
            return None
    else:
        # Handle date objects directly
        try:
            return int(date_obj.strftime('%Y%m%d'))
        except (AttributeError, ValueError):
            return None

# Dimension loading functions
def load_target_date_dimension(engine):
    """Load date dimension from staging to target."""
    print("Loading date dimension to target...")
    
    # Extract data from staging
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                date_key as staging_date_key, date_id, full_date, day_of_week, day_of_month, 
                month, month_name, quarter, year, is_weekend, is_holiday,
                fiscal_year, fiscal_quarter
            FROM stg_date
            ORDER BY full_date
        """))
        date_records = [dict(row._mapping) for row in result]
    
    if not date_records:
        print("No date records found in staging.")
        return {}
    
    # Check for existing records in target
    existing_date_ids = set()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT date_id FROM tgt_dim_date"))
        for row in result:
            existing_date_ids.add(row.date_id)
    
    # Transform and load data
    new_records = []
    date_map = {}
    
    for record in date_records:
        date_id = record['date_id']
        
        # Skip if already exists
        if date_id in existing_date_ids:
            continue
        
        # Create target record
        target_record = {
            'date_id': date_id,
            'full_date': record['full_date'],
            'day_of_week': record['day_of_week'],
            'day_of_month': record['day_of_month'],
            'month': record['month'],
            'month_name': record['month_name'],
            'quarter': record['quarter'],
            'year': record['year'],
            'is_weekend': record['is_weekend'],
            'is_holiday': record['is_holiday'],
            'fiscal_year': record['fiscal_year'],
            'fiscal_quarter': record['fiscal_quarter'],
            'insertion_date': datetime.now(),
            'modification_date': datetime.now()
        }
        new_records.append(target_record)
    
    # Insert new records
    with engine.begin() as conn:
        for record in new_records:
            result = conn.execute(
                text("""
                    INSERT INTO tgt_dim_date (
                        date_id, full_date, day_of_week, day_of_month,
                        month, month_name, quarter, year, is_weekend, is_holiday,
                        fiscal_year, fiscal_quarter, insertion_date, modification_date
                    ) VALUES (
                        :date_id, :full_date, :day_of_week, :day_of_month,
                        :month, :month_name, :quarter, :year, :is_weekend, :is_holiday,
                        :fiscal_year, :fiscal_quarter, :insertion_date, :modification_date
                    )
                    RETURNING date_key, date_id
                """), 
                record
            )
            row = result.fetchone()
            date_map[row.date_id] = row.date_key
    
    # Get all date mappings
    with engine.begin() as conn:
        result = conn.execute(text("SELECT date_key, date_id FROM tgt_dim_date"))
        for row in result:
            date_map[row.date_id] = row.date_key
    
    print(f"Loaded {len(new_records)} new records into target date dimension.")
    return date_map

def load_target_product_dimension(engine):
    """Load product dimension from staging to target with SCD Type 2."""
    print("Loading product dimension to target with SCD Type 2...")
    
    # Extract data from staging
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                product_key as staging_product_key, product_id, product_name, 
                product_category, product_sub_category, product_container,
                unit_price, price_tier, product_base_margin, margin_percentage,
                is_high_margin, supplier_id, supplier_name
            FROM stg_product
        """))
        staging_records = [dict(row._mapping) for row in result]
    
    if not staging_records:
        print("No product records found in staging.")
        return {}
    
    # Get existing products from target
    existing_products = {}
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                product_key, product_id, product_name, product_category, 
                product_sub_category, product_container, unit_price, price_tier,
                product_base_margin, margin_percentage, is_high_margin,
                supplier_id, supplier_name, version, is_current
            FROM tgt_dim_product
            WHERE is_current = true
        """))
        for row in result:
            existing_products[row.product_id] = dict(row._mapping)
    
    # Process each staging record
    new_records = []
    updated_records = []
    product_map = {}
    current_date = datetime.now().date()
    
    for record in staging_records:
        product_id = record['product_id']
        
        if product_id in existing_products:
            # Check if product has changed
            existing = existing_products[product_id]
            has_changed = False
            
            # Compare attributes that might change
            attributes_to_compare = [
                'product_name', 'product_category', 'product_sub_category', 
                'product_container', 'unit_price', 'price_tier', 
                'product_base_margin', 'margin_percentage', 'is_high_margin',
                'supplier_id', 'supplier_name'
            ]
            
            for attr in attributes_to_compare:
                if record[attr] != existing[attr]:
                    has_changed = True
                    break
            
            if has_changed:
                # SCD Type 2: Expire the current record
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            UPDATE tgt_dim_product
                            SET is_current = false,
                                expiry_date = :expiry_date,
                                modification_date = :modification_date
                            WHERE product_key = :product_key
                        """),
                        {
                            'expiry_date': current_date - timedelta(days=1),
                            'modification_date': datetime.now(),
                            'product_key': existing['product_key']
                        }
                    )
                
                # Create new record with updated values
                new_record = {
                    'product_id': product_id,
                    'product_name': record['product_name'],
                    'product_category': record['product_category'],
                    'product_sub_category': record['product_sub_category'],
                    'product_container': record['product_container'],
                    'unit_price': record['unit_price'],
                    'price_tier': record['price_tier'],
                    'product_base_margin': record['product_base_margin'],
                    'margin_percentage': record['margin_percentage'],
                    'is_high_margin': record['is_high_margin'],
                    'supplier_id': record['supplier_id'],
                    'supplier_name': record['supplier_name'],
                    'effective_date': current_date,
                    'expiry_date': FAR_FUTURE_DATE,
                    'is_current': True,
                    'version': existing['version'] + 1,
                    'insertion_date': datetime.now(),
                    'modification_date': datetime.now()
                }
                new_records.append(new_record)
                updated_records.append(product_id)
            else:
                # No changes, use existing product key
                product_map[product_id] = existing['product_key']
        else:
            # New product, create new record
            new_record = {
                'product_id': product_id,
                'product_name': record['product_name'],
                'product_category': record['product_category'],
                'product_sub_category': record['product_sub_category'],
                'product_container': record['product_container'],
                'unit_price': record['unit_price'],
                'price_tier': record['price_tier'],
                'product_base_margin': record['product_base_margin'],
                'margin_percentage': record['margin_percentage'],
                'is_high_margin': record['is_high_margin'],
                'supplier_id': record['supplier_id'],
                'supplier_name': record['supplier_name'],
                'effective_date': current_date,
                'expiry_date': FAR_FUTURE_DATE,
                'is_current': True,
                'version': 1,
                'insertion_date': datetime.now(),
                'modification_date': datetime.now()
            }
            new_records.append(new_record)
    
    # Insert new records
    with engine.begin() as conn:
        for record in new_records:
            result = conn.execute(
                text("""
                    INSERT INTO tgt_dim_product (
                        product_id, product_name, product_category, product_sub_category,
                        product_container, unit_price, price_tier, product_base_margin,
                        margin_percentage, is_high_margin, supplier_id, supplier_name,
                        effective_date, expiry_date, is_current, version,
                        insertion_date, modification_date
                    ) VALUES (
                        :product_id, :product_name, :product_category, :product_sub_category,
                        :product_container, :unit_price, :price_tier, :product_base_margin,
                        :margin_percentage, :is_high_margin, :supplier_id, :supplier_name,
                        :effective_date, :expiry_date, :is_current, :version,
                        :insertion_date, :modification_date
                    )
                    RETURNING product_key, product_id
                """),
                record
            )
            row = result.fetchone()
            product_map[row.product_id] = row.product_key
    
    # Get all current product mappings
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT product_key, product_id 
            FROM tgt_dim_product 
            WHERE is_current = true
        """))
        for row in result:
            product_map[row.product_id] = row.product_key
    
    print(f"Loaded {len(new_records) - len(updated_records)} new products and updated {len(updated_records)} existing products.")
    return product_map

# Main function to load target layer
def load_target_customer_dimension(engine):
    """Load customer dimension from staging to target."""
    print("Loading customer dimension to target...")
    
    # Extract data from staging
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                customer_key as staging_customer_key, customer_id, customer_name, 
                customer_age, age_group, customer_segment, city, state, zip_code, region
            FROM stg_customer
        """))
        staging_records = [dict(row._mapping) for row in result]
    
    if not staging_records:
        print("No customer records found in staging.")
        return {}
    
    # Get existing customers from target
    existing_customers = {}
    with engine.begin() as conn:
        result = conn.execute(text("SELECT customer_key, customer_id FROM tgt_dim_customer"))
        for row in result:
            existing_customers[row.customer_id] = row.customer_key
    
    # Process each staging record
    new_records = []
    customer_map = {}
    
    for record in staging_records:
        customer_id = record['customer_id']
        
        if customer_id in existing_customers:
            # Customer already exists, use existing key
            customer_map[customer_id] = existing_customers[customer_id]
            
            # Update customer if needed
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE tgt_dim_customer
                        SET customer_name = :customer_name,
                            customer_age = :customer_age,
                            age_group = :age_group,
                            customer_segment = :customer_segment,
                            city = :city,
                            state = :state,
                            zip_code = :zip_code,
                            region = :region,
                            modification_date = :modification_date
                        WHERE customer_id = :customer_id
                    """),
                    {
                        'customer_id': customer_id,
                        'customer_name': record['customer_name'],
                        'customer_age': record['customer_age'],
                        'age_group': record['age_group'],
                        'customer_segment': record['customer_segment'],
                        'city': record['city'],
                        'state': record['state'],
                        'zip_code': record['zip_code'],
                        'region': record['region'],
                        'modification_date': datetime.now()
                    }
                )
        else:
            # New customer, create new record
            new_record = {
                'customer_id': customer_id,
                'customer_name': record['customer_name'],
                'customer_age': record['customer_age'],
                'age_group': record['age_group'],
                'customer_segment': record['customer_segment'],
                'city': record['city'],
                'state': record['state'],
                'zip_code': record['zip_code'],
                'region': record['region'],
                'insertion_date': datetime.now(),
                'modification_date': datetime.now()
            }
            new_records.append(new_record)
    
    # Insert new records
    with engine.begin() as conn:
        for record in new_records:
            result = conn.execute(
                text("""
                    INSERT INTO tgt_dim_customer (
                        customer_id, customer_name, customer_age, age_group,
                        customer_segment, city, state, zip_code, region,
                        insertion_date, modification_date
                    ) VALUES (
                        :customer_id, :customer_name, :customer_age, :age_group,
                        :customer_segment, :city, :state, :zip_code, :region,
                        :insertion_date, :modification_date
                    )
                    RETURNING customer_key, customer_id
                """),
                record
            )
            row = result.fetchone()
            customer_map[row.customer_id] = row.customer_key
    
    print(f"Loaded {len(new_records)} new customer records into target.")
    return customer_map

def load_target_supplier_dimension(engine):
    """Load supplier dimension from staging to target."""
    print("Loading supplier dimension to target...")
    
    # Extract data from staging
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                supplier_key as staging_supplier_key, supplier_id, supplier_name, 
                supplier_type, contact_name, contact_phone, contact_email
            FROM stg_supplier
        """))
        staging_records = [dict(row._mapping) for row in result]
    
    if not staging_records:
        print("No supplier records found in staging.")
        return {}
    
    # Get existing suppliers from target
    existing_suppliers = {}
    with engine.begin() as conn:
        result = conn.execute(text("SELECT supplier_key, supplier_id FROM tgt_dim_supplier"))
        for row in result:
            existing_suppliers[row.supplier_id] = row.supplier_key
    
    # Process each staging record
    new_records = []
    supplier_map = {}
    
    for record in staging_records:
        supplier_id = record['supplier_id']
        
        if supplier_id in existing_suppliers:
            # Supplier already exists, use existing key
            supplier_map[supplier_id] = existing_suppliers[supplier_id]
            
            # Update supplier if needed
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE tgt_dim_supplier
                        SET supplier_name = :supplier_name,
                            supplier_type = :supplier_type,
                            contact_name = :contact_name,
                            contact_phone = :contact_phone,
                            contact_email = :contact_email,
                            modification_date = :modification_date
                        WHERE supplier_id = :supplier_id
                    """),
                    {
                        'supplier_id': supplier_id,
                        'supplier_name': record['supplier_name'],
                        'supplier_type': record['supplier_type'],
                        'contact_name': record['contact_name'],
                        'contact_phone': record['contact_phone'],
                        'contact_email': record['contact_email'],
                        'modification_date': datetime.now()
                    }
                )
        else:
            # New supplier, create new record
            new_record = {
                'supplier_id': supplier_id,
                'supplier_name': record['supplier_name'],
                'supplier_type': record['supplier_type'],
                'contact_name': record['contact_name'],
                'contact_phone': record['contact_phone'],
                'contact_email': record['contact_email'],
                'insertion_date': datetime.now(),
                'modification_date': datetime.now()
            }
            new_records.append(new_record)
    
    # Insert new records
    with engine.begin() as conn:
        for record in new_records:
            result = conn.execute(
                text("""
                    INSERT INTO tgt_dim_supplier (
                        supplier_id, supplier_name, supplier_type,
                        contact_name, contact_phone, contact_email,
                        insertion_date, modification_date
                    ) VALUES (
                        :supplier_id, :supplier_name, :supplier_type,
                        :contact_name, :contact_phone, :contact_email,
                        :insertion_date, :modification_date
                    )
                    RETURNING supplier_key, supplier_id
                """),
                record
            )
            row = result.fetchone()
            supplier_map[row.supplier_id] = row.supplier_key
    
    print(f"Loaded {len(new_records)} new supplier records into target.")
    return supplier_map

def load_target_return_reason_dimension(engine):
    """Load return reason dimension from staging to target."""
    print("Loading return reason dimension to target...")
    
    # Extract data from staging
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                reason_key as staging_reason_key, reason_code, reason_description, 
                reason_category, impact_level, is_controllable
            FROM stg_return_reason
        """))
        staging_records = [dict(row._mapping) for row in result]
    
    if not staging_records:
        print("No return reason records found in staging.")
        return {}
    
    # Get existing reasons from target
    existing_reasons = {}
    with engine.begin() as conn:
        result = conn.execute(text("SELECT reason_key, reason_code FROM tgt_dim_return_reason"))
        for row in result:
            existing_reasons[row.reason_code] = row.reason_key
    
    # Process each staging record
    new_records = []
    reason_map = {}
    
    for record in staging_records:
        reason_code = record['reason_code']
        
        if reason_code in existing_reasons:
            # Reason already exists, use existing key
            reason_map[reason_code] = existing_reasons[reason_code]
            
            # Update reason if needed
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE tgt_dim_return_reason
                        SET reason_description = :reason_description,
                            reason_category = :reason_category,
                            impact_level = :impact_level,
                            is_controllable = :is_controllable,
                            modification_date = :modification_date
                        WHERE reason_code = :reason_code
                    """),
                    {
                        'reason_code': reason_code,
                        'reason_description': record['reason_description'],
                        'reason_category': record['reason_category'],
                        'impact_level': record['impact_level'],
                        'is_controllable': record['is_controllable'],
                        'modification_date': datetime.now()
                    }
                )
        else:
            # New reason, create new record
            new_record = {
                'reason_code': reason_code,
                'reason_description': record['reason_description'],
                'reason_category': record['reason_category'],
                'impact_level': record['impact_level'],
                'is_controllable': record['is_controllable'],
                'insertion_date': datetime.now(),
                'modification_date': datetime.now()
            }
            new_records.append(new_record)
    
    # Insert new records
    with engine.begin() as conn:
        for record in new_records:
            result = conn.execute(
                text("""
                    INSERT INTO tgt_dim_return_reason (
                        reason_code, reason_description, reason_category,
                        impact_level, is_controllable,
                        insertion_date, modification_date
                    ) VALUES (
                        :reason_code, :reason_description, :reason_category,
                        :impact_level, :is_controllable,
                        :insertion_date, :modification_date
                    )
                    RETURNING reason_key, reason_code
                """),
                record
            )
            row = result.fetchone()
            reason_map[row.reason_code] = row.reason_key
    
    print(f"Loaded {len(new_records)} new return reason records into target.")
    return reason_map

def load_target_store_dimension(engine):
    """Load store dimension from staging to target with SCD Type 2."""
    print("Loading store dimension to target with SCD Type 2...")
    
    # Extract data from staging
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                store_key as staging_store_key, store_id, store_name, location,
                city, state, zip_code, region, market
            FROM stg_store
        """))
        staging_records = [dict(row._mapping) for row in result]
    
    if not staging_records:
        print("No store records found in staging.")
        return {}
    
    # Get existing stores from target
    existing_stores = {}
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                store_key, store_id, store_name, location, city, state, 
                zip_code, region, market, version, is_current
            FROM tgt_dim_store
            WHERE is_current = true
        """))
        for row in result:
            existing_stores[row.store_id] = dict(row._mapping)
    
    # Process each staging record
    new_records = []
    updated_records = []
    store_map = {}
    current_date = datetime.now().date()
    
    for record in staging_records:
        store_id = record['store_id']
        
        if store_id in existing_stores:
            # Check if store has changed
            existing = existing_stores[store_id]
            has_changed = False
            
            # Compare attributes that might change
            attributes_to_compare = [
                'store_name', 'location', 'city', 'state', 
                'zip_code', 'region', 'market'
            ]
            
            for attr in attributes_to_compare:
                if record[attr] != existing[attr]:
                    has_changed = True
                    break
            
            if has_changed:
                # SCD Type 2: Expire the current record
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            UPDATE tgt_dim_store
                            SET is_current = false,
                                expiry_date = :expiry_date,
                                modification_date = :modification_date
                            WHERE store_key = :store_key
                        """),
                        {
                            'expiry_date': current_date - timedelta(days=1),
                            'modification_date': datetime.now(),
                            'store_key': existing['store_key']
                        }
                    )
                
                # Create new record with updated values
                new_record = {
                    'store_id': store_id,
                    'store_name': record['store_name'],
                    'location': record['location'],
                    'city': record['city'],
                    'state': record['state'],
                    'zip_code': record['zip_code'],
                    'region': record['region'],
                    'market': record['market'],
                    'effective_date': current_date,
                    'expiry_date': FAR_FUTURE_DATE,
                    'is_current': True,
                    'version': existing['version'] + 1,
                    'insertion_date': datetime.now(),
                    'modification_date': datetime.now()
                }
                new_records.append(new_record)
                updated_records.append(store_id)
            else:
                # No changes, use existing store key
                store_map[store_id] = existing['store_key']
        else:
            # New store, create new record
            new_record = {
                'store_id': store_id,
                'store_name': record['store_name'],
                'location': record['location'],
                'city': record['city'],
                'state': record['state'],
                'zip_code': record['zip_code'],
                'region': record['region'],
                'market': record['market'],
                'effective_date': current_date,
                'expiry_date': FAR_FUTURE_DATE,
                'is_current': True,
                'version': 1,
                'insertion_date': datetime.now(),
                'modification_date': datetime.now()
            }
            new_records.append(new_record)
    
    # Insert new records
    with engine.begin() as conn:
        for record in new_records:
            result = conn.execute(
                text("""
                    INSERT INTO tgt_dim_store (
                        store_id, store_name, location, city, state, zip_code, region, market,
                        effective_date, expiry_date, is_current, version,
                        insertion_date, modification_date
                    ) VALUES (
                        :store_id, :store_name, :location, :city, :state, :zip_code, :region, :market,
                        :effective_date, :expiry_date, :is_current, :version,
                        :insertion_date, :modification_date
                    )
                    RETURNING store_key, store_id
                """),
                record
            )
            row = result.fetchone()
            store_map[row.store_id] = row.store_key
    
    # Get all current store mappings
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT store_key, store_id 
            FROM tgt_dim_store 
            WHERE is_current = true
        """))
        for row in result:
            store_map[row.store_id] = row.store_key
    
    print(f"Loaded {len(new_records) - len(updated_records)} new stores and updated {len(updated_records)} existing stores.")
    return store_map

def load_target_sales_fact(engine, date_map, customer_map, product_map, store_map):
    """Load sales fact from staging to target."""
    print("Loading sales fact to target...")
    
    # Get staging to target dimension key mappings
    staging_to_target = {}
    
    # Get staging date to target date mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.date_key as staging_key, t.date_key as target_key
            FROM stg_date s
            JOIN tgt_dim_date t ON s.date_id = t.date_id
        """))
        for row in result:
            staging_to_target[('date', row.staging_key)] = row.target_key
    
    # Get staging customer to target customer mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.customer_key as staging_key, t.customer_key as target_key
            FROM stg_customer s
            JOIN tgt_dim_customer t ON s.customer_id = t.customer_id
        """))
        for row in result:
            staging_to_target[('customer', row.staging_key)] = row.target_key
    
    # Get staging product to target product mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.product_key as staging_key, t.product_key as target_key
            FROM stg_product s
            JOIN tgt_dim_product t ON s.product_id = t.product_id
            WHERE t.is_current = true
        """))
        for row in result:
            staging_to_target[('product', row.staging_key)] = row.target_key
    
    # Get staging store to target store mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.store_key as staging_key, t.store_key as target_key
            FROM stg_store s
            JOIN tgt_dim_store t ON s.store_id = t.store_id
            WHERE t.is_current = true
        """))
        for row in result:
            staging_to_target[('store', row.staging_key)] = row.target_key
    
    print(f"Loaded dimension key mappings: {len(staging_to_target)} mappings found")
    
    # Extract data from staging
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                sale_id, transaction_date_key, customer_key, product_key, store_key,
                order_priority, order_quantity, sales_amount, discount, discount_amount,
                shipping_cost, gross_revenue, net_revenue, profit, profit_margin, 
                is_profitable, ship_date_key, ship_mode
            FROM stg_sales
        """))
        staging_records = [dict(row._mapping) for row in result]
    
    if not staging_records:
        print("No sales records found in staging.")
        return
    
    # Get existing sales from target
    existing_sales = set()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT sale_id FROM tgt_fact_sales"))
        for row in result:
            existing_sales.add(row.sale_id)
    
    # Process each staging record
    new_records = []
    skipped_records = 0
    
    for record in staging_records:
        sale_id = record['sale_id']
        
        # Skip if already exists
        if sale_id in existing_sales:
            continue
        
        # Get dimension keys from direct mapping
        date_key = staging_to_target.get(('date', record['transaction_date_key']))
        customer_key = staging_to_target.get(('customer', record['customer_key']))
        product_key = staging_to_target.get(('product', record['product_key']))
        store_key = staging_to_target.get(('store', record['store_key']))
        
        # Skip if missing dimension keys
        if not all([date_key, customer_key, product_key, store_key]):
            print(f"Skipping sales record {sale_id} due to missing dimension keys.")
            print(f"  Date key: {date_key}, Customer key: {customer_key}, Product key: {product_key}, Store key: {store_key}")
            print(f"  Staging keys: Date={record['transaction_date_key']}, Customer={record['customer_key']}, Product={record['product_key']}, Store={record['store_key']}")
            skipped_records += 1
            continue
        
        # Create target record
        target_record = {
            'sale_id': sale_id,
            'order_id': f"ORD-{sale_id[4:]}",  # Generate order ID from sale ID
            'row_id': int(sale_id[4:]) if sale_id[4:].isdigit() else 0,  # Extract numeric part
            'transaction_date_key': date_key,
            'customer_key': customer_key,
            'product_key': product_key,
            'store_key': store_key,
            'order_priority': record['order_priority'],
            'order_quantity': record['order_quantity'],
            'sales_amount': record['sales_amount'],
            'discount': record['discount'],
            'discount_amount': record['discount_amount'],
            'shipping_cost': record['shipping_cost'],
            'gross_revenue': record['gross_revenue'],
            'net_revenue': record['net_revenue'],
            'profit': record['profit'],
            'profit_margin': record['profit_margin'],
            'is_profitable': record['is_profitable'],
            'ship_date_key': record['ship_date_key'],
            'ship_mode': record['ship_mode'],
            'insertion_date': datetime.now(),
            'modification_date': datetime.now()
        }
        new_records.append(target_record)
    
    # Insert new records
    with engine.begin() as conn:
        for record in new_records:
            conn.execute(
                text("""
                    INSERT INTO tgt_fact_sales (
                        sale_id, order_id, row_id, transaction_date_key, product_key, store_key, customer_key,
                        order_priority, order_quantity, sales_amount, discount, discount_amount,
                        shipping_cost, gross_revenue, net_revenue, profit, profit_margin, is_profitable,
                        ship_date_key, ship_mode, insertion_date, modification_date
                    ) VALUES (
                        :sale_id, :order_id, :row_id, :transaction_date_key, :product_key, :store_key, :customer_key,
                        :order_priority, :order_quantity, :sales_amount, :discount, :discount_amount,
                        :shipping_cost, :gross_revenue, :net_revenue, :profit, :profit_margin, :is_profitable,
                        :ship_date_key, :ship_mode, :insertion_date, :modification_date
                    )
                """),
                record
            )
    
    print(f"Loaded {len(new_records)} new sales records into target. Skipped {skipped_records} records due to missing dimension keys.")

def load_target_inventory_fact(engine, date_map, product_map, store_map):
    """Load inventory fact from staging to target."""
    print("Loading inventory fact to target...")
    
    # Get staging to target dimension key mappings
    staging_to_target = {}
    
    # Get staging date to target date mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.date_key as staging_key, t.date_key as target_key
            FROM stg_date s
            JOIN tgt_dim_date t ON s.date_id = t.date_id
        """))
        for row in result:
            staging_to_target[('date', row.staging_key)] = row.target_key
    
    # Get staging product to target product mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.product_key as staging_key, t.product_key as target_key
            FROM stg_product s
            JOIN tgt_dim_product t ON s.product_id = t.product_id
            WHERE t.is_current = true
        """))
        for row in result:
            staging_to_target[('product', row.staging_key)] = row.target_key
    
    # Get staging store to target store mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.store_key as staging_key, t.store_key as target_key
            FROM stg_store s
            JOIN tgt_dim_store t ON s.store_id = t.store_id
            WHERE t.is_current = true
        """))
        for row in result:
            staging_to_target[('store', row.staging_key)] = row.target_key
    
    print(f"Loaded dimension key mappings: {len(staging_to_target)} mappings found")
    
    # Get data from staging layer
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                inventory_id, date_key, product_key, store_key,
                stock_level, min_stock_level, max_stock_level, reorder_point, 
                last_restock_date_key, days_of_supply, stock_status, is_in_stock
            FROM stg_inventory
        """))
        staging_records = [dict(row._mapping) for row in result]
    
    if not staging_records:
        print("No inventory records found in staging.")
        return
    
    # Get existing inventory from target
    existing_inventory = set()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT inventory_id FROM tgt_fact_inventory"))
        for row in result:
            existing_inventory.add(row.inventory_id)
    
    # Process each staging record
    new_records = []
    skipped_records = 0
    
    for record in staging_records:
        inventory_id = record['inventory_id']
        
        # Skip if already exists
        if inventory_id in existing_inventory:
            continue
        
        # Get dimension keys from direct mapping
        date_key = staging_to_target.get(('date', record['date_key']))
        product_key = staging_to_target.get(('product', record['product_key']))
        store_key = staging_to_target.get(('store', record['store_key']))
        
        # Skip if missing dimension keys
        if not all([date_key, product_key, store_key]):
            print(f"Skipping inventory record {inventory_id} due to missing dimension keys.")
            print(f"  Date key: {date_key}, Product key: {product_key}, Store key: {store_key}")
            print(f"  Staging keys: Date={record['date_key']}, Product={record['product_key']}, Store={record['store_key']}")
            skipped_records += 1
            continue
        
        # Create target record
        target_record = {
            'inventory_id': inventory_id,
            'date_key': date_key,
            'product_key': product_key,
            'store_key': store_key,
            'stock_level': record['stock_level'],
            'min_stock_level': record['min_stock_level'],
            'max_stock_level': record['max_stock_level'],
            'reorder_point': record['reorder_point'],
            'last_restock_date_key': record['last_restock_date_key'],
            'days_of_supply': record['days_of_supply'],
            'stock_status': record['stock_status'],
            'is_in_stock': record['is_in_stock'],
            'insertion_date': datetime.now(),
            'modification_date': datetime.now()
        }
        new_records.append(target_record)
    
    # Insert new records
    with engine.begin() as conn:
        for record in new_records:
            conn.execute(
                text("""
                    INSERT INTO tgt_fact_inventory (
                        inventory_id, date_key, product_key, store_key,
                        stock_level, min_stock_level, max_stock_level, reorder_point,
                        last_restock_date_key, days_of_supply, stock_status, is_in_stock,
                        insertion_date, modification_date
                    ) VALUES (
                        :inventory_id, :date_key, :product_key, :store_key,
                        :stock_level, :min_stock_level, :max_stock_level, :reorder_point,
                        :last_restock_date_key, :days_of_supply, :stock_status, :is_in_stock,
                        :insertion_date, :modification_date
                    )
                """),
                record
            )
    
    print(f"Loaded {len(new_records)} new inventory records into target. Skipped {skipped_records} records due to missing dimension keys.")

def load_target_returns_fact(engine, date_map, product_map, store_map, reason_map):
    """Load returns fact from staging to target."""
    print("Loading returns fact to target...")
    
    # Get staging to target dimension key mappings
    staging_to_target = {}
    
    # Get staging date to target date mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.date_key as staging_key, t.date_key as target_key
            FROM stg_date s
            JOIN tgt_dim_date t ON s.date_id = t.date_id
        """))
        for row in result:
            staging_to_target[('date', row.staging_key)] = row.target_key
    
    # Get staging product to target product mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.product_key as staging_key, t.product_key as target_key
            FROM stg_product s
            JOIN tgt_dim_product t ON s.product_id = t.product_id
            WHERE t.is_current = true
        """))
        for row in result:
            staging_to_target[('product', row.staging_key)] = row.target_key
    
    # Get staging store to target store mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.store_key as staging_key, t.store_key as target_key
            FROM stg_store s
            JOIN tgt_dim_store t ON s.store_id = t.store_id
            WHERE t.is_current = true
        """))
        for row in result:
            staging_to_target[('store', row.staging_key)] = row.target_key
    
    # Get staging reason to target reason mapping
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.reason_key as staging_key, t.reason_key as target_key
            FROM stg_return_reason s
            JOIN tgt_dim_return_reason t ON s.reason_code = t.reason_code
        """))
        for row in result:
            staging_to_target[('reason', row.staging_key)] = row.target_key
    
    print(f"Loaded dimension key mappings: {len(staging_to_target)} mappings found")
    
    # Get data from staging layer
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                return_id, return_date_key, product_key, store_key, reason_key,
                reason_code, quantity_returned, return_amount, 
                original_sale_id, days_since_sale as days_since_purchase
            FROM stg_returns
        """))
        staging_records = [dict(row._mapping) for row in result]
    
    if not staging_records:
        print("No return records found in staging.")
        return
    
    # Get existing returns from target
    existing_returns = set()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT return_id FROM tgt_fact_returns"))
        for row in result:
            existing_returns.add(row.return_id)
    
    # Process each staging record
    new_records = []
    skipped_records = 0
    
    for record in staging_records:
        return_id = record['return_id']
        
        # Skip if already exists
        if return_id in existing_returns:
            continue
        
        # Get dimension keys from direct mapping
        date_key = staging_to_target.get(('date', record['return_date_key']))
        product_key = staging_to_target.get(('product', record['product_key']))
        store_key = staging_to_target.get(('store', record['store_key']))
        reason_key = staging_to_target.get(('reason', record['reason_key']))
        
        # Skip if missing dimension keys
        if not all([date_key, product_key, store_key]):
            print(f"Skipping return record {return_id} due to missing dimension keys.")
            print(f"  Date key: {date_key}, Product key: {product_key}, Store key: {store_key}")
            print(f"  Staging keys: Date={record['return_date_key']}, Product={record['product_key']}, Store={record['store_key']}")
            skipped_records += 1
            continue
        
        # Create target record
        target_record = {
            'return_id': return_id,
            'return_date_key': date_key,
            'product_key': product_key,
            'store_key': store_key,
            'reason_key': reason_key,
            'reason_code': record.get('reason_code', ''),
            'quantity_returned': record['quantity_returned'],
            'return_amount': record['return_amount'],
            'avg_return_price': float(record['return_amount']) / float(record['quantity_returned']) if float(record['quantity_returned']) > 0 else 0,
            'original_sale_id': record['original_sale_id'],
            'original_sale_date_key': date_key - (record['days_since_purchase'] if record['days_since_purchase'] else 0),
            'days_since_sale': record['days_since_purchase'],
            'is_within_30_days': record['days_since_purchase'] <= 30 if record['days_since_purchase'] else False,
            'return_condition': 'Normal',  # Default value
            'insertion_date': datetime.now(),
            'modification_date': datetime.now()
        }
        new_records.append(target_record)
    
    # Insert new records
    with engine.begin() as conn:
        for record in new_records:
            conn.execute(
                text("""
                    INSERT INTO tgt_fact_returns (
                        return_id, return_date_key, product_key, store_key, reason_key,
                        reason_code, return_amount, quantity_returned, avg_return_price,
                        original_sale_id, original_sale_date_key, days_since_sale, is_within_30_days,
                        return_condition, insertion_date, modification_date
                    ) VALUES (
                        :return_id, :return_date_key, :product_key, :store_key, :reason_key,
                        :reason_code, :return_amount, :quantity_returned, :avg_return_price,
                        :original_sale_id, :original_sale_date_key, :days_since_sale, :is_within_30_days,
                        :return_condition, :insertion_date, :modification_date
                    )
                """),
                record
            )
    
    print(f"Loaded {len(new_records)} new return records into target. Skipped {skipped_records} records due to missing dimension keys.")

def load_target_layer():
    """Main function to load data from staging to target layer."""
    print(f"Starting ETL process for target layer with batch ID: {ETL_BATCH_ID}")
    
    # Get database engine
    engine = get_engine()
     
    # Load dimension tables first
    print("\nLoading dimension tables...")
    date_map = load_target_date_dimension(engine)
    customer_map = load_target_customer_dimension(engine)
    supplier_map = load_target_supplier_dimension(engine)
    reason_map = load_target_return_reason_dimension(engine)
    product_map = load_target_product_dimension(engine)
    store_map = load_target_store_dimension(engine)
    
    # Load fact tables after dimensions
    print("\nLoading fact tables...")
    # Using direct dimension key mapping instead of the map dictionaries
    load_target_sales_fact(engine, date_map, customer_map, product_map, store_map)
    load_target_inventory_fact(engine, date_map, product_map, store_map)
    load_target_returns_fact(engine, date_map, product_map, store_map, reason_map)
    
    print(f"\nTarget layer ETL process completed successfully with batch ID: {ETL_BATCH_ID}")

if __name__ == "__main__":
    load_target_layer()
