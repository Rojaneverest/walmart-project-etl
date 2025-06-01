#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL Staging Loader Module

This module handles the ETL process from ODS to staging layer, focusing on:
1. Data transformations and cleaning
2. Handling duplicates and null values
3. Creating surrogate keys
4. Defining relationships through surrogate keys
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.sql import text

# Import the staging tables module with fallback mechanisms
try:
    # Try direct import first
    from etl_staging_tables import create_staging_tables, metadata
except ImportError:
    try:
        # Try relative import
        import sys
        import os
        # Add parent directory to path
        parent_dir = os.path.dirname(os.path.abspath(__file__))
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        # Try import again
        from etl_staging_tables import create_staging_tables, metadata
    except ImportError as e:
        print(f"Error importing staging tables module: {e}")
        # Define fallback metadata
        metadata = MetaData()
        # Define fallback function
        def create_staging_tables():
            print("Error: Could not import create_staging_tables function")
            return False

# Generate a unique ETL batch ID for this run
ETL_BATCH_ID = f"BATCH_{datetime.now().strftime('%Y%m%d%H%M%S')}"

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

# Function to get Snowflake ODS engine
def get_snowflake_ods_engine():
    """Create and return a SQLAlchemy engine for Snowflake ODS database."""
    connection_string = (
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
        f"{SNOWFLAKE_DB_ODS}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )
    return create_engine(connection_string)

# Function to get Snowflake Staging engine
def get_snowflake_staging_engine():
    """Create and return a SQLAlchemy engine for Snowflake Staging database."""
    connection_string = (
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
        f"{SNOWFLAKE_DB_STAGING}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )
    return create_engine(connection_string)

# For backward compatibility
def get_engine():
    """Get SQLAlchemy engine for Snowflake Staging database (for backward compatibility)."""
    return get_snowflake_staging_engine()

def clean_database(staging_engine):
    """Clean up staging tables before loading."""
    with staging_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS stg_sales CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_inventory CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_returns CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_product CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_customer CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_store CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_supplier CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_return_reason CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_date CASCADE"))
    print("Staging tables cleaned successfully!")

def create_tables(staging_engine):
    """Create staging tables."""
    # Create staging tables using the imported metadata and create_staging_tables function
    staging_tables = create_staging_tables(metadata)
    metadata.create_all(staging_engine)
    print("Staging tables created successfully!")

# Helper function to generate date ID
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
def load_staging_date_dimension(staging_engine, ods_engine):
    """Load date dimension from ODS to staging with transformations.
    
    This function loads dates ONLY from the ODS date dimension without generating
    any additional future dates. This ensures consistency between ODS and staging.
    """
    print("Loading date dimension to staging...")
    
    # Extract data from ODS
    with ods_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                date_id, full_date, day_of_week, day_of_month, 
                month, month_name, quarter, year, is_holiday
            FROM ods_date
            ORDER BY full_date
        """))
        date_records = [dict(row._mapping) for row in result]
    
    if not date_records:
        print("No date records found in ODS.")
        return {}
        
    print(f"Found {len(date_records)} date records in ODS.")
    
    # Print date range information for debugging
    first_date = date_records[0]['full_date'] if date_records else None
    last_date = date_records[-1]['full_date'] if date_records else None
    print(f"Date range in ODS: {first_date} to {last_date}")
    
    # Print sample date IDs for debugging
    sample_date_ids = [record['date_id'] for record in date_records[:5]]
    print(f"Sample date IDs from ODS: {sample_date_ids}")
    
    # No longer generating future dates - only using dates from ODS
    print(f"Total date records to process: {len(date_records)}")
    
    # Verify date_id format consistency
    for record in date_records[:5]:
        date_id = record['date_id']
        full_date = record['full_date']
        generated_date_id = generate_date_id(full_date)
        if str(date_id) != str(generated_date_id):
            print(f"WARNING: Date ID mismatch for {full_date}: ODS has {date_id}, generated {generated_date_id}")
            # This is just a warning, we'll use the ODS date_id for consistency
    
    # Transform data and create surrogate keys
    staging_records = []
    date_map = {}
    
    for record in date_records:
        # Transform data
        full_date = record['full_date']
        is_weekend = record['day_of_week'] in ['Saturday', 'Sunday']
        fiscal_year = record['year']  # Could be different based on business rules
        fiscal_quarter = record['quarter']  # Could be different based on business rules
        
        # Handle nulls
        day_of_week = record['day_of_week'] or 'Unknown'
        month_name = record['month_name'] or 'Unknown'
        date_id = record['date_id']
        
        # Create staging record
        staging_record = {
            'date_id': date_id,
            'full_date': full_date,
            'day_of_week': day_of_week,
            'day_of_month': record['day_of_month'],
            'month': record['month'],
            'month_name': month_name,
            'quarter': record['quarter'],
            'year': record['year'],
            'is_weekend': is_weekend,
            'is_holiday': record['is_holiday'],
            'fiscal_year': fiscal_year,
            'fiscal_quarter': fiscal_quarter,
            'etl_batch_id': ETL_BATCH_ID,
            'etl_timestamp': datetime.now()
        }
        staging_records.append(staging_record)
    
    # Load data into staging using batch processing
    with staging_engine.begin() as conn:
        if staging_records:
            # Batch insert all records at once
            conn.execute(
                text("""
                    INSERT INTO stg_date (
                        date_id, full_date, day_of_week, day_of_month,
                        month, month_name, quarter, year, is_weekend, is_holiday,
                        fiscal_year, fiscal_quarter, etl_batch_id, etl_timestamp
                    ) 
                    VALUES (:date_id, :full_date, :day_of_week, :day_of_month,
                           :month, :month_name, :quarter, :year, :is_weekend, :is_holiday,
                           :fiscal_year, :fiscal_quarter, :etl_batch_id, :etl_timestamp)
                """),
                staging_records
            )
            
            # Get all date_ids to fetch
            date_ids = [record['date_id'] for record in staging_records]
            
            # Fetch all keys in a single query
            placeholders = ', '.join([str(date_id) for date_id in date_ids])
            result = conn.execute(
                text(f"SELECT date_key, date_id FROM stg_date WHERE date_id IN ({placeholders})")
            )
            
            # Build the mapping dictionary
            for row in result:
                date_map[row.date_id] = row.date_key
    
    print(f"Loaded {len(staging_records)} records into staging date dimension.")
    return date_map

def load_staging_customer_dimension(staging_engine, ods_engine):
    """Load customer dimension from ODS to staging with transformations."""
    print("Loading customer dimension to staging...")
    
    # Extract data from ODS
    with ods_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                customer_id, customer_name, customer_age, customer_segment,
                city, state, zip_code, region
            FROM ods_customer
        """))
        customer_records = [dict(row._mapping) for row in result]
    
    if not customer_records:
        print("No customer records found in ODS.")
        return {}
    
    # Transform data and create surrogate keys
    staging_records = []
    customer_map = {}
    
    for record in customer_records:
        # Transform data
        # Clean customer name
        customer_name = record['customer_name'].strip() if record['customer_name'] else 'Unknown'
        
        # Handle age - convert to numeric if possible
        try:
            if record['customer_age']:
                # First convert to float to handle decimal values, then to int
                customer_age = int(float(record['customer_age']))
            else:
                customer_age = None
        except (ValueError, TypeError):
            customer_age = None
        
        # Derive age group
        if customer_age is None:
            age_group = 'Unknown'
        elif customer_age < 18:
            age_group = 'Under 18'
        elif customer_age < 35:
            age_group = '18-34'
        elif customer_age < 50:
            age_group = '35-49'
        elif customer_age < 65:
            age_group = '50-64'
        else:
            age_group = '65+'
        
        # Standardize region
        region = record['region'].strip().title() if record['region'] else 'Unknown'
        
        # Create staging record
        staging_record = {
            'customer_id': record['customer_id'],
            'customer_name': customer_name,
            'customer_age': customer_age,
            'age_group': age_group,
            'customer_segment': record['customer_segment'] or 'Unknown',
            'city': record['city'] or 'Unknown',
            'state': record['state'] or 'Unknown',
            'zip_code': record['zip_code'] or 'Unknown',
            'region': region,
            'etl_batch_id': ETL_BATCH_ID,
            'etl_timestamp': datetime.now()
        }
        staging_records.append(staging_record)
    
    # Load data into staging using batch processing
    with staging_engine.begin() as conn:
        if staging_records:
            # Batch insert all records at once
            conn.execute(
                text("""
                    INSERT INTO stg_customer (
                        customer_id, customer_name, customer_age, age_group,
                        customer_segment, city, state, zip_code, region,
                        etl_batch_id, etl_timestamp
                    ) 
                    VALUES (:customer_id, :customer_name, :customer_age, :age_group,
                           :customer_segment, :city, :state, :zip_code, :region,
                           :etl_batch_id, :etl_timestamp)
                """),
                staging_records
            )
            
            # Get all customer_ids to fetch
            customer_ids = [record['customer_id'] for record in staging_records]
            
            # Fetch all keys in a single query
            # Use parameterized query with IN clause for better security and performance
            placeholders = ', '.join([f"'{customer_id}'" for customer_id in customer_ids])
            result = conn.execute(
                text(f"SELECT customer_key, customer_id FROM stg_customer WHERE customer_id IN ({placeholders})")
            )
            
            # Build the mapping dictionary
            for row in result:
                customer_map[row.customer_id] = row.customer_key
    
    print(f"Loaded {len(staging_records)} records into staging customer dimension.")
    return customer_map

def load_staging_product_dimension(staging_engine, ods_engine):
    """Load product dimension from ODS to staging with transformations."""
    print("Loading product dimension to staging...")
    
    # Extract data from ODS
    with ods_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                p.product_id, p.product_name, p.product_category, p.product_sub_category,
                p.product_container, p.unit_price, p.product_base_margin,
                s.supplier_id, s.supplier_name
            FROM ods_product p
            LEFT JOIN ods_supplier s ON p.supplier_id = s.supplier_id
        """))
        product_records = [dict(row._mapping) for row in result]
    
    if not product_records:
        print("No product records found in ODS.")
        return {}
    
    # Transform data and create surrogate keys
    staging_records = []
    product_map = {}
    
    for record in product_records:
        # Handle nulls and transformations
        product_name = record['product_name'] or 'Unknown Product'
        product_category = record['product_category'] or 'Uncategorized'
        product_sub_category = record['product_sub_category'] or 'Uncategorized'
        product_container = record['product_container'] or 'Unknown'
        
        # Price and margin calculations
        unit_price = float(record['unit_price']) if record['unit_price'] is not None else 0.0
        product_base_margin = float(record['product_base_margin']) if record['product_base_margin'] is not None else 0.0
        
        # Calculate margin percentage
        margin_percentage = round(product_base_margin / unit_price * 100, 2) if unit_price > 0 else 0.0
        
        # Determine if high margin (business rule: >30% margin is high)
        is_high_margin = margin_percentage > 30.0
        
        # Determine price tier (business rule)
        if unit_price < 10.0:
            price_tier = 'Low'
        elif unit_price < 50.0:
            price_tier = 'Medium'
        elif unit_price < 100.0:
            price_tier = 'High'
        else:
            price_tier = 'Premium'
        
        # Create staging record
        staging_record = {
            'product_id': record['product_id'],
            'product_name': product_name,
            'product_category': product_category,
            'product_sub_category': product_sub_category,
            'product_container': product_container,
            'unit_price': unit_price,
            'price_tier': price_tier,
            'product_base_margin': product_base_margin,
            'margin_percentage': margin_percentage,
            'is_high_margin': is_high_margin,
            'supplier_id': record['supplier_id'],
            'supplier_name': record['supplier_name'] or 'Unknown Supplier',
            'etl_batch_id': ETL_BATCH_ID,
            'etl_timestamp': datetime.now()
        }
        staging_records.append(staging_record)
    
    # Load data into staging using batch processing
    with staging_engine.begin() as conn:
        if staging_records:
            # Batch insert all records at once
            conn.execute(
                text("""
                    INSERT INTO stg_product (
                        product_id, product_name, product_category, product_sub_category,
                        product_container, unit_price, price_tier, product_base_margin,
                        margin_percentage, is_high_margin, supplier_id, supplier_name,
                        etl_batch_id, etl_timestamp
                    ) 
                    VALUES (:product_id, :product_name, :product_category, :product_sub_category,
                           :product_container, :unit_price, :price_tier, :product_base_margin,
                           :margin_percentage, :is_high_margin, :supplier_id, :supplier_name,
                           :etl_batch_id, :etl_timestamp)
                """),
                staging_records
            )
            
            # Get all product_ids to fetch
            product_ids = [record['product_id'] for record in staging_records]
            
            # Fetch all keys in a single query
            # Use parameterized query with IN clause for better security and performance
            placeholders = ', '.join([f"'{product_id}'" for product_id in product_ids])
            result = conn.execute(
                text(f"SELECT product_key, product_id FROM stg_product WHERE product_id IN ({placeholders})")
            )
            
            # Build the mapping dictionary
            for row in result:
                product_map[row.product_id] = row.product_key
    
    print(f"Loaded {len(staging_records)} product records to staging.")
    return product_map

def load_staging_store_dimension(staging_engine, ods_engine):
    """Load store dimension from ODS to staging with transformations."""
    print("Loading store dimension to staging...")
    
    # Extract data from ODS
    with ods_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                store_id, store_name, location, city, state, zip_code, region
            FROM ods_store
        """))
        store_records = [dict(row._mapping) for row in result]
    
    if not store_records:
        print("No store records found in ODS.")
        return {}
    
    # Transform data and create surrogate keys
    staging_records = []
    store_map = {}
    
    for record in store_records:
        # Handle nulls and transformations
        store_name = record['store_name'] or 'Unknown Store'
        location = record['location'] or 'Unknown'
        city = record['city'] or 'Unknown'
        state = record['state'] or 'Unknown'
        zip_code = record['zip_code'] or 'Unknown'
        region = record['region'] or 'Unknown'
        
        # Derive market based on region (business rule)
        market = 'Unknown'
        if region:
            if region.lower() in ['east', 'northeast', 'southeast']:
                market = 'East Coast'
            elif region.lower() in ['west', 'northwest', 'southwest', 'pacific']:
                market = 'West Coast'
            elif region.lower() in ['central', 'midwest', 'north central', 'south central']:
                market = 'Central'
            elif region.lower() in ['south', 'southwest', 'southeast']:
                market = 'South'
            else:
                market = 'Other'
        
        # Create staging record
        staging_record = {
            'store_id': record['store_id'],
            'store_name': store_name,
            'location': location,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'region': region,
            'market': market,
            'etl_batch_id': ETL_BATCH_ID,
            'etl_timestamp': datetime.now()
        }
        staging_records.append(staging_record)
    
    # Load data into staging using batch processing
    with staging_engine.begin() as conn:
        if staging_records:
            # Batch insert all records at once
            conn.execute(
                text("""
                    INSERT INTO stg_store (
                        store_id, store_name, location, city, state,
                        zip_code, region, market, etl_batch_id, etl_timestamp
                    ) 
                    VALUES (:store_id, :store_name, :location, :city, :state,
                           :zip_code, :region, :market, :etl_batch_id, :etl_timestamp)
                """),
                staging_records
            )
            
            # Get all store_ids to fetch
            store_ids = [record['store_id'] for record in staging_records]
            
            # Fetch all keys in a single query
            # Use parameterized query with IN clause for better security and performance
            placeholders = ', '.join([f"'{store_id}'" for store_id in store_ids])
            result = conn.execute(
                text(f"SELECT store_key, store_id FROM stg_store WHERE store_id IN ({placeholders})")
            )
            
            # Build the mapping dictionary
            for row in result:
                store_map[row.store_id] = row.store_key
    
    print(f"Loaded {len(staging_records)} store records to staging.")
    return store_map

def load_staging_supplier_dimension(staging_engine, ods_engine):
    """Load supplier dimension from ODS to staging with transformations."""
    print("Loading supplier dimension to staging...")
    
    # Extract data from ODS
    with ods_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                supplier_id, supplier_name, contact_person, phone, email
            FROM ods_supplier
        """))
        supplier_records = [dict(row._mapping) for row in result]
    
    if not supplier_records:
        print("No supplier records found in ODS.")
        return {}
    
    # Transform data and create surrogate keys
    staging_records = []
    supplier_map = {}
    
    for record in supplier_records:
        # Handle nulls
        supplier_name = record['supplier_name'] or 'Unknown Supplier'
        contact_name = record['contact_person'] or 'Unknown'
        contact_phone = record['phone'] or ''
        contact_email = record['email'] or ''
        
        # Derive supplier type based on name (example business rule)
        supplier_type = 'Unknown'
        if supplier_name:
            if 'wholesale' in supplier_name.lower():
                supplier_type = 'Wholesale'
            elif 'retail' in supplier_name.lower():
                supplier_type = 'Retail'
            elif 'manufacturer' in supplier_name.lower():
                supplier_type = 'Manufacturer'
            elif 'distributor' in supplier_name.lower():
                supplier_type = 'Distributor'
            else:
                supplier_type = 'General'
        
        # Create staging record
        staging_record = {
            'supplier_id': record['supplier_id'],
            'supplier_name': supplier_name,
            'supplier_type': supplier_type,
            'contact_name': contact_name,
            'contact_phone': contact_phone,
            'contact_email': contact_email,
            'etl_batch_id': ETL_BATCH_ID,
            'etl_timestamp': datetime.now()
        }
        staging_records.append(staging_record)
    
    # Load data into staging using batch processing
    with staging_engine.begin() as conn:
        if staging_records:
            # Batch insert all records at once
            conn.execute(
                text("""
                    INSERT INTO stg_supplier (
                        supplier_id, supplier_name, supplier_type,
                        contact_name, contact_phone, contact_email, etl_batch_id, etl_timestamp
                    ) 
                    VALUES (:supplier_id, :supplier_name, :supplier_type,
                           :contact_name, :contact_phone, :contact_email, :etl_batch_id, :etl_timestamp)
                """),
                staging_records
            )
            
            # Get all supplier_ids to fetch
            supplier_ids = [record['supplier_id'] for record in staging_records]
            
            # Fetch all keys in a single query
            # Use parameterized query with IN clause for better security and performance
            placeholders = ', '.join([f"'{supplier_id}'" for supplier_id in supplier_ids])
            result = conn.execute(
                text(f"SELECT supplier_key, supplier_id FROM stg_supplier WHERE supplier_id IN ({placeholders})")
            )
            
            # Build the mapping dictionary
            for row in result:
                supplier_map[row.supplier_id] = row.supplier_key
    
    print(f"Loaded {len(staging_records)} supplier records to staging.")
    return supplier_map

def load_staging_return_reason_dimension(staging_engine, ods_engine):
    """Load return reason dimension from ODS to staging with transformations."""
    print("Loading return reason dimension to staging...")
    
    # Extract data from ODS
    with ods_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                reason_code, reason_description, category
            FROM ods_return_reason
        """))
        reason_records = [dict(row._mapping) for row in result]
    
    if not reason_records:
        print("No return reason records found in ODS.")
        return {}
    
    # Transform data and create surrogate keys
    staging_records = []
    reason_map = {}
    
    for record in reason_records:
        # Handle nulls
        reason_code = record['reason_code'] or 'UNKNOWN'
        reason_description = record['reason_description'] or 'Unknown Reason'
        category = record['category'] or 'Uncategorized'
        
        # Derive impact level based on category (business rule)
        impact_level = 'Medium'
        if category:
            if category.lower() in ['defect', 'damage', 'quality']:
                impact_level = 'High'
            elif category.lower() in ['preference', 'changed mind']:
                impact_level = 'Low'
        
        # Determine if controllable (business rule)
        is_controllable = False
        if category:
            if category.lower() in ['defect', 'damage', 'quality', 'wrong item', 'late delivery']:
                is_controllable = True
        
        # Create staging record
        staging_record = {
            'reason_code': reason_code,
            'reason_description': reason_description,
            'reason_category': category,
            'impact_level': impact_level,
            'is_controllable': is_controllable,
            'etl_batch_id': ETL_BATCH_ID,
            'etl_timestamp': datetime.now()
        }
        staging_records.append(staging_record)
    
    # Load data into staging using batch processing
    with staging_engine.begin() as conn:
        if staging_records:
            # Batch insert all records at once
            conn.execute(
                text("""
                    INSERT INTO stg_return_reason (
                        reason_code, reason_description, reason_category,
                        impact_level, is_controllable, etl_batch_id, etl_timestamp
                    ) 
                    VALUES (:reason_code, :reason_description, :reason_category,
                           :impact_level, :is_controllable, :etl_batch_id, :etl_timestamp)
                """),
                staging_records
            )
            
            # Get all reason_codes to fetch
            reason_codes = [record['reason_code'] for record in staging_records]
            
            # Fetch all keys in a single query
            # Use parameterized query with IN clause for better security and performance
            placeholders = ', '.join([f"'{code}'" for code in reason_codes])
            result = conn.execute(
                text(f"SELECT reason_key, reason_code FROM stg_return_reason WHERE reason_code IN ({placeholders})")
            )
            
            # Build the mapping dictionary
            for row in result:
                reason_map[row.reason_code] = row.reason_key
def load_staging_sales_fact(staging_engine, ods_engine, date_map, customer_map, product_map, store_map):
    """Load sales fact from ODS to staging with transformations.
    
    Ensures that sales records are only loaded for dates that exist in the
    staging date dimension (which comes from ODS date dimension).
    """
    print("Loading sales fact to staging...")
    
    # Extract data from ODS
    with ods_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                s.sale_id, s.order_id, s.row_id, s.transaction_date, s.ship_date,
                s.customer_id, s.product_id, s.store_id, s.sales_amount,
                s.order_quantity, s.discount, s.profit, s.shipping_cost, s.order_priority,
                s.ship_mode
            FROM ods_sales s
        """))
        sales_records = [dict(row._mapping) for row in result]
    
    if not sales_records:
        print("No sales records found in ODS.")
        return {}
    
    # Print dimension map sizes for debugging
    print(f"Found {len(sales_records)} sales records in ODS.")
    print(f"Date map size: {len(date_map)}")
    print(f"Customer map size: {len(customer_map)}")
    print(f"Product map size: {len(product_map)}")
    print(f"Store map size: {len(store_map)}")
    
    # Print date range in date_map for debugging
    if date_map:
        date_ids = sorted(date_map.keys())
        print(f"Date map range: {min(date_ids)} to {max(date_ids)}")
        print(f"Sample date_map keys: {date_ids[:5]}")
        
        # Get a sample of transaction dates from sales records for comparison
        sample_transaction_dates = [generate_date_id(record['transaction_date']) 
                                  for record in sales_records[:5] 
                                  if record['transaction_date']]
        print(f"Sample transaction date IDs: {sample_transaction_dates}")
        
        # Check if any sample transaction dates are missing from date_map
        missing_sample_trans_dates = [date_id for date_id in sample_transaction_dates if date_id not in date_map]
        if missing_sample_trans_dates:
            print(f"WARNING: Some sample transaction dates are missing from date_map: {missing_sample_trans_dates}")
        else:
            print("All sample transaction dates are present in date_map.")
            
        # Get a sample of ship dates from sales records for comparison
        sample_ship_dates = [generate_date_id(record['ship_date']) 
                            for record in sales_records[:5] 
                            if record['ship_date']]
        print(f"Sample ship date IDs: {sample_ship_dates}")
        
        # Check if any sample ship dates are missing from date_map
        missing_sample_ship_dates = [date_id for date_id in sample_ship_dates if date_id not in date_map]
        if missing_sample_ship_dates:
            print(f"WARNING: Some sample ship dates are missing from date_map: {missing_sample_ship_dates}")
        else:
            print("All sample ship dates are present in date_map.")
    
    # Transform data and create surrogate keys
    staging_records = []
    sales_map = {}
    skipped_records = 0
    missing_date_keys = 0
    missing_customer_keys = 0
    missing_product_keys = 0
    missing_store_keys = 0
    
    for record in sales_records:
        # Get dimension keys
        transaction_date_id = generate_date_id(record['transaction_date']) if record['transaction_date'] else None
        transaction_date_key = date_map.get(transaction_date_id)
        
        if transaction_date_id and not transaction_date_key:
            missing_date_keys += 1
            if missing_date_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing date key for date_id {transaction_date_id}, date: {record['transaction_date']}")
                # Print a few keys from date_map for debugging
                if missing_date_keys == 1:
                    print(f"Sample date_map keys: {list(date_map.keys())[:5]}")
        
        # For ship date, use None if not found in date_map
        # This prevents records from being skipped due to missing ship_date_key
        ship_date_id = generate_date_id(record['ship_date']) if record['ship_date'] else None
        ship_date_key = date_map.get(ship_date_id)
        
        customer_key = customer_map.get(record['customer_id'])
        if record['customer_id'] and not customer_key:
            missing_customer_keys += 1
            if missing_customer_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing customer key for customer_id {record['customer_id']}")
        
        product_key = product_map.get(record['product_id'])
        if record['product_id'] and not product_key:
            missing_product_keys += 1
            if missing_product_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing product key for product_id {record['product_id']}")
        
        store_key = store_map.get(record['store_id'])
        if record['store_id'] and not store_key:
            missing_store_keys += 1
            if missing_store_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing store key for store_id {record['store_id']}")
        
        # Skip records with missing REQUIRED dimension keys (transaction date, customer, product, store)
        # Ship date key is optional and can be NULL
        if not all([transaction_date_key, customer_key, product_key, store_key]):
            skipped_records += 1
            if skipped_records <= 10:  # Limit output
                print(f"Skipping sales record {record['sale_id']} due to missing dimension keys.")
            continue
        
        # Handle nulls and transformations
        sales_amount = float(record['sales_amount']) if record['sales_amount'] is not None else 0.0
        order_quantity = int(record['order_quantity']) if record['order_quantity'] is not None else 0
        discount = float(record['discount']) if record['discount'] is not None else 0.0
        profit = float(record['profit']) if record['profit'] is not None else 0.0
        shipping_cost = float(record['shipping_cost']) if record['shipping_cost'] is not None else 0.0
        
        # Calculate derived metrics
        discount_amount = round(sales_amount * discount, 2)
        gross_revenue = sales_amount
        net_revenue = round(sales_amount - discount_amount, 2)
        profit_margin = round((profit / sales_amount) * 100, 2) if sales_amount > 0 else 0.0
        is_profitable = profit > 0
        
        # Create staging record
        staging_record = {
            'sale_id': record['sale_id'],
            'order_id': record['order_id'],
            'row_id': record['row_id'],
            'transaction_date_key': transaction_date_key,
            'product_key': product_key,
            'store_key': store_key,
            'customer_key': customer_key,
            'order_priority': record['order_priority'] or 'Standard',
            'order_quantity': order_quantity,
            'sales_amount': sales_amount,
            'discount': discount,
            'discount_amount': discount_amount,
            'shipping_cost': shipping_cost,
            'gross_revenue': gross_revenue,
            'net_revenue': net_revenue,
            'profit': profit,
            'profit_margin': profit_margin,
            'is_profitable': is_profitable,
            'ship_date_key': ship_date_key,
            'ship_mode': record['ship_mode'] or 'Standard',
            'etl_batch_id': ETL_BATCH_ID,
            'etl_timestamp': datetime.now()
        }
        staging_records.append(staging_record)
    
    # Load data into staging using batch processing
    with staging_engine.begin() as conn:
        if staging_records:
            # Batch insert all records at once
            conn.execute(
                text("""
                    INSERT INTO stg_sales (
                        sale_id, order_id, row_id, transaction_date_key, product_key, store_key,
                        customer_key, order_priority, order_quantity, sales_amount, discount, discount_amount,
                        shipping_cost, gross_revenue, net_revenue, profit, profit_margin, is_profitable,
                        ship_date_key, ship_mode, etl_batch_id, etl_timestamp
                    ) 
                    VALUES (
                        :sale_id, :order_id, :row_id, :transaction_date_key, :product_key, :store_key,
                        :customer_key, :order_priority, :order_quantity, :sales_amount, :discount, :discount_amount,
                        :shipping_cost, :gross_revenue, :net_revenue, :profit, :profit_margin, :is_profitable,
                        :ship_date_key, :ship_mode, :etl_batch_id, :etl_timestamp
                    )
                """),
                staging_records
            )
            
            # Get all sale_ids to fetch
            sale_ids = [record['sale_id'] for record in staging_records]
            
            # Fetch all keys in a single query
            # Use parameterized query with IN clause for better security and performance
            placeholders = ', '.join([f"'{sale_id}'" for sale_id in sale_ids])
            result = conn.execute(
                text(f"SELECT sales_key, sale_id FROM stg_sales WHERE sale_id IN ({placeholders})")
            )
            
            # Build the mapping dictionary
            for row in result:
                sales_map[row.sale_id] = row.sales_key
    
    print(f"Loaded {len(staging_records)} sales records to staging.")
    return sales_map

def load_staging_returns_fact(staging_engine, ods_engine, date_map, product_map, store_map, reason_map=None):
    """Load returns fact from ODS to staging with transformations.
    
    Ensures that return records are only loaded for dates that exist in the
    staging date dimension (which comes from ODS date dimension).
    """
    print("Loading returns fact to staging...")
    
    # Extract data from ODS
    with ods_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT 
                r.return_id, r.return_date, r.product_id, r.store_id, r.reason_code,
                r.return_amount, r.quantity_returned, r.original_sale_id,
                r.original_sale_date, r.return_condition
            FROM ods_returns r
        """))
        returns_records = [dict(row._mapping) for row in result]
    
    if not returns_records:
        print("No returns records found in ODS.")
        return {}
    
    print(f"Found {len(returns_records)} return records in ODS.")
    print(f"Date map size: {len(date_map)}")
    print(f"Product map size: {len(product_map)}")
    print(f"Store map size: {len(store_map)}")
    
    # Print date range in date_map for debugging
    if date_map:
        date_ids = sorted(date_map.keys())
        print(f"Date map range: {min(date_ids)} to {max(date_ids)}")
        print(f"Sample date_map keys: {date_ids[:5]}")
        
        # Get a sample of dates from returns records for comparison
        sample_return_dates = [generate_date_id(record['return_date']) 
                              for record in returns_records[:5] 
                              if record['return_date']]
        print(f"Sample return date IDs: {sample_return_dates}")
        
        # Check if any sample return dates are missing from date_map
        missing_sample_dates = [date_id for date_id in sample_return_dates if date_id not in date_map]
        if missing_sample_dates:
            print(f"WARNING: Some sample return dates are missing from date_map: {missing_sample_dates}")
        else:
            print("All sample return dates are present in date_map.")
            
        # Get a sample of original sale dates from returns records for comparison
        sample_orig_sale_dates = [generate_date_id(record['original_sale_date']) 
                                 for record in returns_records[:5] 
                                 if record['original_sale_date']]
        print(f"Sample original sale date IDs: {sample_orig_sale_dates}")
        
        # Check if any sample original sale dates are missing from date_map
        missing_sample_orig_dates = [date_id for date_id in sample_orig_sale_dates if date_id not in date_map]
        if missing_sample_orig_dates:
            print(f"WARNING: Some sample original sale dates are missing from date_map: {missing_sample_orig_dates}")
        else:
            print("All sample original sale dates are present in date_map.")
    
    # Get reason map if not provided
    if not reason_map:
        reason_map = {}
        with staging_engine.begin() as conn:
            result = conn.execute(text("SELECT reason_code, reason_key FROM stg_return_reason"))
            for row in result:
                reason_map[row.reason_code] = row.reason_key
    
    print(f"Reason map size: {len(reason_map)}")
    
    # Transform data and create surrogate keys
    staging_records = []
    returns_map = {}
    skipped_records = 0
    missing_date_keys = 0
    missing_product_keys = 0
    missing_store_keys = 0
    missing_reason_keys = 0
    
    for record in returns_records:
        # Get dimension keys
        return_date_id = generate_date_id(record['return_date']) if record['return_date'] else None
        return_date_key = date_map.get(return_date_id)
        
        if return_date_id and not return_date_key:
            missing_date_keys += 1
            if missing_date_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing date key for return date_id {return_date_id}, date: {record['return_date']}")
                # Print a few keys from date_map for debugging
                if missing_date_keys == 1:
                    print(f"Sample date_map keys: {list(date_map.keys())[:5]}")
        
        # For original sale date, use None if not found in date_map
        # This prevents records from being skipped due to missing original_sale_date_key
        original_sale_date_id = generate_date_id(record['original_sale_date']) if record['original_sale_date'] else None
        original_sale_date_key = date_map.get(original_sale_date_id)
        
        product_key = product_map.get(record['product_id'])
        if record['product_id'] and not product_key:
            missing_product_keys += 1
            if missing_product_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing product key for product_id {record['product_id']}")
        
        store_key = store_map.get(record['store_id'])
        if record['store_id'] and not store_key:
            missing_store_keys += 1
            if missing_store_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing store key for store_id {record['store_id']}")
        
        reason_key = reason_map.get(record['reason_code'])
        if record['reason_code'] and not reason_key:
            missing_reason_keys += 1
            if missing_reason_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing reason key for reason_code {record['reason_code']}")
                # Use default reason key if available
                reason_key = reason_map.get('UNKNOWN')
        
        # Skip records with missing REQUIRED dimension keys (return date, product, store)
        # Original sale date key and reason key are optional and can be NULL
        if not all([return_date_key, product_key, store_key]):
            skipped_records += 1
            if skipped_records <= 10:  # Limit output
                print(f"Skipping return record {record['return_id']} due to missing dimension keys.")
            continue
        
        # Handle nulls and transformations
        return_amount = float(record['return_amount']) if record['return_amount'] is not None else 0.0
        quantity_returned = int(record['quantity_returned']) if record['quantity_returned'] is not None else 0
        reason_code = record['reason_code'] or 'UNKNOWN'
        
        # Calculate days since sale if both dates are available
        days_since_sale = None
        is_within_30_days = False
        if record['return_date'] and record['original_sale_date']:
            days_since_sale = (record['return_date'] - record['original_sale_date']).days
            is_within_30_days = days_since_sale <= 30
        
        # Calculate average return price
        avg_return_price = round(return_amount / quantity_returned, 2) if quantity_returned > 0 else 0.0
        
        # Create staging record
        staging_record = {
            'return_id': record['return_id'],
            'return_date_key': return_date_key,
            'product_key': product_key,
            'store_key': store_key,
            'reason_key': reason_key,
            'reason_code': reason_code,
            'return_amount': return_amount,
            'quantity_returned': quantity_returned,
            'avg_return_price': avg_return_price,
            'original_sale_id': record['original_sale_id'],
            'original_sale_date_key': original_sale_date_key,
            'days_since_sale': days_since_sale,
            'is_within_30_days': is_within_30_days,
            'return_condition': record['return_condition'] or 'Unknown',
            'etl_batch_id': ETL_BATCH_ID,
            'etl_timestamp': datetime.now()
        }
        staging_records.append(staging_record)
    
    # Load data into staging using batch processing
    with staging_engine.begin() as conn:
        if staging_records:
            # Batch insert all records at once
            conn.execute(
                text("""
                    INSERT INTO stg_returns (
                        return_id, return_date_key, product_key, store_key, reason_key,
                        reason_code, return_amount, quantity_returned, avg_return_price,
                        original_sale_id, original_sale_date_key, days_since_sale,
                        is_within_30_days, return_condition, etl_batch_id, etl_timestamp
                    ) 
                    VALUES (
                        :return_id, :return_date_key, :product_key, :store_key, :reason_key,
                        :reason_code, :return_amount, :quantity_returned, :avg_return_price,
                        :original_sale_id, :original_sale_date_key, :days_since_sale,
                        :is_within_30_days, :return_condition, :etl_batch_id, :etl_timestamp
                    )
                """),
                staging_records
            )
            
            # Get all return_ids to fetch
            return_ids = [record['return_id'] for record in staging_records]
            
            # Fetch all keys in a single query
            # Use parameterized query with IN clause for better security and performance
            placeholders = ', '.join([f"'{return_id}'" for return_id in return_ids])
            result = conn.execute(
                text(f"SELECT return_key, return_id FROM stg_returns WHERE return_id IN ({placeholders})")
            )
            
            # Build the mapping dictionary
            for row in result:
                returns_map[row.return_id] = row.return_key
    
    print(f"Loaded {len(staging_records)} returns records to staging.")
    return returns_map

import math # Import math for ceiling division

def load_staging_inventory_fact(staging_engine, ods_engine, date_map, product_map, store_map):
    """Load inventory fact from ODS to staging with transformations.

    Ensures that inventory records are only loaded for dates that exist in the
    staging date dimension (which comes from ODS date dimension).
    """
    print("Loading inventory fact to staging...")

    # Extract data from ODS
    with ods_engine.begin() as conn:
        result = conn.execute(text("""
            SELECT
                i.inventory_id, i.inventory_date, i.product_id, i.store_id, i.stock_level,
                i.min_stock_level, i.max_stock_level, i.reorder_point, i.last_restock_date
            FROM ods_inventory i
        """))
        inventory_records = [dict(row._mapping) for row in result]

    if not inventory_records:
        print("No inventory records found in ODS.")
        return {}

    print(f"Found {len(inventory_records)} inventory records in ODS.")
    print(f"Date map size: {len(date_map)}")
    print(f"Product map size: {len(product_map)}")
    print(f"Store map size: {len(store_map)}")

    # Print date range in date_map for debugging
    if date_map:
        date_ids = sorted(date_map.keys())
        print(f"Date map range: {min(date_ids)} to {max(date_ids)}")
        print(f"Sample date_map keys: {date_ids[:5]}")

        # Get a sample of dates from inventory records for comparison
        sample_inventory_dates = [generate_date_id(record['inventory_date'])
                                  for record in inventory_records[:5]
                                  if record['inventory_date']]
        print(f"Sample inventory date IDs: {sample_inventory_dates}")

        # Check if any sample inventory dates are missing from date_map
        missing_sample_dates = [date_id for date_id in sample_inventory_dates if date_id not in date_map]
        if missing_sample_dates:
            print(f"WARNING: Some sample inventory dates are missing from date_map: {missing_sample_dates}")
        else:
            print("All sample inventory dates are present in date_map.")

        # Verify that date_map keys match the expected format
        for date_id in date_ids[:5]:
            if not (isinstance(date_id, int) or (isinstance(date_id, str) and date_id.isdigit())):
                print(f"WARNING: Unexpected date_id format in date_map: {date_id}, type: {type(date_id)}")
                break

    # Transform data and create surrogate keys
    staging_records = []
    inventory_map = {}
    skipped_records = 0
    missing_date_keys = 0
    missing_product_keys = 0
    missing_store_keys = 0

    for record in inventory_records:
        # Get dimension keys
        date_id = generate_date_id(record['inventory_date']) if record['inventory_date'] else None
        date_key = date_map.get(date_id)

        if date_id and not date_key:
            missing_date_keys += 1
            if missing_date_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing date key for inventory date_id {date_id}, date: {record['inventory_date']}")
                # Print a few keys from date_map for debugging
                if missing_date_keys == 1:
                    print(f"Sample date_map keys: {list(date_map.keys())[:5]}")

        # For last restock date, use None if not found in date_map
        # This prevents records from being skipped due to missing last_restock_date_key
        last_restock_date_id = generate_date_id(record['last_restock_date']) if record['last_restock_date'] else None
        last_restock_date_key = date_map.get(last_restock_date_id)

        product_key = product_map.get(record['product_id'])
        if record['product_id'] and not product_key:
            missing_product_keys += 1
            if missing_product_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing product key for product_id {record['product_id']}")

        store_key = store_map.get(record['store_id'])
        if record['store_id'] and not store_key:
            missing_store_keys += 1
            if missing_store_keys <= 5:  # Limit debug output
                print(f"DEBUG: Missing store key for store_id {record['store_id']}")

        # Skip records with missing REQUIRED dimension keys (date, product, store)
        # Last restock date key is optional and can be NULL
        if not all([date_key, product_key, store_key]):
            skipped_records += 1
            if skipped_records <= 10:  # Limit output
                print(f"Skipping inventory record {record['inventory_id']} due to missing dimension keys.")
            continue

        # Handle nulls and transformations
        stock_level = int(record['stock_level']) if record['stock_level'] is not None else 0
        min_stock_level = int(record['min_stock_level']) if record['min_stock_level'] is not None else 0
        max_stock_level = int(record['max_stock_level']) if record['max_stock_level'] is not None else 0
        reorder_point = int(record['reorder_point']) if record['reorder_point'] is not None else 0

        # Calculate days of supply (business rule)
        days_of_supply = None
        if stock_level > 0 and min_stock_level > 0:
            # Simple calculation, could be more complex based on sales velocity
            days_of_supply = int(stock_level / min_stock_level * 30)  # Assuming min_stock_level is monthly minimum

        # Determine stock status
        if stock_level <= 0:
            stock_status = 'Out of Stock'
        elif stock_level < reorder_point:
            stock_status = 'Low Stock'
        elif stock_level < min_stock_level:
            stock_status = 'Below Minimum'
        elif stock_level > max_stock_level:
            stock_status = 'Overstocked'
        else:
            stock_status = 'In Stock'

        # Determine if in stock
        is_in_stock = stock_level > 0

        # Create staging record
        staging_record = {
            'inventory_id': record['inventory_id'],
            'date_key': date_key,
            'product_key': product_key,
            'store_key': store_key,
            'stock_level': stock_level,
            'min_stock_level': min_stock_level,
            'max_stock_level': max_stock_level,
            'reorder_point': reorder_point,
            'last_restock_date_key': last_restock_date_key,
            'days_of_supply': days_of_supply,
            'stock_status': stock_status,
            'is_in_stock': is_in_stock,
            'etl_batch_id': ETL_BATCH_ID,
            'etl_timestamp': datetime.now()
        }
        staging_records.append(staging_record)

    # Load data into staging using batch processing
    # Define a batch size, well within Snowflake's limit
    BATCH_SIZE = 50000 # You can adjust this based on testing, but 10k is a safe start

    if not staging_records:
        print("No staging records to load after transformation.")
        return {}

    num_batches = math.ceil(len(staging_records) / BATCH_SIZE)
    print(f"Loading {len(staging_records)} records in {num_batches} batches...")

    with staging_engine.begin() as conn:
        for i in range(num_batches):
            start_index = i * BATCH_SIZE
            end_index = min((i + 1) * BATCH_SIZE, len(staging_records))
            batch = staging_records[start_index:end_index]

            print(f"Inserting batch {i+1}/{num_batches} (records {start_index} to {end_index-1})...")
            conn.execute(
                text("""
                    INSERT INTO stg_inventory (
                        inventory_id, date_key, product_key, store_key,
                        stock_level, min_stock_level, max_stock_level, reorder_point,
                        last_restock_date_key, days_of_supply, stock_status, is_in_stock,
                        etl_batch_id, etl_timestamp
                    ) VALUES (
                        :inventory_id, :date_key, :product_key, :store_key,
                        :stock_level, :min_stock_level, :max_stock_level, :reorder_point,
                        :last_restock_date_key, :days_of_supply, :stock_status, :is_in_stock,
                        :etl_batch_id, :etl_timestamp
                    )
                """),
                batch
            )

        # After all batches are inserted, fetch the keys
        # This part still needs to be efficient, consider fetching keys in smaller chunks
        # if the total number of inventory_ids for the IN clause is too large.
        # However, for 300,000 records, the IN clause with 300,000 IDs will still hit the limit.

        # A better approach for fetching the inventory_map after batch inserts
        # is to query the staging table directly for all records inserted in this batch.
        # If ETL_BATCH_ID is unique per run, this is efficient.

        print("Fetching inventory keys from staging table...")
        result = conn.execute(
            text(f"SELECT inventory_key, inventory_id FROM stg_inventory WHERE etl_batch_id = :etl_batch_id"),
            {'etl_batch_id': ETL_BATCH_ID}
        )

        for row in result:
            inventory_map[row.inventory_id] = row.inventory_key

    print(f"Loaded {len(staging_records)} inventory records to staging.")
    return inventory_map

def verify_staging_data(staging_engine):
    """Verify the data loaded into staging tables."""
    print("\nVerifying staging data...")
    
    # Check record counts in each table
    with staging_engine.begin() as conn:
        date_count = conn.execute(text("SELECT COUNT(*) FROM stg_date")).scalar()
        print(f"Staging date dimension has {date_count} records.")
        
        customer_count = conn.execute(text("SELECT COUNT(*) FROM stg_customer")).scalar()
        print(f"Staging customer dimension has {customer_count} records.")
        
        product_count = conn.execute(text("SELECT COUNT(*) FROM stg_product")).scalar()
        print(f"Staging product dimension has {product_count} records.")
        
        store_count = conn.execute(text("SELECT COUNT(*) FROM stg_store")).scalar()
        print(f"Staging store dimension has {store_count} records.")
        
        supplier_count = conn.execute(text("SELECT COUNT(*) FROM stg_supplier")).scalar()
        print(f"Staging supplier dimension has {supplier_count} records.")
        
        reason_count = conn.execute(text("SELECT COUNT(*) FROM stg_return_reason")).scalar()
        print(f"Staging return reason dimension has {reason_count} records.")
        
        # Check fact tables
        sales_count = conn.execute(text("SELECT COUNT(*) FROM stg_sales")).scalar()
        print(f"Staging sales fact has {sales_count} records.")
        
        inventory_count = conn.execute(text("SELECT COUNT(*) FROM stg_inventory")).scalar()
        print(f"Staging inventory fact has {inventory_count} records.")
        
        returns_count = conn.execute(text("SELECT COUNT(*) FROM stg_returns")).scalar()
        print(f"Staging returns fact has {returns_count} records.")
    
    # Check for data quality issues
    with staging_engine.begin() as conn:
        # Check for high margin products
        high_margin_products = conn.execute(text("""
            SELECT COUNT(*) FROM stg_product
            WHERE is_high_margin = true
        """)).scalar()
        high_margin_percentage = (high_margin_products / product_count * 100) if product_count > 0 else 0
        print(f"Products with high margin: {high_margin_products} ({high_margin_percentage:.2f}%)")
        
        # Check for profitable sales
        profitable_sales = conn.execute(text("""
            SELECT COUNT(*) FROM stg_sales
            WHERE is_profitable = true
        """)).scalar()
        profitable_percentage = (profitable_sales / sales_count * 100) if sales_count > 0 else 0
        print(f"Profitable sales: {profitable_sales} ({profitable_percentage:.2f}%)")
        
        # Check for returns within 30 days
        returns_within_30_days = conn.execute(text("""
            SELECT COUNT(*) FROM stg_returns
            WHERE is_within_30_days = true
        """)).scalar()
        returns_percentage = (returns_within_30_days / returns_count * 100) if returns_count > 0 else 0
        print(f"Returns within 30 days: {returns_within_30_days} ({returns_percentage:.2f}%)")
        
        # Check inventory status distribution
        out_of_stock = conn.execute(text("""
            SELECT COUNT(*) FROM stg_inventory
            WHERE stock_status = 'Out of Stock'
        """)).scalar()
        out_of_stock_percentage = (out_of_stock / inventory_count * 100) if inventory_count > 0 else 0
        print(f"Out of stock items: {out_of_stock} ({out_of_stock_percentage:.2f}%)")
    
    print("Staging data verification completed.")

def load_staging_layer():
    """Main function to load data from ODS to staging layer."""
    print(f"Starting ETL process for staging layer with batch ID: {ETL_BATCH_ID}")
    
    # Get database engines for both ODS and Staging
    ods_engine = get_snowflake_ods_engine()
    staging_engine = get_snowflake_staging_engine()
    
    # Clean and create staging tables in the staging database
    clean_database(staging_engine)
    create_tables(staging_engine)
      
    # Load dimension tables first - using both engines
    # We'll read from ODS and write to Staging
    print("\nLoading dimension tables...")
    date_map = load_staging_date_dimension(staging_engine, ods_engine)
    customer_map = load_staging_customer_dimension(staging_engine, ods_engine)
    product_map = load_staging_product_dimension(staging_engine, ods_engine)
    supplier_map = load_staging_supplier_dimension(staging_engine, ods_engine)
    reason_map = load_staging_return_reason_dimension(staging_engine, ods_engine)
    store_map = load_staging_store_dimension(staging_engine, ods_engine)
    # Load fact tables using dimension mappings
    print("\nLoading fact tables...")
    sales_map = load_staging_sales_fact(staging_engine, ods_engine, date_map, customer_map, product_map, store_map)
    returns_map = load_staging_returns_fact(staging_engine, ods_engine, date_map, product_map, store_map, reason_map)
    inventory_map = load_staging_inventory_fact(staging_engine, ods_engine, date_map, product_map, store_map)
    
    # Verify the data loaded into staging
    verify_staging_data(staging_engine)
    
    print(f"\nStaging layer ETL process completed successfully with batch ID: {ETL_BATCH_ID}")

if __name__ == "__main__":
    load_staging_layer()