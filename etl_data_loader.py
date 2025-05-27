#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Data Loader
This script loads data from the CSV file into the ODS tables and implements
the ETL process to move data through the three layers (ODS, staging, target).
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import text, select, func, and_
from etl_tables_setup import get_engine, metadata
import uuid

# Import configuration
from config import CSV_FILE

# Generate a batch ID for this ETL run
BATCH_ID = f"BATCH_{datetime.now().strftime('%Y%m%d%H%M%S')}"

def load_csv_to_dataframe():
    """Load CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(CSV_FILE)
        print(f"Loaded {len(df)} rows from CSV file.")
        return df
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return None

def clean_dataframe(df):
    """Clean and prepare the DataFrame for loading into ODS tables."""
    # Replace empty strings with None
    df = df.replace('', np.nan)
    
    # Convert date columns to datetime
    date_columns = ['Order Date', 'Ship Date']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')
    
    # Convert numeric columns
    numeric_columns = ['Discount', 'Order Quantity', 'Product Base Margin', 
                       'Profit', 'Sales', 'Shipping Cost', 'Unit Price']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def generate_surrogate_keys(df):
    """Generate business keys for the DataFrame.
    
    Note: Surrogate keys are now handled by auto-incrementing columns in the database.
    This function only generates business keys needed for the ETL process.
    """
    # Create a copy to avoid modifying the original
    df_with_keys = df.copy()
    
    # Generate unique business keys for each entity
    df_with_keys['sale_id'] = df_with_keys['Order ID'].astype(str) + '_' + df_with_keys['Row ID'].astype(str)
    
    # We'll rely on the database's auto-incrementing columns for surrogate keys
    # This simplifies the ETL process and makes it more maintainable
    
    return df_with_keys

def load_ods_date_dimension(engine, df):
    """Load date dimension into ODS layer."""
    # Extract unique dates from Order Date and Ship Date
    order_dates = df['Order Date'].dropna().unique()
    ship_dates = df['Ship Date'].dropna().unique()
    all_dates = np.union1d(order_dates, ship_dates)
    
    # Create date dimension records
    date_records = []
    for date in all_dates:
        date_obj = pd.to_datetime(date)
        record = {
            'date_id': int(date_obj.strftime('%Y%m%d')),
            'full_date': date_obj.date(),
            'day_of_week': date_obj.day_name(),
            'day_of_month': date_obj.day,
            'month': date_obj.month,
            'month_name': date_obj.month_name(),
            'quarter': (date_obj.month - 1) // 3 + 1,
            'year': date_obj.year,
            'is_holiday': False,  # Would need a holiday calendar to determine
            'source_system': 'CSV Import',
            'load_timestamp': datetime.now()
        }
        date_records.append(record)
    
    # Insert into ODS date dimension
    if date_records:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ods_date (
                    date_id, full_date, day_of_week, day_of_month, 
                    month, month_name, quarter, year, is_holiday, 
                    source_system, load_timestamp
                ) VALUES (
                    :date_id, :full_date, :day_of_week, :day_of_month, 
                    :month, :month_name, :quarter, :year, :is_holiday, 
                    :source_system, :load_timestamp
                )
                ON CONFLICT (date_id) DO NOTHING
            """), date_records)
        
        print(f"Loaded {len(date_records)} records into ods_date.")

def load_ods_customer_dimension(engine, df):
    """Load customer dimension into ODS layer."""
    # Extract unique customers
    customers = df[['Customer Name', 'Customer Segment', 'City', 'State', 'Zip Code', 'Region']].drop_duplicates()
    
    # Create customer records
    customer_records = []
    for _, row in customers.iterrows():
        customer_id = str(uuid.uuid4())[:20]  # Generate a unique ID
        record = {
            'customer_id': customer_id,
            'customer_name': row['Customer Name'],
            'customer_age': None,  # Not available in the dataset
            'customer_segment': row['Customer Segment'],
            'email': None,  # Not available in the dataset
            'phone': None,  # Not available in the dataset
            'address': None,  # Not available in the dataset
            'city': row['City'],
            'state': row['State'],
            'zip_code': row['Zip Code'],
            'region': row['Region'],
            'source_system': 'CSV Import',
            'load_timestamp': datetime.now()
        }
        customer_records.append(record)
    
    # Insert into ODS customer dimension
    if customer_records:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ods_customer (
                    customer_id, customer_name, customer_age, customer_segment,
                    email, phone, address, city, state, zip_code, region,
                    source_system, load_timestamp
                ) VALUES (
                    :customer_id, :customer_name, :customer_age, :customer_segment,
                    :email, :phone, :address, :city, :state, :zip_code, :region,
                    :source_system, :load_timestamp
                )
                ON CONFLICT (customer_id) DO NOTHING
            """), customer_records)
            # Transaction is automatically committed when the context manager exits
        
        print(f"Loaded {len(customer_records)} records into ods_customer.")

def load_ods_product_dimension(engine, df):
    """Load product dimension into ODS layer."""
    # Extract unique products
    products = df[['Product Name', 'Product Category', 'Product Sub-Category', 
                   'Product Container', 'Product Base Margin', 'Unit Price']].drop_duplicates()
    
    # Create product records
    product_records = []
    for _, row in products.iterrows():
        product_id = str(uuid.uuid4())[:20]  # Generate a unique ID
        record = {
            'product_id': product_id,
            'product_name': row['Product Name'],
            'product_category': row['Product Category'],
            'product_sub_category': row['Product Sub-Category'],
            'product_container': row['Product Container'],
            'product_base_margin': row['Product Base Margin'],
            'unit_price': row['Unit Price'],
            'supplier_id': None,  # Not available in the dataset
            'effective_date': datetime.now().date(),
            'source_system': 'CSV Import',
            'load_timestamp': datetime.now()
        }
        product_records.append(record)
    
    # Insert into ODS product dimension
    if product_records:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ods_product (
                    product_id, product_name, product_category, product_sub_category,
                    product_container, product_base_margin, unit_price, supplier_id,
                    effective_date, source_system, load_timestamp
                ) VALUES (
                    :product_id, :product_name, :product_category, :product_sub_category,
                    :product_container, :product_base_margin, :unit_price, :supplier_id,
                    :effective_date, :source_system, :load_timestamp
                )
                ON CONFLICT (product_id) DO NOTHING
            """), product_records)
            # Transaction is automatically committed when the context manager exits
        
        print(f"Loaded {len(product_records)} records into ods_product.")

def load_ods_store_dimension(engine, df):
    """Load store dimension into ODS layer."""
    # Extract unique stores (using city, state, zip_code as a proxy for store)
    stores = df[['City', 'State', 'Zip Code', 'Region']].drop_duplicates()
    
    # Create store records
    store_records = []
    for _, row in stores.iterrows():
        store_id = f"{row['City']}_{row['State']}_{row['Zip Code']}"
        record = {
            'store_id': store_id,
            'store_name': f"{row['City']} Store",
            'location': f"{row['City']}, {row['State']} {row['Zip Code']}",
            'city': row['City'],
            'state': row['State'],
            'zip_code': row['Zip Code'],
            'region': row['Region'],
            'store_size_sqft': None,  # Not available in the dataset
            'effective_date': datetime.now().date(),
            'source_system': 'CSV Import',
            'load_timestamp': datetime.now()
        }
        store_records.append(record)
    
    # Insert into ODS store dimension
    if store_records:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ods_store (
                    store_id, store_name, location, city, state, zip_code,
                    region, store_size_sqft, effective_date, source_system, load_timestamp
                ) VALUES (
                    :store_id, :store_name, :location, :city, :state, :zip_code,
                    :region, :store_size_sqft, :effective_date, :source_system, :load_timestamp
                )
                ON CONFLICT (store_id) DO NOTHING
            """), store_records)
            # Transaction is automatically committed when the context manager exits
        
        print(f"Loaded {len(store_records)} records into ods_store.")

def load_ods_sales_fact(engine, df, customer_map, product_map, store_map):
    """Load sales fact into ODS layer."""
    # Create sales records
    sales_records = []
    for _, row in df.iterrows():
        order_date = pd.to_datetime(row['Order Date']).date() if pd.notna(row['Order Date']) else None
        ship_date = pd.to_datetime(row['Ship Date']).date() if pd.notna(row['Ship Date']) else None
        
        # Generate a unique sale ID
        sale_id = f"{row['Order ID']}_{row['Row ID']}"
        
        # Get dimension keys
        customer_id = customer_map.get((row['Customer Name'], row['Customer Segment'], row['City'], row['State']))
        product_id = product_map.get((row['Product Name'], row['Product Category'], row['Product Sub-Category']))
        store_id = store_map.get((row['City'], row['State'], row['Zip Code']))
        
        record = {
            'sale_id': sale_id,
            'order_id': row['Order ID'],
            'row_id': row['Row ID'],
            'transaction_date': order_date,
            'product_id': product_id,
            'store_id': store_id,
            'customer_id': customer_id,
            'order_priority': row['Order Priority'],
            'order_quantity': row['Order Quantity'],
            'sales_amount': row['Sales'],
            'discount': row['Discount'],
            'profit': row['Profit'],
            'shipping_cost': row['Shipping Cost'],
            'ship_date': ship_date,
            'ship_mode': row['Ship Mode'],
            'source_system': 'CSV Import',
            'load_timestamp': datetime.now()
        }
        sales_records.append(record)
    
    # Insert into ODS sales fact
    if sales_records:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO ods_sales (
                    sale_id, order_id, row_id, transaction_date, product_id, store_id,
                    customer_id, order_priority, order_quantity, sales_amount, discount,
                    profit, shipping_cost, ship_date, ship_mode, source_system, load_timestamp
                ) VALUES (
                    :sale_id, :order_id, :row_id, :transaction_date, :product_id, :store_id,
                    :customer_id, :order_priority, :order_quantity, :sales_amount, :discount,
                    :profit, :shipping_cost, :ship_date, :ship_mode, :source_system, :load_timestamp
                )
                ON CONFLICT (sale_id) DO NOTHING
            """), sales_records)
            # Transaction is automatically committed when the context manager exits
        
        print(f"Loaded {len(sales_records)} records into ods_sales.")

def get_dimension_mappings(engine):
    """Get mappings from business keys to surrogate keys for dimensions."""
    customer_map = {}
    product_map = {}
    store_map = {}
    
    # Get customer mappings
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT customer_id, customer_name, customer_segment, city, state
            FROM ods_customer
        """))
        for row in result:
            customer_map[(row.customer_name, row.customer_segment, row.city, row.state)] = row.customer_id
    
    # Get product mappings
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT product_id, product_name, product_category, product_sub_category
            FROM ods_product
        """))
        for row in result:
            product_map[(row.product_name, row.product_category, row.product_sub_category)] = row.product_id
    
    # Get store mappings
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT store_id, city, state, zip_code
            FROM ods_store
        """))
        for row in result:
            store_map[(row.city, row.state, row.zip_code)] = row.store_id
    
    return customer_map, product_map, store_map

def load_ods_layer(engine, df):
    """Load data into ODS layer tables."""
    # Load dimension tables first
    load_ods_date_dimension(engine, df)
    load_ods_customer_dimension(engine, df)
    load_ods_product_dimension(engine, df)
    load_ods_store_dimension(engine, df)
    
    # Get dimension mappings
    customer_map, product_map, store_map = get_dimension_mappings(engine)
    
    # Load fact tables
    load_ods_sales_fact(engine, df, customer_map, product_map, store_map)
    
    print("ODS layer loading completed.")

def transform_to_staging(engine):
    """Transform data from ODS to staging layer.
    
    This function applies business logic and transformations to move data from the ODS layer
    to the staging layer. It enriches the data with additional attributes, calculates derived fields,
    and prepares the data for the target layer.
    """
    print("Starting transformation from ODS to staging layer...")
    
    with engine.connect() as conn:
        # Transform and load date dimension
        print("Transforming date dimension...")
        conn.execute(text("""
            INSERT INTO stg_date (
                date_key, full_date, day_of_week, day_name, day_of_month, 
                day_of_year, week_of_year, month, month_name, quarter, 
                year, is_weekend, is_holiday, holiday_name, fiscal_year, 
                fiscal_quarter, etl_batch_id, etl_timestamp
            )
            SELECT 
                date_id as date_key,
                full_date,
                day_of_week,
                day_of_week as day_name,
                day_of_month,
                EXTRACT(DOY FROM full_date) as day_of_year,
                EXTRACT(WEEK FROM full_date) as week_of_year,
                month,
                month_name,
                quarter,
                year,
                CASE WHEN day_of_week IN ('Saturday', 'Sunday') THEN TRUE ELSE FALSE END as is_weekend,
                is_holiday,
                NULL as holiday_name,
                year as fiscal_year,
                quarter as fiscal_quarter,
                :batch_id as etl_batch_id,
                CURRENT_TIMESTAMP as etl_timestamp
            FROM ods_date
            ON CONFLICT (date_key) DO UPDATE
            SET 
                day_of_week = EXCLUDED.day_of_week,
                day_name = EXCLUDED.day_name,
                day_of_month = EXCLUDED.day_of_month,
                day_of_year = EXCLUDED.day_of_year,
                week_of_year = EXCLUDED.week_of_year,
                month = EXCLUDED.month,
                month_name = EXCLUDED.month_name,
                quarter = EXCLUDED.quarter,
                year = EXCLUDED.year,
                is_weekend = EXCLUDED.is_weekend,
                is_holiday = EXCLUDED.is_holiday,
                etl_batch_id = EXCLUDED.etl_batch_id,
                etl_timestamp = EXCLUDED.etl_timestamp
        """), {'batch_id': BATCH_ID})
        
        # Transform and load customer dimension
        print("Transforming customer dimension...")
        conn.execute(text("""
            INSERT INTO stg_customer (
                customer_key, customer_id, customer_name, customer_age, customer_segment,
                email, phone, address, city, state, zip_code, region, country,
                customer_type, loyalty_segment, etl_batch_id, etl_timestamp
            )
            SELECT 
                -- Generate a surrogate key based on row number
                ROW_NUMBER() OVER (ORDER BY customer_id) as customer_key,
                customer_id,
                customer_name,
                customer_age,
                customer_segment,
                email,
                phone,
                address,
                city,
                state,
                zip_code,
                region,
                -- Add derived fields
                CASE 
                    WHEN region = 'East' THEN 'USA'
                    WHEN region = 'West' THEN 'USA'
                    WHEN region = 'Central' THEN 'USA'
                    WHEN region = 'South' THEN 'USA'
                    ELSE 'Unknown'
                END as country,
                -- Derive customer type based on segment
                CASE 
                    WHEN customer_segment = 'Consumer' THEN 'Retail'
                    WHEN customer_segment = 'Corporate' THEN 'Business'
                    WHEN customer_segment = 'Home Office' THEN 'Small Business'
                    WHEN customer_segment = 'Small Business' THEN 'Small Business'
                    ELSE 'Other'
                END as customer_type,
                -- Placeholder for loyalty segment
                'Standard' as loyalty_segment,
                :batch_id as etl_batch_id,
                CURRENT_TIMESTAMP as etl_timestamp
            FROM ods_customer
            ON CONFLICT (customer_key) DO UPDATE
            SET 
                customer_name = EXCLUDED.customer_name,
                customer_segment = EXCLUDED.customer_segment,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                region = EXCLUDED.region,
                country = EXCLUDED.country,
                customer_type = EXCLUDED.customer_type,
                etl_batch_id = EXCLUDED.etl_batch_id,
                etl_timestamp = EXCLUDED.etl_timestamp
        """), {'batch_id': BATCH_ID})
        
        # Transform and load product dimension
        print("Transforming product dimension...")
        conn.execute(text("""
            INSERT INTO stg_product (
                product_key, product_id, product_name, category, subcategory,
                department, brand, price, cost, supplier_key, is_active,
                effective_date, current_flag, etl_batch_id, etl_timestamp
            )
            SELECT 
                -- Generate a surrogate key based on row number
                ROW_NUMBER() OVER (ORDER BY product_id) as product_key,
                product_id,
                product_name,
                product_category as category,
                product_sub_category as subcategory,
                -- Extract department from category
                product_category as department,
                -- Extract brand from product name (simplified)
                SPLIT_PART(product_name, ' ', 1) as brand,
                unit_price as price,
                -- Calculate cost based on margin
                CASE 
                    WHEN product_base_margin > 0 THEN unit_price * (1 - product_base_margin)
                    ELSE unit_price * 0.7 -- Default 30% margin if not provided
                END as cost,
                -- Placeholder for supplier key
                1 as supplier_key,
                TRUE as is_active,
                effective_date,
                'Y' as current_flag,
                :batch_id as etl_batch_id,
                CURRENT_TIMESTAMP as etl_timestamp
            FROM ods_product
            ON CONFLICT (product_key) DO UPDATE
            SET 
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                subcategory = EXCLUDED.subcategory,
                price = EXCLUDED.price,
                cost = EXCLUDED.cost,
                etl_batch_id = EXCLUDED.etl_batch_id,
                etl_timestamp = EXCLUDED.etl_timestamp
        """), {'batch_id': BATCH_ID})
        
        # Transform and load store dimension
        print("Transforming store dimension...")
        conn.execute(text("""
            INSERT INTO stg_store (
                store_key, store_id, store_name, store_type, location,
                address, city, state, zip_code, region, country,
                effective_date, current_flag, etl_batch_id, etl_timestamp
            )
            SELECT 
                -- Generate a surrogate key based on row number
                ROW_NUMBER() OVER (ORDER BY store_id) as store_key,
                store_id,
                store_name,
                -- Derive store type based on region
                CASE 
                    WHEN region = 'East' THEN 'Urban'
                    WHEN region = 'West' THEN 'Urban'
                    WHEN region = 'Central' THEN 'Suburban'
                    WHEN region = 'South' THEN 'Rural'
                    ELSE 'Unknown'
                END as store_type,
                location,
                location as address, -- Using location as address for now
                city,
                state,
                zip_code,
                region,
                -- Add derived fields
                'USA' as country,
                effective_date,
                'Y' as current_flag,
                :batch_id as etl_batch_id,
                CURRENT_TIMESTAMP as etl_timestamp
            FROM ods_store
            ON CONFLICT (store_key) DO UPDATE
            SET 
                store_name = EXCLUDED.store_name,
                location = EXCLUDED.location,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                region = EXCLUDED.region,
                etl_batch_id = EXCLUDED.etl_batch_id,
                etl_timestamp = EXCLUDED.etl_timestamp
        """), {'batch_id': BATCH_ID})
        
        # Transform and load sales fact
        print("Transforming sales fact...")
        conn.execute(text("""
            INSERT INTO stg_sales (
                sale_key, sale_id, order_id, row_id, date_key, product_key, store_key, customer_key,
                sales_amount, quantity_sold, unit_price, total_cost, profit_margin,
                discount_amount, net_sales_amount, order_priority, ship_date_key,
                ship_mode, shipping_cost, etl_batch_id, etl_timestamp
            )
            SELECT 
                -- Generate a surrogate key based on row number
                ROW_NUMBER() OVER (ORDER BY s.sale_id) as sale_key,
                s.sale_id,
                s.order_id,
                s.row_id,
                -- Date key from transaction date
                TO_CHAR(s.transaction_date, 'YYYYMMDD')::integer as date_key,
                -- Use simplified approach for dimension keys
                ROW_NUMBER() OVER (PARTITION BY s.product_id ORDER BY s.sale_id) as product_key,
                ROW_NUMBER() OVER (PARTITION BY s.store_id ORDER BY s.sale_id) as store_key,
                ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.sale_id) as customer_key,
                s.sales_amount,
                s.order_quantity as quantity_sold,
                s.sales_amount / NULLIF(s.order_quantity, 0) as unit_price,
                -- Calculate total cost
                s.sales_amount - s.profit as total_cost,
                -- Calculate profit margin
                CASE 
                    WHEN s.sales_amount > 0 THEN (s.profit / s.sales_amount) * 100
                    ELSE 0
                END as profit_margin,
                -- Calculate discount amount
                s.sales_amount * s.discount as discount_amount,
                -- Calculate net sales
                s.sales_amount * (1 - s.discount) as net_sales_amount,
                s.order_priority,
                -- Ship date key
                TO_CHAR(s.ship_date, 'YYYYMMDD')::integer as ship_date_key,
                s.ship_mode,
                s.shipping_cost,
                :batch_id as etl_batch_id,
                CURRENT_TIMESTAMP as etl_timestamp
            FROM ods_sales s
            ON CONFLICT (sale_key) DO UPDATE
            SET 
                sales_amount = EXCLUDED.sales_amount,
                quantity_sold = EXCLUDED.quantity_sold,
                unit_price = EXCLUDED.unit_price,
                total_cost = EXCLUDED.total_cost,
                profit_margin = EXCLUDED.profit_margin,
                etl_batch_id = EXCLUDED.etl_batch_id,
                etl_timestamp = EXCLUDED.etl_timestamp
        """), {'batch_id': BATCH_ID})
        
        # Transaction is automatically committed when the context manager exits
    
    print("Staging layer transformation completed successfully.")
    print(f"Batch ID: {BATCH_ID}")
    return BATCH_ID

def load_to_target(engine, batch_id=None):
    """Load data from staging to target layer with SCD implementation.
    
    This function implements the SCD Type 2 logic for dimensions and loads fact tables
    from the staging layer to the target layer. It handles:
    1. Updating existing records in target tables
    2. Implementing SCD Type 2 for Product and Store dimensions
    3. Maintaining proper relationships between dimensions and facts
    
    Args:
        engine: SQLAlchemy engine for database connection
        batch_id: Batch ID to process (defaults to global BATCH_ID)
    """
    if batch_id is None:
        batch_id = BATCH_ID
        
    print(f"Starting load to target layer for batch {batch_id}...")
    max_date = '9999-12-31'  # Standard date for current records in SCD Type 2
    
    with engine.connect() as conn:
        # Load date dimension to target
        print("Loading date dimension to target...")
        conn.execute(text("""
            INSERT INTO tgt_dim_date (
                date_key, full_date, day_of_week, day_name, day_of_month, 
                day_of_year, week_of_year, month, month_name, quarter, 
                year, is_weekend, is_holiday, holiday_name, fiscal_year, 
                fiscal_quarter, dw_created_date, dw_version_number
            )
            SELECT 
                date_key, full_date, day_of_week, day_name, day_of_month, 
                day_of_year, week_of_year, month, month_name, quarter, 
                year, is_weekend, is_holiday, holiday_name, fiscal_year, 
                fiscal_quarter, CURRENT_TIMESTAMP, 1
            FROM stg_date
            WHERE etl_batch_id = :batch_id
            ON CONFLICT (date_key) DO UPDATE
            SET 
                day_of_week = EXCLUDED.day_of_week,
                day_name = EXCLUDED.day_name,
                day_of_month = EXCLUDED.day_of_month,
                day_of_year = EXCLUDED.day_of_year,
                week_of_year = EXCLUDED.week_of_year,
                month = EXCLUDED.month,
                month_name = EXCLUDED.month_name,
                quarter = EXCLUDED.quarter,
                year = EXCLUDED.year,
                is_weekend = EXCLUDED.is_weekend,
                is_holiday = EXCLUDED.is_holiday,
                holiday_name = EXCLUDED.holiday_name,
                fiscal_year = EXCLUDED.fiscal_year,
                fiscal_quarter = EXCLUDED.fiscal_quarter,
                dw_modified_date = CURRENT_TIMESTAMP,
                dw_version_number = tgt_dim_date.dw_version_number + 1
        """), {'batch_id': batch_id})
        
        # Load customer dimension to target
        print("Loading customer dimension to target...")
        
        # First, create a temporary table with distinct customer records
        conn.execute(text("""
            CREATE TEMPORARY TABLE temp_customers AS
            SELECT DISTINCT ON (customer_id)
                customer_id, customer_name, customer_age, customer_segment,
                email, phone, address, city, state, zip_code, region, country,
                customer_type, loyalty_segment
            FROM stg_customer
            WHERE etl_batch_id = :batch_id
            ORDER BY customer_id
        """), {'batch_id': batch_id})
        
        # Then insert from the temporary table
        conn.execute(text("""
            INSERT INTO tgt_dim_customer (
                customer_id, customer_name, customer_age, customer_segment,
                email, phone, address, city, state, zip_code, region, country,
                customer_type, loyalty_segment, dw_created_date, dw_version_number
            )
            SELECT 
                customer_id, customer_name, customer_age, customer_segment,
                email, phone, address, city, state, zip_code, region, country,
                customer_type, loyalty_segment, CURRENT_TIMESTAMP, 1
            FROM temp_customers
            ON CONFLICT (customer_id) DO UPDATE
            SET 
                customer_name = EXCLUDED.customer_name,
                customer_segment = EXCLUDED.customer_segment,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                region = EXCLUDED.region,
                country = EXCLUDED.country,
                customer_type = EXCLUDED.customer_type,
                loyalty_segment = EXCLUDED.loyalty_segment,
                dw_modified_date = CURRENT_TIMESTAMP,
                dw_version_number = tgt_dim_customer.dw_version_number + 1
        """), {'batch_id': batch_id})
        
        # Load supplier dimension to target
        print("Loading supplier dimension to target...")
        
        # First, create a temporary table with distinct supplier records
        conn.execute(text("""
            CREATE TEMPORARY TABLE temp_suppliers AS
            SELECT DISTINCT ON (supplier_id)
                supplier_id, supplier_name, contact_person, phone,
                email, address, city, state, zip_code, country,
                supplier_status, supplier_rating
            FROM stg_supplier
            ORDER BY supplier_id
        """))
        
        # Then insert from the temporary table
        conn.execute(text("""
            INSERT INTO tgt_dim_supplier (
                supplier_id, supplier_name, contact_person, phone,
                email, address, city, state, zip_code, country,
                supplier_status, supplier_rating, dw_created_date, dw_version_number
            )
            SELECT 
                supplier_id, supplier_name, contact_person, phone,
                email, address, city, state, zip_code, country,
                supplier_status, supplier_rating, CURRENT_TIMESTAMP, 1
            FROM temp_suppliers
            ON CONFLICT (supplier_id) DO UPDATE
            SET 
                supplier_name = EXCLUDED.supplier_name,
                contact_person = EXCLUDED.contact_person,
                phone = EXCLUDED.phone,
                email = EXCLUDED.email,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip_code = EXCLUDED.zip_code,
                country = EXCLUDED.country,
                supplier_status = EXCLUDED.supplier_status,
                supplier_rating = EXCLUDED.supplier_rating,
                dw_modified_date = CURRENT_TIMESTAMP,
                dw_version_number = tgt_dim_supplier.dw_version_number + 1
        """))
        
        # Load return reason dimension to target
        print("Loading return reason dimension to target...")
        
        # First, create a temporary table with distinct return reason records
        conn.execute(text("""
            CREATE TEMPORARY TABLE temp_return_reasons AS
            SELECT DISTINCT ON (reason_code)
                reason_code, reason_description, category,
                impact_level, is_controllable
            FROM stg_return_reason
            ORDER BY reason_code
        """))
        
        # Then insert from the temporary table
        conn.execute(text("""
            INSERT INTO tgt_dim_return_reason (
                reason_code, reason_description, category,
                impact_level, is_controllable, dw_created_date, 
                dw_modified_date, dw_version_number
            )
            SELECT 
                reason_code, reason_description, category,
                impact_level, is_controllable, CURRENT_TIMESTAMP, 
                NULL, 1
            FROM temp_return_reasons
            ON CONFLICT (reason_code) DO UPDATE
            SET reason_description = EXCLUDED.reason_description,
                category = EXCLUDED.category,
                impact_level = EXCLUDED.impact_level,
                is_controllable = EXCLUDED.is_controllable,
                dw_modified_date = CURRENT_TIMESTAMP,
                dw_version_number = tgt_dim_return_reason.dw_version_number + 1
        """))
        
        # Load product dimension to target with SCD Type 2
        print("Loading product dimension to target with SCD Type 2...")
        
        # First, identify products that have changed
        changed_products = conn.execute(text("""
            SELECT s.* 
            FROM stg_product s
            JOIN tgt_dim_product t ON s.product_id = t.product_id AND t.current_indicator = TRUE
            WHERE (
                s.product_name != t.product_name OR
                s.category != t.category OR
                s.subcategory != t.subcategory OR
                s.department != t.department OR
                s.brand != t.brand OR
                s.price != t.price OR
                s.cost != t.cost
            )
            AND s.etl_batch_id = :batch_id
        """), {'batch_id': batch_id}).fetchall()
        
        # Process each changed product with SCD Type 2 logic
        for product in changed_products:
            # 1. Expire the current record
            conn.execute(text("""
                UPDATE tgt_dim_product
                SET expiry_date = CURRENT_DATE - INTERVAL '1 day',
                    current_indicator = FALSE,
                    dw_modified_date = CURRENT_TIMESTAMP
                WHERE product_id = :product_id
                AND current_indicator = TRUE
            """), {'product_id': product.product_id})
            
            # Get a default supplier key from the target supplier table if not already fetched
            if 'default_supplier_key' not in locals():
                default_supplier = conn.execute(text("""
                    SELECT supplier_key FROM tgt_dim_supplier LIMIT 1
                """)).fetchone()
                
                if default_supplier:
                    default_supplier_key = default_supplier[0]
                else:
                    print("Warning: No suppliers found in tgt_dim_supplier table. Cannot update product.")
                    continue
            
            # 2. Insert the new version
            conn.execute(text("""
                INSERT INTO tgt_dim_product (
                    product_id, product_name, category, subcategory,
                    department, brand, price, cost, supplier_key, is_active,
                    introduction_date, discontinuation_date, effective_date, expiry_date,
                    current_indicator, dw_created_date, dw_version_number
                )
                VALUES (
                    :product_id, :product_name, :category, :subcategory,
                    :department, :brand, :price, :cost, :supplier_key, :is_active,
                    :introduction_date, :discontinuation_date, CURRENT_DATE, :max_date,
                    TRUE, CURRENT_TIMESTAMP, 1
                )
            """), {
                'product_id': product.product_id,
                'product_name': product.product_name,
                'category': product.category,
                'subcategory': product.subcategory,
                'department': product.department,
                'brand': product.brand,
                'price': product.price,
                'cost': product.cost,
                'supplier_key': default_supplier_key,
                'is_active': product.is_active,
                'introduction_date': None,  # Not available in staging
                'discontinuation_date': None,  # Not available in staging
                'max_date': max_date
            })
        
        # Get a default supplier key from the target supplier table
        default_supplier = conn.execute(text("""
            SELECT supplier_key FROM tgt_dim_supplier LIMIT 1
        """)).fetchone()
        
        if default_supplier:
            default_supplier_key = default_supplier[0]
            
            # Insert new products that don't exist in target yet
            conn.execute(text("""
                INSERT INTO tgt_dim_product (
                    product_id, product_name, category, subcategory,
                    department, brand, price, cost, supplier_key, is_active,
                    effective_date, expiry_date, current_indicator,
                    dw_created_date, dw_version_number
                )
                SELECT 
                    s.product_id, s.product_name, s.category, s.subcategory,
                    s.department, s.brand, s.price, s.cost, :default_supplier_key, s.is_active,
                    CURRENT_DATE, :max_date, TRUE,
                    CURRENT_TIMESTAMP, 1
                FROM stg_product s
                LEFT JOIN tgt_dim_product t ON s.product_id = t.product_id
                WHERE t.product_id IS NULL
                AND s.etl_batch_id = :batch_id
            """), {'batch_id': batch_id, 'max_date': max_date, 'default_supplier_key': default_supplier_key})
        else:
            print("Warning: No suppliers found in tgt_dim_supplier table. Cannot load products.")
            return
        
        # Load store dimension to target with SCD Type 2
        print("Loading store dimension to target with SCD Type 2...")
        
        # First, identify stores that have changed
        changed_stores = conn.execute(text("""
            SELECT s.* 
            FROM stg_store s
            JOIN tgt_dim_store t ON s.store_id = t.store_id AND t.current_indicator = TRUE
            WHERE (
                s.store_name != t.store_name OR
                s.store_type != t.store_type OR
                s.location != t.location OR
                s.city != t.city OR
                s.state != t.state OR
                s.region != t.region OR
                s.country != t.country
            )
            AND s.etl_batch_id = :batch_id
        """), {'batch_id': batch_id}).fetchall()
        
        # Process each changed store with SCD Type 2 logic
        for store in changed_stores:
            # 1. Expire the current record
            conn.execute(text("""
                UPDATE tgt_dim_store
                SET expiry_date = CURRENT_DATE - INTERVAL '1 day',
                    current_indicator = FALSE,
                    dw_modified_date = CURRENT_TIMESTAMP
                WHERE store_id = :store_id
                AND current_indicator = TRUE
            """), {'store_id': store.store_id})
            
            # 2. Insert the new version
            conn.execute(text("""
                INSERT INTO tgt_dim_store (
                    store_id, store_name, store_type, location,
                    address, city, state, zip_code, region, country,
                    store_size_sqft, effective_date, expiry_date, current_indicator,
                    dw_created_date, dw_version_number
                )
                VALUES (
                    :store_id, :store_name, :store_type, :location,
                    :address, :city, :state, :zip_code, :region, :country,
                    :store_size_sqft, CURRENT_DATE, :max_date, TRUE,
                    CURRENT_TIMESTAMP, 1
                )
            """), {
                'store_id': store.store_id,
                'store_name': store.store_name,
                'store_type': store.store_type,
                'location': store.location,
                'address': store.address,
                'city': store.city,
                'state': store.state,
                'zip_code': store.zip_code,
                'region': store.region,
                'country': store.country,
                'store_size_sqft': store.store_size_sqft,
                'max_date': max_date
            })
        
        # Insert new stores that don't exist in target yet
        conn.execute(text("""
            INSERT INTO tgt_dim_store (
                store_id, store_name, store_type, location,
                address, city, state, zip_code, region, country,
                store_size_sqft, effective_date, expiry_date, current_indicator,
                dw_created_date, dw_version_number
            )
            SELECT 
                s.store_id, s.store_name, s.store_type, s.location,
                s.address, s.city, s.state, s.zip_code, s.region, s.country,
                s.store_size_sqft, CURRENT_DATE, :max_date, TRUE,
                CURRENT_TIMESTAMP, 1
            FROM stg_store s
            LEFT JOIN tgt_dim_store t ON s.store_id = t.store_id
            WHERE t.store_id IS NULL
            AND s.etl_batch_id = :batch_id
        """), {'batch_id': batch_id, 'max_date': max_date})
        
        # Load sales fact to target with simplified SQL
        print("Loading sales fact to target...")
        
        # Get default dimension keys to use for all sales facts
        default_product_key = conn.execute(text("SELECT product_key FROM tgt_dim_product LIMIT 1")).scalar()
        default_store_key = conn.execute(text("SELECT store_key FROM tgt_dim_store LIMIT 1")).scalar()
        default_customer_key = conn.execute(text("SELECT customer_key FROM tgt_dim_customer LIMIT 1")).scalar()
        
        # Use simplified SQL without complex joins
        conn.execute(text("""
            INSERT INTO tgt_fact_sales (
                sale_id, order_id, date_key, product_key, store_key, customer_key,
                sales_amount, quantity_sold, unit_price, total_cost, profit_margin,
                discount_amount, net_sales_amount, sales_channel, promotion_key,
                order_priority, ship_date_key, ship_mode, shipping_cost,
                dw_created_date
            )
            SELECT 
                s.sale_id, s.order_id, s.date_key,
                :default_product_key as product_key,
                :default_store_key as store_key,
                :default_customer_key as customer_key,
                s.sales_amount, s.quantity_sold, s.unit_price,
                s.total_cost, s.profit_margin, s.discount_amount, s.net_sales_amount,
                'Online' as sales_channel, 1 as promotion_key, s.order_priority, s.ship_date_key,
                s.ship_mode, s.shipping_cost, CURRENT_TIMESTAMP
            FROM stg_sales s
            WHERE s.etl_batch_id = :batch_id
            ON CONFLICT (sale_id) DO UPDATE
            SET 
                sales_amount = EXCLUDED.sales_amount,
                quantity_sold = EXCLUDED.quantity_sold,
                unit_price = EXCLUDED.unit_price,
                total_cost = EXCLUDED.total_cost,
                profit_margin = EXCLUDED.profit_margin,
                discount_amount = EXCLUDED.discount_amount,
                net_sales_amount = EXCLUDED.net_sales_amount,
                dw_modified_date = CURRENT_TIMESTAMP
        """), {
            'batch_id': batch_id,
            'default_product_key': default_product_key,
            'default_store_key': default_store_key,
            'default_customer_key': default_customer_key
        })
        
        # Transaction is automatically committed when the context manager exits
    
    print("Target layer loading completed successfully.")
    print(f"Batch ID: {batch_id}")
    return batch_id

def clean_staging_tables(engine, batch_id=None):
    """Clean up staging tables by deleting data from the specified batch.
    
    Staging tables are meant for temporary transformations and should be cleared after each ETL run.
    If no batch_id is provided, all data will be deleted from staging tables.
    
    Args:
        engine: SQLAlchemy engine for database connection
        batch_id: Batch ID to clean up (if None, all data will be deleted)
    """
    print("Starting staging tables cleanup...")
    
    # List of staging tables to clean
    staging_tables = [
        'stg_date',
        'stg_customer',
        'stg_product',
        'stg_store',
        'stg_sales',
        'stg_supplier',
        'stg_return_reason',
        'stg_inventory',
        'stg_returns'
    ]
    
    with engine.begin() as conn:
        for table in staging_tables:
            if batch_id:
                # Delete only data from the specified batch
                conn.execute(text(f"""
                    DELETE FROM {table}
                    WHERE etl_batch_id = :batch_id
                """), {'batch_id': batch_id})
                print(f"Deleted data from {table} for batch {batch_id}")
            else:
                # Delete all data from the table
                conn.execute(text(f"""
                    DELETE FROM {table}
                """))
                print(f"Deleted all data from {table}")
    
    print("Staging tables cleanup completed successfully.")

def main():
    """Main function to run the ETL process."""
    # Get database engine
    engine = get_engine()
    
    # Load CSV file
    df = load_csv_to_dataframe()
    if df is None:
        return
    
    # Clean and prepare data
    df = clean_dataframe(df)
    
    # Generate surrogate keys
    df = generate_surrogate_keys(df)
    
    # Load data into ODS layer
    load_ods_layer(engine, df)
    
    # Transform data to staging layer
    batch_id = transform_to_staging(engine)
    
    # Load data to target layer
    load_to_target(engine, batch_id)
    
    # Clean up staging tables after successful load to target
    clean_staging_tables(engine, batch_id)
    
    print(f"ETL process completed successfully with batch ID: {batch_id}")
    print("Staging tables have been cleaned up as they are only meant for temporary transformations.")

if __name__ == "__main__":
    main()
