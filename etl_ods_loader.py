#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Data Loader
This script loads data from the CSV file into the ODS tables with proper ID generation using hashing.
"""

import os
import pandas as pd
import numpy as np
import hashlib
import random
from datetime import datetime, timedelta
from sqlalchemy.sql import text
import uuid

# Import configuration and table definitions
from config import CSV_FILE, get_connection_string
from etl_ods_tables import get_engine, metadata, create_ods_tables
# ID Generation Functions
def generate_date_id(date_obj):
    """Generate a date ID in the format YYYYMMDD."""
    return int(date_obj.strftime('%Y%m%d'))

def generate_customer_id(customer_name):
    """Generate a deterministic customer ID based on customer name only.
    
    We hash only the customer name to create a unique ID.
    This allows the same customer to appear in multiple locations.
    Ensures the ID is no longer than 20 characters to match the database column definition.
    """
    # Create a consistent string from just the customer name
    key_string = customer_name.strip()
    # Generate a deterministic ID using MD5 hash, limiting to 14 characters to fit within 20 chars total
    return f"CUST_{hashlib.md5(key_string.encode()).hexdigest()[:14]}"

def generate_product_id(product_name):
    """Generate a deterministic product ID based on product name only.
    
    As specified, we hash ONLY the product name.
    Ensures the ID is no longer than 20 characters to match the database column definition.
    """
    # Hash only the product name as specified, limiting to 14 characters to fit within 20 chars total
    return f"PROD_{hashlib.md5(product_name.encode()).hexdigest()[:14]}"

def generate_supplier_id(supplier_name):
    """Generate a deterministic supplier ID based on supplier name.
    
    Ensures the ID is no longer than 20 characters to match the database column definition.
    """
    # Hash the supplier name, limiting to 14 characters to fit within 20 chars total
    return f"SUPP_{hashlib.md5(supplier_name.encode()).hexdigest()[:14]}"

def generate_return_reason_id(reason_code):
    """Generate a deterministic return reason ID based on reason code.
    
    Ensures the ID is no longer than 20 characters to match the database column definition.
    """
    # Hash the reason code, limiting to 14 characters to fit within 20 chars total
    return f"REAS_{hashlib.md5(reason_code.encode()).hexdigest()[:14]}"

def generate_return_id(sale_id, return_date):
    """Generate a deterministic return ID based on sale ID and return date.
    
    Ensures the ID is no longer than 20 characters to match the database column definition.
    """
    # Create a key string from sale ID and return date
    key_string = f"{sale_id}_{return_date}"
    # Hash the key string, limiting to 14 characters to fit within 20 chars total
    return f"RET_{hashlib.md5(key_string.encode()).hexdigest()[:14]}"

def generate_inventory_id(product_id, store_id, inventory_date):
    """Generate a deterministic inventory ID based on product ID, store ID, and inventory date.
    
    Ensures the ID is no longer than 20 characters to match the database column definition.
    """
    # Create a key string from product ID, store ID, and inventory date
    key_string = f"{product_id}_{store_id}_{inventory_date}"
    # Hash the key string, limiting to 14 characters to fit within 20 chars total
    return f"INV_{hashlib.md5(key_string.encode()).hexdigest()[:14]}"

def generate_store_id(city, state, zip_code):
    """Generate a deterministic store ID based on store name only.
    
    As specified, we hash ONLY the store name (created from city).
    Ensures the ID is no longer than 20 characters to match the database column definition.
    """
    # Create a store name from city
    store_name = f"{city} Store"
    # Hash only the store name as specified, limiting to 14 characters to fit within 20 chars total
    return f"STORE_{hashlib.md5(store_name.encode()).hexdigest()[:14]}"

def generate_sale_id(order_id, row_id):
    """Generate a deterministic sale ID based on order ID and row ID.
    
    We hash the combination of order ID and row ID to create a unique sale ID.
    Ensures the ID is no longer than 20 characters to match the database column definition.
    """
    key_string = f"{order_id}_{row_id}"
    return f"SALE_{hashlib.md5(key_string.encode()).hexdigest()[:14]}"

# Data Loading Functions
def load_csv_to_dataframe():
    """Load CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(CSV_FILE)
        print(f"Loaded {len(df)} rows from CSV file.")
        return df
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return None

def prepare_dataframe(df):
    """Prepare the DataFrame for loading into ODS tables without transformations.
    
    In the ODS layer, we only map data and load it as is, without transformations.
    """
    # For ODS layer, we keep the data as is, only handling empty values for proper loading
    df = df.replace('', np.nan)
    
    return df

def load_ods_date_dimension(engine, df):
    """Load date dimension into ODS layer without transformations."""
    # Extract unique dates from Order Date and Ship Date columns as they appear in the CSV
    # For ODS layer, we're just mapping the data without transformations
    order_dates = df['Order Date'].dropna().unique()
    ship_dates = df['Ship Date'].dropna().unique()
    all_dates = np.union1d(order_dates, ship_dates)
    
    # Create date dimension records
    date_records = []
    for date in all_dates:
        # Parse the date string as it appears in the CSV
        if isinstance(date, str):
            try:
                # Try to parse the date in the format it appears in the CSV
                date_parts = date.split('/')
                if len(date_parts) == 3:
                    month, day, year = date_parts
                    date_obj = datetime(int(year), int(month), int(day))
                else:
                    # If not in expected format, try pandas to_datetime as fallback
                    date_obj = pd.to_datetime(date)
            except:
                # If parsing fails, use pandas to_datetime as fallback
                date_obj = pd.to_datetime(date)
        else:
            # If already a datetime object, use it directly
            date_obj = date
            
        # Use standard library methods instead of pandas methods
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        
        record = {
            'date_id': generate_date_id(date_obj),
            'full_date': date_obj.date(),
            'day_of_week': day_names[date_obj.weekday()],
            'day_of_month': date_obj.day,
            'month': date_obj.month,
            'month_name': month_names[date_obj.month - 1],
            'quarter': (date_obj.month - 1) // 3 + 1,
            'year': date_obj.year,
            'is_holiday': False,  # Would need a holiday calendar to determine
            'source_system': 'CSV Import',
            'load_timestamp': datetime.now()
        }
        date_records.append(record)
    
    # Insert into ODS date dimension
    if date_records:
        with engine.begin() as conn:  # Using begin() to auto-commit the transaction
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
    """Load customer dimension into ODS layer.
    
    This updated version handles customers with multiple locations by:
    1. Using only customer name for ID generation
    2. Finding the most common location for each customer
    3. Creating one record per customer with their primary location
    """
    # Group by customer name and find the most common location for each customer
    customer_locations = df.groupby('Customer Name').agg({
        'City': lambda x: x.value_counts().index[0],
        'State': lambda x: x.value_counts().index[0],
        'Zip Code': lambda x: x.value_counts().index[0],
        'Region': lambda x: x.value_counts().index[0],
        'Customer Age': 'first',
        'Customer Segment': 'first'
    }).reset_index()
    
    # Create customer records
    customer_records = []
    for _, row in customer_locations.iterrows():
        customer_id = generate_customer_id(row['Customer Name'])
        record = {
            'customer_id': customer_id,
            'customer_name': row['Customer Name'],
            'customer_age': row['Customer Age'],
            'customer_segment': row['Customer Segment'],
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
        with engine.begin() as conn:  # Using begin() to auto-commit the transaction
            conn.execute(text("""
                INSERT INTO ods_customer (
                    customer_id, customer_name, customer_age, customer_segment,
                    city, state, zip_code, region, source_system, load_timestamp
                ) VALUES (
                    :customer_id, :customer_name, :customer_age, :customer_segment,
                    :city, :state, :zip_code, :region, :source_system, :load_timestamp
                )
                ON CONFLICT (customer_id) DO NOTHING
            """), customer_records)
        
        print(f"Loaded {len(customer_records)} records into ods_customer with primary locations.")
        
        # Log information about customers with multiple locations
        customer_location_counts = df.groupby('Customer Name')['City'].nunique()
        multi_location_customers = customer_location_counts[customer_location_counts > 1].count()
        print(f"Note: {multi_location_customers} customers appear in multiple locations in the source data.")
        print(f"Each customer has been assigned their most frequent location.")


def load_ods_supplier_dimension(engine, df):
    """Load supplier dimension into ODS layer.
    
    Since the CSV doesn't contain explicit supplier information,
    we'll create suppliers based on product categories and sub-categories
    to ensure we have enough suppliers.
    """
    # Extract unique product categories and sub-categories
    categories = df['Product Category'].unique()
    subcategories = df['Product Sub-Category'].unique()
    
    # Create supplier records
    supplier_records = []
    supplier_map = {}  # To map categories to supplier IDs
    category_supplier_map = {}  # To map categories to multiple supplier IDs
    
    # Create a main supplier for each product category
    for category in categories:
        supplier_name = f"{category} Main Suppliers Inc."
        supplier_id = generate_supplier_id(supplier_name)
        supplier_map[category] = supplier_id  # For backward compatibility
        
        if category not in category_supplier_map:
            category_supplier_map[category] = []
        category_supplier_map[category].append(supplier_id)
        
        # Create a record for the main supplier
        record = {
            'supplier_id': supplier_id,
            'supplier_name': supplier_name,
            'contact_person': f"Main Contact for {category}",
            'email': f"contact@{category.lower().replace(' ', '')}suppliers.com",
            'phone': f"555-{hash(category) % 10000:04d}",
            'address': f"{hash(category) % 1000} Main St",
            'city': 'Supplier City',
            'state': 'SC',
            'zip_code': f"{10000 + hash(category) % 90000}",
            'contract_start_date': datetime(2020, 1, 1).date(),
            'source_system': 'Generated',
            'load_timestamp': datetime.now()
        }
        supplier_records.append(record)
    
    # Create additional suppliers based on sub-categories
    for subcategory in subcategories:
        # Find the parent category for this subcategory
        parent_category = None
        for _, row in df[['Product Category', 'Product Sub-Category']].drop_duplicates().iterrows():
            if row['Product Sub-Category'] == subcategory:
                parent_category = row['Product Category']
                break
        
        if parent_category is None:
            continue
        
        # Create a supplier for this subcategory
        supplier_name = f"{subcategory} Specialized Suppliers"
        supplier_id = generate_supplier_id(supplier_name)
        
        if parent_category not in category_supplier_map:
            category_supplier_map[parent_category] = []
        category_supplier_map[parent_category].append(supplier_id)
        
        # Create a record for the specialized supplier
        record = {
            'supplier_id': supplier_id,
            'supplier_name': supplier_name,
            'contact_person': f"Specialized Contact for {subcategory}",
            'email': f"contact@{subcategory.lower().replace(' ', '')}suppliers.com",
            'phone': f"555-{hash(subcategory) % 10000:04d}",
            'address': f"{hash(subcategory) % 1000} Specialty Ave",
            'city': 'Supplier City',
            'state': 'SC',
            'zip_code': f"{10000 + hash(subcategory) % 90000}",
            'contract_start_date': datetime(2020, 1, 1).date(),
            'source_system': 'Generated',
            'load_timestamp': datetime.now()
        }
        supplier_records.append(record)
    
    # Insert into ODS supplier dimension
    if supplier_records:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ods_supplier (
                    supplier_id, supplier_name, contact_person, email, phone,
                    address, city, state, zip_code, contract_start_date,
                    source_system, load_timestamp
                ) VALUES (
                    :supplier_id, :supplier_name, :contact_person, :email, :phone,
                    :address, :city, :state, :zip_code, :contract_start_date,
                    :source_system, :load_timestamp
                )
                ON CONFLICT (supplier_id) DO NOTHING
            """), supplier_records)
        
        print(f"Loaded {len(supplier_records)} records into ods_supplier.")
    
    return category_supplier_map

def load_ods_product_dimension(engine, df, category_supplier_map):
    """Load product dimension into ODS layer.
    
    Maps products to suppliers based on their product category and sub-category.
    Each product is assigned to one of the suppliers for its category.
    """
    # Extract unique products
    products = df[[
        'Product Name', 'Product Category', 'Product Sub-Category',
        'Product Container', 'Product Base Margin', 'Unit Price'
    ]].drop_duplicates()
    
    # Create product records
    product_records = []
    
    for _, row in products.iterrows():
        product_id = generate_product_id(row['Product Name'])
        
        # Get the supplier ID for this product's category
        category = row['Product Category']
        supplier_ids = category_supplier_map.get(category, [])
        
        if not supplier_ids:
            # If no suppliers for this category, skip this product
            continue
        
        # Deterministically select a supplier based on the product name
        supplier_index = int(hashlib.md5(row['Product Name'].encode()).hexdigest(), 16) % len(supplier_ids)
        supplier_id = supplier_ids[supplier_index]
        
        record = {
            'product_id': product_id,
            'product_name': row['Product Name'],
            'product_category': row['Product Category'],
            'product_sub_category': row['Product Sub-Category'],
            'product_container': row['Product Container'],
            'product_base_margin': row['Product Base Margin'],
            'unit_price': row['Unit Price'],
            'supplier_id': supplier_id,  # Add the supplier ID
            'source_system': 'CSV Import',
            'load_timestamp': datetime.now()
        }
        product_records.append(record)
    
    # Insert into ODS product dimension
    if product_records:
        with engine.begin() as conn:  # Using begin() to auto-commit the transaction
            conn.execute(text("""
                INSERT INTO ods_product (
                    product_id, product_name, product_category, product_sub_category,
                    product_container, product_base_margin, unit_price, supplier_id,
                    source_system, load_timestamp
                ) VALUES (
                    :product_id, :product_name, :product_category, :product_sub_category,
                    :product_container, :product_base_margin, :unit_price, :supplier_id,
                    :source_system, :load_timestamp
                )
                ON CONFLICT (product_id) DO UPDATE SET
                    supplier_id = EXCLUDED.supplier_id,
                    load_timestamp = EXCLUDED.load_timestamp
            """), product_records)
        
        print(f"Loaded {len(product_records)} records into ods_product.")

def load_ods_store_dimension(engine, df):
    """Load store dimension into ODS layer."""
    # Extract unique stores (using city, state, zip_code as a proxy for store)
    stores = df[['City', 'State', 'Zip Code', 'Region']].drop_duplicates()
    
    # Create store records
    store_records = []
    for _, row in stores.iterrows():
        store_name = f"{row['City']} Store"
        store_id = generate_store_id(row['City'], row['State'], row['Zip Code'])
        record = {
            'store_id': store_id,
            'store_name': store_name,
            'city': row['City'],
            'state': row['State'],
            'zip_code': row['Zip Code'],
            'region': row['Region'],
            'source_system': 'CSV Import',
            'load_timestamp': datetime.now()
        }
        store_records.append(record)
    
    # Insert into ODS store dimension
    if store_records:
        with engine.begin() as conn:  # Using begin() to auto-commit the transaction
            conn.execute(text("""
                INSERT INTO ods_store (
                    store_id, store_name, city, state, zip_code,
                    region, source_system, load_timestamp
                ) VALUES (
                    :store_id, :store_name, :city, :state, :zip_code,
                    :region, :source_system, :load_timestamp
                )
                ON CONFLICT (store_id) DO NOTHING
            """), store_records)
        
        print(f"Loaded {len(store_records)} records into ods_store.")

def get_dimension_mappings(engine):
    """Get mappings from dimension tables for use in fact loading.
    
    This updated version uses only customer name for customer ID generation.
    """
    customer_map = {}
    product_map = {}
    store_map = {}
    reason_map = {}
    
    # Get customer mapping - now using only customer name for ID generation
    with engine.connect() as conn:
        result = conn.execute(text("SELECT customer_id, customer_name FROM ods_customer"))
        for row in result:
            # Generate the customer ID the same way as in load_ods_customer_dimension
            customer_id_key = generate_customer_id(row.customer_name)
            customer_map[customer_id_key] = row.customer_id
    
    # Get product mapping
    with engine.connect() as conn:
        result = conn.execute(text("SELECT product_id, product_name FROM ods_product"))
        for row in result:
            # Generate the product ID the same way as in load_ods_product_dimension
            product_id_key = generate_product_id(row.product_name)
            product_map[product_id_key] = row.product_id
    
    # Get store mapping
    with engine.connect() as conn:
        result = conn.execute(text("SELECT store_id, city, state, zip_code FROM ods_store"))
        for row in result:
            # Generate the store ID the same way as in load_ods_store_dimension
            store_id_key = generate_store_id(row.city, row.state, row.zip_code)
            store_map[store_id_key] = row.store_id
    
    # Get return reason mapping
    with engine.connect() as conn:
        result = conn.execute(text("SELECT reason_code, reason_description FROM ods_return_reason"))
        for row in result:
            # Generate the reason ID the same way as in load_ods_return_reason_dimension
            reason_id_key = generate_return_reason_id(row.reason_code)
            reason_map[reason_id_key] = row.reason_code
    
    return customer_map, product_map, store_map, reason_map

def load_ods_sales_fact(engine, df, customer_map, product_map, store_map):
    """Load sales fact into ODS layer without transformations.
    
    This updated version uses only customer name for customer ID generation.
    It also stores the transaction-specific location data in the fact table.
    """
    # Create sales records
    sales_records = []
    skipped_records = 0
    
    for _, row in df.iterrows():
        # Generate IDs
        sale_id = generate_sale_id(str(row['Order ID']), str(row['Row ID']))
        
        # Get dimension keys
        transaction_date = pd.to_datetime(row['Order Date']).date() if pd.notna(row['Order Date']) else None
        ship_date = pd.to_datetime(row['Ship Date']).date() if pd.notna(row['Ship Date']) else None
        
        # Skip if we don't have valid dates
        if not transaction_date or not ship_date:
            skipped_records += 1
            continue
        
        # Get dimension keys using the maps - now only using customer name for customer ID
        customer_id = customer_map.get(generate_customer_id(row['Customer Name']))
        product_id = product_map.get(generate_product_id(row['Product Name']))
        store_id = store_map.get(generate_store_id(row['City'], row['State'], row['Zip Code']))
        
        # Skip if we don't have valid dimension keys
        if not customer_id or not product_id or not store_id:
            skipped_records += 1
            continue
        
        # Create the record
        record = {
            'sale_id': sale_id,
            'order_id': str(row['Order ID']),
            'row_id': int(row['Row ID']),
            'transaction_date': transaction_date,
            'ship_date': ship_date,
            'customer_id': customer_id,
            'product_id': product_id,
            'store_id': store_id,
            'order_priority': row['Order Priority'],
            'order_quantity': int(row['Order Quantity']),
            'sales_amount': float(row['Sales']),
            'discount': float(row['Discount']),
            'profit': float(row['Profit']),
            'shipping_cost': float(row['Shipping Cost']),
            'product_base_margin': float(row['Product Base Margin']) if pd.notna(row['Product Base Margin']) else None,
            'ship_mode': row['Ship Mode'],
            'transaction_city': row['City'],     # Store transaction-specific location
            'transaction_state': row['State'],   # Store transaction-specific location
            'transaction_zip': row['Zip Code'],  # Store transaction-specific location
            'source_system': 'CSV Import',
            'load_timestamp': datetime.now()
        }
        sales_records.append(record)
    
    # Insert into ODS sales fact
    if sales_records:
        with engine.begin() as conn:  # Using begin() to auto-commit the transaction
            conn.execute(text("""
                INSERT INTO ods_sales (
                    sale_id, order_id, row_id, transaction_date, product_id, store_id, customer_id,
                    order_priority, order_quantity, sales_amount, discount, profit, shipping_cost,
                    ship_date, ship_mode, transaction_city, transaction_state, transaction_zip,
                    product_base_margin, source_system, load_timestamp
                ) VALUES (
                    :sale_id, :order_id, :row_id, :transaction_date, :product_id, :store_id, :customer_id,
                    :order_priority, :order_quantity, :sales_amount, :discount, :profit, :shipping_cost,
                    :ship_date, :ship_mode, :transaction_city, :transaction_state, :transaction_zip,
                    :product_base_margin, :source_system, :load_timestamp
                )
                ON CONFLICT (sale_id) DO NOTHING
            """), sales_records)
        
        print(f"Loaded {len(sales_records)} records into ods_sales.")
        if skipped_records > 0:
            print(f"Skipped {skipped_records} records due to missing dimension keys or invalid dates.")

def load_ods_return_reason_dimension(engine):
    """Load return reason dimension into ODS layer.
    
    Creates synthetic return reason data.
    """
    # Define common return reasons
    reasons = [
        {'code': 'DEFECTIVE', 'description': 'Product is defective or damaged', 'category': 'Quality Issue'},
        {'code': 'WRONG_ITEM', 'description': 'Wrong item was received', 'category': 'Order Error'},
        {'code': 'SIZE_FIT', 'description': 'Size or fit issue', 'category': 'Customer Preference'},
        {'code': 'PERFORMANCE', 'description': 'Product did not perform as expected', 'category': 'Quality Issue'},
        {'code': 'LATE_DELIVERY', 'description': 'Delivery was too late', 'category': 'Shipping Issue'},
        {'code': 'CHANGED_MIND', 'description': 'Customer changed their mind', 'category': 'Customer Preference'},
        {'code': 'BETTER_PRICE', 'description': 'Found better price elsewhere', 'category': 'Price Issue'},
        {'code': 'MISSING_PARTS', 'description': 'Product missing parts', 'category': 'Quality Issue'},
        {'code': 'NOT_AS_DESCRIBED', 'description': 'Product not as described', 'category': 'Product Description'},
        {'code': 'ACCIDENTAL_ORDER', 'description': 'Order was placed accidentally', 'category': 'Order Error'}
    ]
    
    # Create return reason records
    reason_records = []
    for reason in reasons:
        reason_id = generate_return_reason_id(reason['code'])
        record = {
            'reason_code': reason_id,
            'reason_description': reason['description'],
            'category': reason['category'],
            'source_system': 'Generated',
            'load_timestamp': datetime.now()
        }
        reason_records.append(record)
    
    # Insert into ODS return reason dimension
    if reason_records:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ods_return_reason (
                    reason_code, reason_description, category,
                    source_system, load_timestamp
                ) VALUES (
                    :reason_code, :reason_description, :category,
                    :source_system, :load_timestamp
                )
                ON CONFLICT (reason_code) DO NOTHING
            """), reason_records)
        
        print(f"Loaded {len(reason_records)} records into ods_return_reason.")
    
    return {reason['code']: generate_return_reason_id(reason['code']) for reason in reasons}

def load_ods_returns_fact(engine, df, customer_map, product_map, store_map, reason_map):
    """Load returns fact into ODS layer.
    
    Creates synthetic return data for the last 3 months of sales data.
    """
    # Get all sales data since we might not have any in the last 3 months due to synthetic data
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT s.sale_id, s.order_id, s.transaction_date, s.product_id, s.store_id,
                   s.customer_id, s.order_quantity, s.sales_amount
            FROM ods_sales s
            ORDER BY s.transaction_date DESC
            LIMIT 5000
        """))
        sales = [row._mapping for row in result]
    
    # Create return records (approximately 10% of sales are returned)
    return_records = []
    return_count = 0
    reason_codes = list(reason_map.values())
    
    for sale in sales:
        # Only return about 10% of sales
        if random.random() > 0.1:
            continue
        
        # Generate return date (1-14 days after transaction date)
        return_date = sale['transaction_date'] + timedelta(days=random.randint(1, 14))
        
        # Skip if return date is in the future
        if return_date > datetime.now().date():
            continue
        
        # Generate return ID
        return_id = generate_return_id(sale['sale_id'], return_date.isoformat())
        
        # Randomly select a reason code
        reason_code = random.choice(reason_codes)
        
        # Determine if full or partial return
        is_full_return = random.random() > 0.3  # 70% are full returns
        quantity_returned = sale['order_quantity'] if is_full_return else random.randint(1, sale['order_quantity'])
        return_amount = sale['sales_amount'] if is_full_return else (sale['sales_amount'] * quantity_returned / sale['order_quantity'])
        
        # Create return record
        record = {
            'return_id': return_id,
            'return_date': return_date,
            'product_id': sale['product_id'],
            'store_id': sale['store_id'],
            'reason_code': reason_code,
            'return_amount': return_amount,
            'quantity_returned': quantity_returned,
            'original_sale_id': sale['sale_id'],
            'original_sale_date': sale['transaction_date'],
            'return_condition': random.choice(['New', 'Used', 'Damaged']),
            'source_system': 'Generated',
            'load_timestamp': datetime.now()
        }
        return_records.append(record)
        return_count += 1
    
    # Insert into ODS returns fact
    if return_records:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ods_returns (
                    return_id, return_date, product_id, store_id,
                    reason_code, return_amount, quantity_returned, original_sale_id,
                    original_sale_date, return_condition, source_system, load_timestamp
                ) VALUES (
                    :return_id, :return_date, :product_id, :store_id,
                    :reason_code, :return_amount, :quantity_returned, :original_sale_id,
                    :original_sale_date, :return_condition, :source_system, :load_timestamp
                )
                ON CONFLICT (return_id) DO NOTHING
            """), return_records)
        
        print(f"Loaded {len(return_records)} records into ods_returns.")

def load_ods_inventory_fact(engine, df):
    """Load inventory fact into ODS layer.
    
    Creates synthetic daily inventory snapshots for all products and stores.
    """
    # Get product and store IDs
    with engine.begin() as conn:
        product_result = conn.execute(text("SELECT product_id FROM ods_product"))
        product_ids = [row[0] for row in product_result]
        
        store_result = conn.execute(text("SELECT store_id FROM ods_store"))
        store_ids = [row[0] for row in store_result]
    
    # Create inventory records for the last 30 days
    inventory_records = []
    today = datetime.now().date()
    
    # Limit the number of combinations to avoid excessive records
    # Select a subset of products and stores if there are too many
    max_products = min(len(product_ids), 100)  # Limit to 100 products
    max_stores = min(len(store_ids), 50)      # Limit to 50 stores
    
    selected_products = random.sample(product_ids, max_products) if len(product_ids) > max_products else product_ids
    selected_stores = random.sample(store_ids, max_stores) if len(store_ids) > max_stores else store_ids
    
    # Generate daily inventory snapshots
    for day_offset in range(30):
        inventory_date = today - timedelta(days=day_offset)
        
        for product_id in selected_products:
            for store_id in selected_stores:
                # Generate a deterministic but varying inventory level
                base_inventory = int(hashlib.md5(f"{product_id}_{store_id}".encode()).hexdigest(), 16) % 100 + 10
                daily_variation = (int(hashlib.md5(f"{product_id}_{store_id}_{inventory_date}".encode()).hexdigest(), 16) % 20) - 10
                inventory_level = max(0, base_inventory + daily_variation)
                
                # Generate inventory ID
                inventory_id = generate_inventory_id(product_id, store_id, inventory_date.isoformat())
                
                # Create inventory record
                record = {
                    'inventory_id': inventory_id,
                    'product_id': product_id,
                    'store_id': store_id,
                    'inventory_date': inventory_date,
                    'stock_level': inventory_level,
                    'min_stock_level': max(5, inventory_level - random.randint(5, 20)),
                    'max_stock_level': inventory_level + random.randint(20, 50),
                    'reorder_point': random.randint(5, 25),
                    'last_restock_date': inventory_date - timedelta(days=random.randint(1, 14)),
                    'source_system': 'Generated',
                    'load_timestamp': datetime.now()
                }
                inventory_records.append(record)
                
                # Batch insert to avoid memory issues
                if len(inventory_records) >= 1000:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO ods_inventory (
                                inventory_id, product_id, store_id, inventory_date,
                                stock_level, min_stock_level, max_stock_level,
                                reorder_point, last_restock_date, source_system, load_timestamp
                            ) VALUES (
                                :inventory_id, :product_id, :store_id, :inventory_date,
                                :stock_level, :min_stock_level, :max_stock_level,
                                :reorder_point, :last_restock_date, :source_system, :load_timestamp
                            )
                            ON CONFLICT (inventory_id) DO NOTHING
                        """), inventory_records)
                    print(f"Loaded {len(inventory_records)} records into ods_inventory.")
                    inventory_records = []
    
    # Insert any remaining inventory records
    if inventory_records:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO ods_inventory (
                    inventory_id, product_id, store_id, inventory_date,
                    stock_level, min_stock_level, max_stock_level,
                    reorder_point, last_restock_date, source_system, load_timestamp
                ) VALUES (
                    :inventory_id, :product_id, :store_id, :inventory_date,
                    :stock_level, :min_stock_level, :max_stock_level,
                    :reorder_point, :last_restock_date, :source_system, :load_timestamp
                )
                ON CONFLICT (inventory_id) DO NOTHING
            """), inventory_records)
        print(f"Loaded {len(inventory_records)} records into ods_inventory.")

def load_ods_layer(engine, df):
    """Load data into ODS layer tables."""
    # Load dimension tables first
    load_ods_date_dimension(engine, df)
    load_ods_customer_dimension(engine, df)
    
    # Load supplier dimension before product dimension
    supplier_map = load_ods_supplier_dimension(engine, df)
    
    # Load product dimension with supplier mapping
    load_ods_product_dimension(engine, df, supplier_map)
    load_ods_store_dimension(engine, df)
    
    # Load return reason dimension
    reason_map = load_ods_return_reason_dimension(engine)
    
    # Get dimension mappings
    customer_map, product_map, store_map, reason_map = get_dimension_mappings(engine)
    
    # Load fact tables
    load_ods_sales_fact(engine, df, customer_map, product_map, store_map)
    
    # Load returns and inventory after sales
    load_ods_returns_fact(engine, df, customer_map, product_map, store_map, reason_map)
    load_ods_inventory_fact(engine, df)
    
    print("ODS layer loading completed.")

def verify_data_loading(engine):
    """Verify that data was loaded correctly and relationships are established."""
    with engine.begin() as conn:  # Using begin() for consistent transaction management
        # Check count of records in each table
        tables = ['ods_date', 'ods_customer', 'ods_product', 'ods_store', 'ods_supplier', 
                 'ods_return_reason', 'ods_sales', 'ods_returns', 'ods_inventory']
        for table in tables:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"Table {table} has {result} records.")
        
        # Verify foreign key relationships
        print("\nVerifying foreign key relationships:")
        
        # Check if all product_ids in sales exist in product dimension
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_sales s
            LEFT JOIN ods_product p ON s.product_id = p.product_id
            WHERE p.product_id IS NULL
        """)).scalar()
        print(f"Sales records with invalid product_id: {result}")
        
        # Check if all store_ids in sales exist in store dimension
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_sales s
            LEFT JOIN ods_store st ON s.store_id = st.store_id
            WHERE st.store_id IS NULL
        """)).scalar()
        print(f"Sales records with invalid store_id: {result}")
        
        # Check if all customer_ids in sales exist in customer dimension
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_sales s
            LEFT JOIN ods_customer c ON s.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """)).scalar()
        print(f"Sales records with invalid customer_id: {result}")
        
        # Check if all transaction_dates in sales exist in date dimension
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_sales s
            LEFT JOIN ods_date d ON EXTRACT(YEAR FROM s.transaction_date) * 10000 + 
                                  EXTRACT(MONTH FROM s.transaction_date) * 100 + 
                                  EXTRACT(DAY FROM s.transaction_date) = d.date_id
            WHERE d.date_id IS NULL AND s.transaction_date IS NOT NULL
        """)).scalar()
        print(f"Sales records with invalid transaction_date: {result}")
        
        # Check if all supplier_ids in products exist in supplier dimension
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_product p
            LEFT JOIN ods_supplier s ON p.supplier_id = s.supplier_id
            WHERE p.supplier_id IS NOT NULL AND s.supplier_id IS NULL
        """)).scalar()
        print(f"Product records with invalid supplier_id: {result}")
        
        # Check how many products have supplier_id populated
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_product
            WHERE supplier_id IS NOT NULL
        """)).scalar()
        total = conn.execute(text("SELECT COUNT(*) FROM ods_product")).scalar()
        print(f"Products with supplier_id: {result} out of {total} ({result/total*100:.1f}%)")
        
        # Check if all return records have valid references
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_returns r
            LEFT JOIN ods_sales s ON r.original_sale_id = s.sale_id
            WHERE s.sale_id IS NULL
        """)).scalar()
        print(f"Return records with invalid original_sale_id: {result}")
        
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_returns r
            LEFT JOIN ods_return_reason rr ON r.reason_code = rr.reason_code
            WHERE rr.reason_code IS NULL
        """)).scalar()
        print(f"Return records with invalid reason_code: {result}")
        
        # Check if all inventory records have valid references
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_inventory i
            LEFT JOIN ods_product p ON i.product_id = p.product_id
            WHERE p.product_id IS NULL
        """)).scalar()
        print(f"Inventory records with invalid product_id: {result}")
        
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_inventory i
            LEFT JOIN ods_store s ON i.store_id = s.store_id
            WHERE s.store_id IS NULL
        """)).scalar()
        print(f"Inventory records with invalid store_id: {result}")
        
        # Check if we meet the volume requirements
        print("\nVerifying volume requirements:")
        sales_count = conn.execute(text("SELECT COUNT(*) FROM ods_sales")).scalar()
        print(f"Sales records: {sales_count} (requirement: at least 5,000)")
        
        product_count = conn.execute(text("SELECT COUNT(*) FROM ods_product")).scalar()
        print(f"Product records: {product_count} (requirement: at least 1,000)")
        
        store_count = conn.execute(text("SELECT COUNT(*) FROM ods_store")).scalar()
        print(f"Store records: {store_count} (requirement: at least 100)")
        
        # Check inventory snapshots
        inventory_days = conn.execute(text("""
            SELECT COUNT(DISTINCT inventory_date) FROM ods_inventory
        """)).scalar()
        print(f"Daily inventory snapshots: {inventory_days} days")
        
        # Check if we have enough return transactions (3 months)
        result = conn.execute(text("""
            SELECT COUNT(*) FROM ods_returns
        """)).scalar() or 0
        
        if result > 0:
            # If we have returns, check the date range
            months = conn.execute(text("""
                SELECT
                    EXTRACT(MONTH FROM AGE(MAX(return_date), MIN(return_date))) +
                    EXTRACT(YEAR FROM AGE(MAX(return_date), MIN(return_date))) * 12
                FROM ods_returns
            """)).scalar() or 0
            # Add 1 to include the current month
            months = months + 1
        else:
            months = 0
            
        print(f"Return transactions: {result} records covering {months} months (requirement: 3 months)")
        


def main():
    """Main function to run the ETL process."""
    # Get database engine
    engine = get_engine()
     
    # Load data from CSV file
    df = load_csv_to_dataframe()
    if df is None:
        return
    
    # Prepare data for ODS layer (no transformations)
    df = prepare_dataframe(df)
    
    # Load data into ODS layer
    load_ods_layer(engine, df)
    
    # Verify data loading
    verify_data_loading(engine)
    
    print("ETL process completed successfully!")

if __name__ == "__main__":
    main()