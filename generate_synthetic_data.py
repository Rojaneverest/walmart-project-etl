#!/usr/bin/env python3
"""
Script to generate synthetic data for tables that have no data from the original CSV:
1. Supplier dimension
2. Inventory fact
3. Returns fact
4. Return Reason dimension
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import random
from datetime import datetime, timedelta
import uuid
import os
from dotenv import load_dotenv

# Import configuration
from config import get_connection_string

# Create SQLAlchemy engine
engine = create_engine(get_connection_string())

def generate_supplier_data(num_suppliers=50):
    """Generate synthetic supplier data"""
    print("Generating supplier data...")
    
    # Check if suppliers already exist
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM ods_supplier"))
        count = result.scalar()
        
        if count > 0:
            print(f"Found {count} existing supplier records, skipping generation")
            # Retrieve existing suppliers
            result = conn.execute(text("SELECT * FROM ods_supplier"))
            suppliers = []
            for row in result:
                supplier = {}
                for column, value in row._mapping.items():
                    supplier[column] = value
                suppliers.append(supplier)
            return suppliers
    
    # Company name components
    prefixes = ["Global", "National", "Premier", "Elite", "Superior", "Quality", "Value", "Prime", "Mega", "Ultra"]
    mid_parts = ["Supply", "Distribution", "Logistics", "Retail", "Wholesale", "Products", "Goods", "Merchandise", "Trading"]
    suffixes = ["Corp", "Inc", "LLC", "Co", "Partners", "Group", "Enterprises", "Solutions", "Industries"]
    
    suppliers = []
    
    # Get existing supplier IDs to avoid duplicates
    existing_ids = set()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT supplier_id FROM ods_supplier"))
            for row in result:
                existing_ids.add(row[0])
    except Exception as e:
        print(f"Error retrieving existing supplier IDs: {e}")
    
    for i in range(1, num_suppliers + 1):
        # Generate a UUID for supplier_id to avoid duplicates, truncated to 20 chars
        supplier_id = str(uuid.uuid4())[:20]
        while supplier_id in existing_ids:
            supplier_id = str(uuid.uuid4())[:20]
        supplier_name = f"{random.choice(prefixes)} {random.choice(mid_parts)} {random.choice(suffixes)}"
        contact_person = f"{random.choice(['John', 'Jane', 'Mike', 'Sarah', 'David', 'Lisa', 'Robert', 'Emily'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia'])}"
        email = f"{contact_person.lower().replace(' ', '.')}@{supplier_name.lower().replace(' ', '')}.com"
        phone = f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Location data
        city = random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"])
        states = ["California", "Texas", "New York", "Florida", "Illinois", "Pennsylvania", "Ohio", "Georgia", "North Carolina", "Michigan"]
        state = random.choice(states)
        zip_code = f"{random.randint(10000, 99999)}"
        address = f"{random.randint(100, 9999)} Main St, Suite {random.randint(100, 999)}"
        
        # Contract data
        contract_start_date = datetime(2010, 1, 1) + timedelta(days=random.randint(0, 365*3))
        
        # Create record
        suppliers.append({
            'supplier_id': supplier_id,
            'supplier_name': supplier_name,
            'contact_person': contact_person,
            'email': email,
            'phone': phone,
            'address': address,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'contract_start_date': contract_start_date,
            'source_system': 'Synthetic Data',
            'load_timestamp': datetime.now()
        })
    
    # Create DataFrame
    df_suppliers = pd.DataFrame(suppliers)
    
    # Load to ODS
    df_suppliers.to_sql('ods_supplier', engine, if_exists='append', index=False)
    print(f"Loaded {len(df_suppliers)} supplier records to ods_supplier")
    
    return df_suppliers

def generate_return_reason_data():
    """Generate synthetic return reason data"""
    print("Generating return reason data...")
    
    # Check if return reasons already exist
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM ods_return_reason"))
        count = result.scalar()
        
        if count > 0:
            print(f"Found {count} existing return reason records, skipping generation")
            # Retrieve existing return reasons
            result = conn.execute(text("SELECT * FROM ods_return_reason"))
            reasons = []
            for row in result:
                reason = {}
                for column, value in row._mapping.items():
                    reason[column] = value
                reasons.append(reason)
            return reasons
    
    reasons = [
        {'reason_code': 'DEFECT', 'reason_description': 'Product Defect', 'category': 'Quality Issue'},
        {'reason_code': 'DAMAGE', 'reason_description': 'Damaged in Transit', 'category': 'Shipping Issue'},
        {'reason_code': 'WRONG', 'reason_description': 'Wrong Item Received', 'category': 'Order Error'},
        {'reason_code': 'LATE', 'reason_description': 'Late Delivery', 'category': 'Shipping Issue'},
        {'reason_code': 'SIZE', 'reason_description': 'Wrong Size', 'category': 'Product Mismatch'},
        {'reason_code': 'COLOR', 'reason_description': 'Wrong Color', 'category': 'Product Mismatch'},
        {'reason_code': 'PERF', 'reason_description': 'Performance Issues', 'category': 'Quality Issue'},
        {'reason_code': 'CHANGE', 'reason_description': 'Customer Changed Mind', 'category': 'Customer Decision'},
        {'reason_code': 'PRICE', 'reason_description': 'Found Better Price', 'category': 'Customer Decision'},
        {'reason_code': 'OTHER', 'reason_description': 'Other Reasons', 'category': 'Miscellaneous'}
    ]
    
    # Add source system to each record
    for reason in reasons:
        reason['source_system'] = 'Synthetic Data'
    
    # Load to database
    df = pd.DataFrame(reasons)
    df.to_sql('ods_return_reason', engine, if_exists='append', index=False)
    
    print(f"Loaded {len(reasons)} return reason records to ods_return_reason")
    return reasons

def get_existing_data():
    """Retrieve existing data from the database to use for generating related synthetic data"""
    print("Retrieving existing data...")
    
    existing_data = {}
    
    # Get products
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT product_id, product_name, product_category, unit_price
                FROM ods_product
            """))
            products = []
            for row in result:
                product = {}
                for column, value in row._mapping.items():
                    product[column] = value
                products.append(product)
            existing_data['products'] = products
    except Exception as e:
        print(f"Error retrieving products: {e}")
        existing_data['products'] = []
    
    # Get stores
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT store_id, store_name, city, state
                FROM ods_store
            """))
            stores = []
            for row in result:
                store = {}
                for column, value in row._mapping.items():
                    store[column] = value
                stores.append(store)
            existing_data['stores'] = stores
    except Exception as e:
        print(f"Error retrieving stores: {e}")
        existing_data['stores'] = []
    
    # Get return reasons
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT reason_code, reason_description, category
                FROM ods_return_reason
            """))
            reasons = []
            for row in result:
                reason = {}
                for column, value in row._mapping.items():
                    reason[column] = value
                reasons.append(reason)
            existing_data['return_reasons'] = reasons
    except Exception as e:
        print(f"Error retrieving return reasons: {e}")
        existing_data['return_reasons'] = []
    
    # Get customers
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT customer_id, customer_name
                FROM ods_customer
            """))
            customers = []
            for row in result:
                customer = {}
                for column, value in row._mapping.items():
                    customer[column] = value
                customers.append(customer)
            existing_data['customers'] = customers
    except Exception as e:
        print(f"Error retrieving customers: {e}")
        existing_data['customers'] = []
    
    # Get dates
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT date_id, full_date
                FROM ods_date
            """))
            dates = []
            for row in result:
                date = {}
                for column, value in row._mapping.items():
                    date[column] = value
                dates.append(date)
            existing_data['dates'] = dates
    except Exception as e:
        print(f"Error retrieving dates: {e}")
        existing_data['dates'] = []
    
    return existing_data

def generate_inventory_data(existing_data, num_records=500):
    """Generate synthetic inventory data"""
    print("Generating inventory data...")
    
    # Check if inventory records already exist
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM ods_inventory"))
        count = result.scalar()
        
        if count > 0:
            print(f"Found {count} existing inventory records, skipping generation")
            # Retrieve existing inventory records
            result = conn.execute(text("SELECT * FROM ods_inventory"))
            inventory_records = []
            for row in result:
                record = {}
                for column, value in row._mapping.items():
                    record[column] = value
                inventory_records.append(record)
            return inventory_records
    
    products = existing_data.get('products', [])
    stores = existing_data.get('stores', [])
    
    if not products or not stores:
        print("No products or stores found. Cannot generate inventory data.")
        return []
    
    inventory_records = []
    
    # Get existing inventory IDs to avoid duplicates
    existing_ids = set()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT inventory_id FROM ods_inventory"))
            for row in result:
                existing_ids.add(row[0])
    except Exception as e:
        print(f"Error retrieving existing inventory IDs: {e}")
    
    for i in range(1, num_records + 1):
        # Generate a unique inventory ID (column is varchar(50))
        inventory_id = f"INV{uuid.uuid4().hex[:12]}"
        while inventory_id in existing_ids:
            inventory_id = f"INV{uuid.uuid4().hex[:12]}"
        
        product = random.choice(products)
        store = random.choice(stores)
        
        # Inventory date within the last 30 days
        inventory_date = datetime.now().date() - timedelta(days=random.randint(0, 30))
        
        # Stock levels
        stock_level = random.randint(0, 500)
        min_stock_level = random.randint(10, 50)
        max_stock_level = random.randint(200, 400)
        reorder_point = random.randint(min_stock_level, min_stock_level + 20)
        
        # Last restock date
        last_restock_date = inventory_date - timedelta(days=random.randint(1, 14))
        
        inventory_records.append({
            'inventory_id': inventory_id,
            'inventory_date': inventory_date,
            'product_id': product['product_id'],
            'store_id': store['store_id'],
            'stock_level': stock_level,
            'min_stock_level': min_stock_level,
            'max_stock_level': max_stock_level,
            'reorder_point': reorder_point,
            'last_restock_date': last_restock_date,
            'source_system': 'Synthetic Data',
            'load_timestamp': datetime.now()
        })
    
    # Create DataFrame
    df_inventory = pd.DataFrame(inventory_records)
    
    # Load to ODS
    df_inventory.to_sql('ods_inventory', engine, if_exists='append', index=False)
    print(f"Loaded {len(df_inventory)} inventory records to ods_inventory")
    
    return df_inventory

def generate_returns_data(existing_data, num_records=200):
    """Generate synthetic returns data"""
    print("Generating returns data...")
    
    # Check if returns records already exist
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM ods_returns"))
        count = result.scalar()
        
        if count > 0:
            print(f"Found {count} existing returns records, skipping generation")
            # Retrieve existing returns records
            result = conn.execute(text("SELECT * FROM ods_returns"))
            returns_records = []
            for row in result:
                record = {}
                for column, value in row._mapping.items():
                    record[column] = value
                returns_records.append(record)
            return returns_records
    
    products = existing_data.get('products', [])
    stores = existing_data.get('stores', [])
    return_reasons = existing_data.get('return_reasons', [])
    
    if not products or not stores or not return_reasons:
        print("Missing required data for returns generation.")
        return []
    
    returns_records = []
    
    # Get existing return IDs to avoid duplicates
    existing_ids = set()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT return_id FROM ods_returns"))
            for row in result:
                existing_ids.add(row[0])
    except Exception as e:
        print(f"Error retrieving existing return IDs: {e}")
    
    for i in range(1, num_records + 1):
        # Generate a unique return ID (column is varchar(50))
        return_id = f"RET{uuid.uuid4().hex[:12]}"
        while return_id in existing_ids:
            return_id = f"RET{uuid.uuid4().hex[:12]}"
        
        product = random.choice(products)
        store = random.choice(stores)
        reason = random.choice(return_reasons)
        
        # Return date within the last 90 days
        return_date = datetime.now().date() - timedelta(days=random.randint(0, 90))
        
        # Return details
        quantity_returned = random.randint(1, 5)
        unit_price = float(product['unit_price']) if 'unit_price' in product else random.uniform(10.0, 100.0)
        return_amount = round(quantity_returned * unit_price, 2)
        
        # Return condition
        return_condition = random.choice(['Good', 'Damaged', 'Unopened', 'Used'])
        
        # Original sale ID (fictional)
        original_sale_id = f"SALE{random.randint(10000, 99999)}"
        
        returns_records.append({
            'return_id': return_id,
            'return_date': return_date,
            'product_id': product['product_id'],
            'store_id': store['store_id'],
            'reason_code': reason['reason_code'],
            'return_amount': return_amount,
            'quantity_returned': quantity_returned,
            'return_condition': return_condition,
            'original_sale_id': original_sale_id,
            'source_system': 'Synthetic Data'
        })
    
    # Load to database
    df = pd.DataFrame(returns_records)
    df.to_sql('ods_returns', engine, if_exists='append', index=False)
    
    print(f"Loaded {len(returns_records)} return records to ods_returns")
    return returns_records

def transform_and_load_to_staging():
    """Transform ODS data and load to staging tables"""
    print("Transforming and loading data to staging tables...")
    
    # Generate batch ID
    batch_id = f"BATCH_SYNTHETIC_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Supplier dimension
    supplier_query = text("""
        INSERT INTO stg_supplier (
            supplier_id, supplier_name, contact_person, email, 
            phone, address, city, state, zip_code, contract_start_date, 
            supplier_status, etl_batch_id, etl_timestamp
        )
        SELECT 
            supplier_id, supplier_name, contact_person, email, 
            phone, address, city, state, zip_code, contract_start_date, 
            'Active', :batch_id, NOW()
        FROM ods_supplier
    """)
    
    # Return reason dimension
    reason_query = text("""
        INSERT INTO stg_return_reason (
            reason_code, reason_description, category,
            impact_level, is_controllable, etl_batch_id, etl_timestamp
        )
        SELECT 
            reason_code, reason_description, category,
            'Medium', TRUE, :batch_id, NOW()
        FROM ods_return_reason
    """)
    
    # Execute queries
    with engine.begin() as conn:
        conn.execute(supplier_query, {"batch_id": batch_id})
        conn.execute(reason_query, {"batch_id": batch_id})
    
    print(f"Supplier and return reason data successfully loaded to staging tables with batch ID: {batch_id}")
    return batch_id

def load_to_target(engine, batch_id=None):
    """Load data from staging to target tables"""
    print(f"Starting load to target layer for batch {batch_id}...")
    
    if not batch_id:
        print("Error: No batch ID provided. Cannot load to target.")
        return
    
    # Set expiry date for current records
    max_date = '9999-12-31'
    
    # Supplier dimension
    supplier_query = text("""
        INSERT INTO tgt_dim_supplier (
            supplier_id, supplier_name, contact_person, 
            email, phone, address, city, state, zip_code, country,
            contract_start_date, contract_end_date, supplier_rating, supplier_status, 
            dw_created_date, dw_modified_date, dw_version_number
        )
        SELECT 
            s.supplier_id, s.supplier_name, s.contact_person, 
            s.email, s.phone, s.address, s.city, s.state, s.zip_code, 'USA',
            s.contract_start_date, s.contract_start_date + INTERVAL '2 years', 5, s.supplier_status, 
            NOW(), NULL, 1
        FROM stg_supplier s
        LEFT JOIN tgt_dim_supplier t ON s.supplier_id = t.supplier_id
        WHERE t.supplier_id IS NULL
        AND s.etl_batch_id = :batch_id
    """)
    
    # Return reason dimension
    reason_query = text("""
        INSERT INTO tgt_dim_return_reason (
            reason_code, reason_description, category,
            impact_level, is_controllable, dw_created_date, 
            dw_modified_date, dw_version_number
        )
        SELECT 
            reason_code, reason_description, category,
            impact_level, is_controllable, NOW(), 
            NULL, 1
        FROM stg_return_reason
        WHERE etl_batch_id = :batch_id
        ON CONFLICT (reason_code) DO UPDATE
        SET reason_description = EXCLUDED.reason_description,
            category = EXCLUDED.category,
            impact_level = EXCLUDED.impact_level,
            is_controllable = EXCLUDED.is_controllable,
            dw_modified_date = NOW(),
            dw_version_number = tgt_dim_return_reason.dw_version_number + 1
    """)
    
    # Execute queries
    with engine.begin() as conn:
        conn.execute(supplier_query, {"batch_id": batch_id})
        conn.execute(reason_query, {"batch_id": batch_id, "max_date": max_date})
    
    print("Supplier and return reason data successfully loaded to target tables")

# We no longer need to create sequences since we're using auto-incrementing surrogate keys

def main():
    """Main function to generate and load synthetic data"""
    print("Starting synthetic data generation process...")
    
    # Generate and load supplier data
    suppliers = generate_supplier_data(num_suppliers=50)
    
    # Generate and load return reason data
    reasons = generate_return_reason_data()
    
    # Transform and load to staging
    batch_id = transform_and_load_to_staging()
    
    # Load to target
    load_to_target(engine, batch_id)
    
    print("Synthetic data generation and loading complete!")

if __name__ == "__main__":
    main()
