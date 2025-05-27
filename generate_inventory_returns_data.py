#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Generate Inventory and Returns Data
This script generates synthetic inventory and returns data for the Walmart ETL project.
It depends on products and stores data already being available in the ODS tables.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from sqlalchemy.sql import text
from etl_tables_setup import get_engine

def get_existing_data():
    """Retrieve existing data from the database to use for generating related synthetic data"""
    print("Retrieving existing data for inventory and returns generation...")
    
    engine = get_engine()
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
    
    return existing_data

def generate_inventory_data(existing_data, num_records=500):
    """Generate synthetic inventory data"""
    print("Generating inventory data...")
    
    engine = get_engine()
    
    # Check if we already have inventory data
    with engine.connect() as conn:
        inventory_count = conn.execute(text("SELECT COUNT(*) FROM ods_inventory")).scalar()
        if inventory_count > 0:
            print(f"Found {inventory_count} existing inventory records, skipping generation")
            inventory_records = []
            for row in conn.execute(text("SELECT * FROM ods_inventory")):
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
    
    for i in range(1, num_records + 1):
        inventory_id = f"INV{i:06d}"
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
    
    engine = get_engine()
    
    # Check if we already have returns data
    with engine.connect() as conn:
        returns_count = conn.execute(text("SELECT COUNT(*) FROM ods_returns")).scalar()
        if returns_count > 0:
            print(f"Found {returns_count} existing return records, skipping generation")
            return_records = []
            for row in conn.execute(text("SELECT * FROM ods_returns")):
                record = {}
                for column, value in row._mapping.items():
                    record[column] = value
                return_records.append(record)
            return return_records
    
    products = existing_data.get('products', [])
    stores = existing_data.get('stores', [])
    reasons = existing_data.get('return_reasons', [])
    
    if not products or not stores:
        print("No products or stores found. Cannot generate returns data.")
        return []
    
    if not reasons:
        print("No return reasons found. Using default reasons.")
        reasons = [
            {'reason_code': 'DEFECT', 'reason_description': 'Product Defect'},
            {'reason_code': 'DAMAGE', 'reason_description': 'Shipping Damage'},
            {'reason_code': 'WRONG', 'reason_description': 'Wrong Item Received'}
        ]
    
    return_records = []
    
    for i in range(1, num_records + 1):
        return_id = f"RET{i:06d}"
        product = random.choice(products)
        store = random.choice(stores)
        reason = random.choice(reasons)
        
        # Return date within the last 30 days
        return_date = datetime.now().date() - timedelta(days=random.randint(0, 30))
        
        # Original sale date 1-30 days before return date
        original_sale_date = return_date - timedelta(days=random.randint(1, 30))
        
        # Original sale ID
        original_sale_id = f"SALE{random.randint(10000, 99999)}"
        
        # Return amount and quantity
        quantity_returned = random.randint(1, 5)
        unit_price = product.get('unit_price', random.uniform(10, 100))
        return_amount = quantity_returned * unit_price
        
        # Return condition
        conditions = ['New', 'Like New', 'Used', 'Damaged', 'Defective']
        return_condition = random.choice(conditions)
        
        return_records.append({
            'return_id': return_id,
            'return_date': return_date,
            'product_id': product['product_id'],
            'store_id': store['store_id'],
            'reason_code': reason['reason_code'],
            'return_amount': return_amount,
            'quantity_returned': quantity_returned,
            'return_condition': return_condition,
            'original_sale_id': original_sale_id,
            'original_sale_date': original_sale_date,
            'source_system': 'Synthetic Data',
            'load_timestamp': datetime.now()
        })
    
    # Create DataFrame
    df_returns = pd.DataFrame(return_records)
    
    # Load to ODS
    df_returns.to_sql('ods_returns', engine, if_exists='append', index=False)
    print(f"Loaded {len(df_returns)} return records to ods_returns")
    
    return df_returns

def main():
    """Main function to generate and load inventory and returns data"""
    print("Starting inventory and returns data generation...")
    
    # Get existing data to use for generating related synthetic data
    existing_data = get_existing_data()
    
    # Generate and load inventory data (depends on products and stores)
    inventory_data = generate_inventory_data(existing_data, num_records=500)
    
    # Generate and load returns data (depends on products, stores, and return reasons)
    returns_data = generate_returns_data(existing_data, num_records=200)
    
    print("Inventory and returns data generation complete!")
    
    return "Inventory and returns data generation complete"

if __name__ == "__main__":
    main()
