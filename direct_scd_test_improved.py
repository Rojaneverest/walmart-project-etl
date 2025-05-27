"""
Walmart ETL Project - Direct SCD Type 2 Test (Improved)
This script directly tests the SCD Type 2 implementation by:
1. Directly modifying products in the ODS layer
2. Running the transformation and loading steps
3. Checking if historical records are created in the target dimension
"""

import sys
import os
from sqlalchemy import text
from datetime import datetime

# Add the project directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set environment variable to indicate we're running in Docker
os.environ['IN_DOCKER'] = 'True'

# Import the ETL functions
from etl_tables_setup import get_engine
from etl_data_loader import transform_to_staging, load_to_target

def direct_scd_test():
    """Direct test of SCD Type 2 implementation for product dimension."""
    print("Starting direct SCD Type 2 test...")
    
    # Get the database connection
    engine = get_engine()
    
    # 1. First, check existing products in the target dimension
    with engine.connect() as conn:
        print("\nExisting products in target dimension:")
        products = conn.execute(text("""
            SELECT product_id, product_name, category, subcategory, price, current_indicator, effective_date, expiry_date
            FROM tgt_dim_product
            WHERE product_name LIKE '%Tennsco%' OR product_name LIKE '%Artistic%' OR product_name LIKE '%Crayola%'
            ORDER BY product_name, current_indicator DESC
        """)).fetchall()
        
        for product in products:
            print(f"{product.product_id} | {product.product_name} | {product.category} | {product.subcategory} | {product.price} | {product.current_indicator} | {product.effective_date} | {product.expiry_date}")
    
    # 2. Directly modify products in the ODS layer
    with engine.connect() as conn:
        print("\nModifying products in ODS layer...")
        
        # Update Tennsco product
        conn.execute(text("""
            UPDATE ods_product
            SET product_category = 'Home Decor',
                unit_price = 299.99
            WHERE product_name LIKE '%Tennsco Stur-D-Stor Boltless Shelving%'
        """))
        
        # Update Artistic product
        conn.execute(text("""
            UPDATE ods_product
            SET product_category = 'Home Decor',
                unit_price = 49.99
            WHERE product_name LIKE '%Artistic Insta-Plaque%'
        """))
        
        # Update Crayola product
        conn.execute(text("""
            UPDATE ods_product
            SET product_category = 'Art Supplies',
                product_sub_category = 'Art Supplies'
            WHERE product_name LIKE '%Crayola Colored Pencils%'
        """))
        
        print("Products modified successfully.")
    
    # 3. Generate a batch ID for this test
    batch_id = f"BATCH_TEST_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # 4. Run the transformation to staging
    print(f"\nTransforming data to staging with batch ID: {batch_id}")
    # The transform_to_staging function only takes the engine parameter
    # The batch ID is generated inside the function
    transform_to_staging(engine)
    
    # 5. Run the load to target
    print("\nLoading data from staging to target")
    # Get the batch ID from the transform_to_staging function result
    with engine.connect() as conn:
        batch_id = conn.execute(text("SELECT MAX(etl_batch_id) FROM stg_product")).scalar()
        print(f"Using batch ID from staging: {batch_id}")
    
    # The load_to_target function takes engine and batch_id
    load_to_target(engine, batch_id)
    
    # 6. Check if historical records were created
    with engine.connect() as conn:
        print("\nProducts in target dimension after SCD Type 2 processing:")
        products = conn.execute(text("""
            SELECT product_id, product_name, category, subcategory, price, current_indicator, effective_date, expiry_date
            FROM tgt_dim_product
            WHERE product_name LIKE '%Tennsco%' OR product_name LIKE '%Artistic%' OR product_name LIKE '%Crayola%'
            ORDER BY product_name, current_indicator DESC
        """)).fetchall()
        
        for product in products:
            print(f"{product.product_id} | {product.product_name} | {product.category} | {product.subcategory} | {product.price} | {product.current_indicator} | {product.effective_date} | {product.expiry_date}")
        
        # Count historical records
        historical_count = conn.execute(text("""
            SELECT COUNT(*) 
            FROM tgt_dim_product 
            WHERE current_indicator = FALSE
        """)).scalar()
        
        print(f"\nNumber of historical records (current_indicator = FALSE): {historical_count}")
    
    print("\nDirect SCD Type 2 test completed.")

if __name__ == "__main__":
    direct_scd_test()
