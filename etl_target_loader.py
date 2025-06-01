#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL Target Loader Module

This module handles the ETL process from the Staging layer to the Target layer (Data Warehouse),
focusing on:
1. Loading dimension tables, including SCD Type 2 for Product and Store.
2. Loading fact tables, resolving surrogate keys from target dimensions.
"""

import os
import sys
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text, func
# Removed snowflake-specific import that was causing errors
# from sqlalchemy.dialects.snowflake import insert as snowflake_insert

# Import the target tables module (assuming it's in the same directory or Python path)
try:
    from etl_target_tables import metadata as target_metadata
    # We don't strictly need create_target_tables here if they are already created,
    # but metadata is useful if we want to refer to table objects.
    # For simplicity, we'll use table names directly in SQL strings.
except ImportError as e:
    print(f"Error importing target tables module (etl_target_tables.py): {e}")
    print("Please ensure etl_target_tables.py is in the same directory or your PYTHONPATH.")
    sys.exit(1)

# Generate a unique ETL batch ID for this run
ETL_BATCH_ID = f"TARGET_LOAD_{datetime.now().strftime('%Y%m%d%H%M%S')}"

# Snowflake connection parameters (ensure these are set as environment variables or adjust defaults)
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER', 'ROJAN')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD', 'e!Mv5ashy5aVdNb')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT', 'GEBNTIK-YU16043')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')

# Database-specific parameters
SNOWFLAKE_DB_STAGING = os.environ.get('SNOWFLAKE_DB_STAGING', 'STAGING_DB')
SNOWFLAKE_DB_TARGET = os.environ.get('SNOWFLAKE_DB_TARGET', 'TARGET_DB')

# SCD Type 2 Date Constants
TODAY_DATE = datetime.now().date()
EXPIRY_DATE_FOR_OLD_RECORDS = TODAY_DATE - timedelta(days=1)
FAR_FUTURE_EXPIRY_DATE = date(9999, 12, 31)

# --- Database Engine Functions ---
def get_snowflake_engine(db_name):
    """Create and return a SQLAlchemy engine for the specified Snowflake database."""
    connection_string = (
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
        f"{db_name}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )
    return create_engine(connection_string)

def get_snowflake_staging_engine():
    return get_snowflake_engine(SNOWFLAKE_DB_STAGING)

def get_snowflake_target_engine():
    return get_snowflake_engine(SNOWFLAKE_DB_TARGET)

# --- Helper Function to Execute SQL ---
def execute_sql(engine, sql_statement, params=None):
    """Executes a given SQL statement."""
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(text(sql_statement), params or {})
            transaction.commit()
            print(f"Successfully executed SQL.")
        except Exception as e:
            transaction.rollback()
            print(f"Error executing SQL: {sql_statement}\nParams: {params}\nError: {e}")
            raise

# --- Dimension Loading Functions ---

def load_dim_date(target_engine):
    """Loads data into tgt_dim_date from stg_date."""
    print("Loading data into tgt_dim_date...")
    
    sql = f"""
    MERGE INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date AS tgt
    USING (
        SELECT 
            date_id,
            full_date,
            day_of_week,
            day_of_month,
            month,
            month_name,
            quarter,
            year,
            is_weekend,
            is_holiday,
            fiscal_year,
            fiscal_quarter,
            etl_timestamp
        FROM (
            SELECT 
                date_id,
                full_date,
                day_of_week,
                day_of_month,
                month,
                month_name,
                quarter,
                year,
                is_weekend,
                is_holiday,
                fiscal_year,
                fiscal_quarter,
                etl_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY date_id 
                    ORDER BY etl_timestamp DESC, full_date DESC
                ) as rn
            FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date
        ) ranked
        WHERE rn = 1
    ) AS src
    ON tgt.date_id = src.date_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.full_date = src.full_date,
            tgt.day_of_week = src.day_of_week,
            tgt.day_of_month = src.day_of_month,
            tgt.month = src.month,
            tgt.month_name = src.month_name,
            tgt.quarter = src.quarter,
            tgt.year = src.year,
            tgt.is_weekend = src.is_weekend,
            tgt.is_holiday = src.is_holiday,
            tgt.fiscal_year = src.fiscal_year,
            tgt.fiscal_quarter = src.fiscal_quarter,
            tgt.modification_date = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            date_id, full_date, day_of_week, day_of_month,
            month, month_name, quarter, year, is_weekend, is_holiday,
            fiscal_year, fiscal_quarter, insertion_date, modification_date
        ) VALUES (
            src.date_id, src.full_date, src.day_of_week, src.day_of_month,
            src.month, src.month_name, src.quarter, src.year, src.is_weekend, src.is_holiday,
            src.fiscal_year, src.fiscal_quarter, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
    """
    
    execute_sql(target_engine, sql)
    print("tgt_dim_date loaded successfully.")

def load_dim_customer(target_engine):
    """Loads data into tgt_dim_customer from stg_customer (SCD Type 1 like)."""
    print("Loading data into tgt_dim_customer...")
    

    # Main MERGE with deduplication
    sql = f"""
    MERGE INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_customer tgt
    USING (
        SELECT
            customer_id, customer_name, customer_age, age_group,
            customer_segment, city, state, zip_code, region
        FROM (
            SELECT
                customer_id, customer_name, customer_age, age_group,
                customer_segment, city, state, zip_code, region,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id 
                    ORDER BY customer_name ASC, city ASC  -- Choose your business logic here
                ) as rn
            FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_customer
        ) ranked
        WHERE rn = 1
    ) src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.customer_name = src.customer_name,
            tgt.customer_age = src.customer_age,
            tgt.age_group = src.age_group,
            tgt.customer_segment = src.customer_segment,
            tgt.city = src.city,
            tgt.state = src.state,
            tgt.zip_code = src.zip_code,
            tgt.region = src.region,
            tgt.modification_date = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            customer_id, customer_name, customer_age, age_group,
            customer_segment, city, state, zip_code, region,
            insertion_date, modification_date
        ) VALUES (
            src.customer_id, src.customer_name, src.customer_age, src.age_group,
            src.customer_segment, src.city, src.state, src.zip_code, src.region,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );
    """
    
    execute_sql(target_engine, sql)
    print("tgt_dim_customer loaded successfully.")

def load_dim_supplier(target_engine):
    """Loads data into tgt_dim_supplier from stg_supplier (SCD Type 1 like)."""
    print("Loading data into tgt_dim_supplier...")
    
    sql = f"""
    MERGE INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_supplier tgt
    USING (
        SELECT
            supplier_id, supplier_name, supplier_type,
            contact_name, contact_phone, contact_email
        FROM (
            SELECT
                supplier_id, supplier_name, supplier_type,
                contact_name, contact_phone, contact_email,
                ROW_NUMBER() OVER (
                    PARTITION BY supplier_id 
                    ORDER BY supplier_name ASC, contact_name ASC
                ) as rn
            FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_supplier
        ) ranked
        WHERE rn = 1
    ) src
    ON tgt.supplier_id = src.supplier_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.supplier_name = src.supplier_name,
            tgt.supplier_type = src.supplier_type,
            tgt.contact_name = src.contact_name,
            tgt.contact_phone = src.contact_phone,
            tgt.contact_email = src.contact_email,
            tgt.modification_date = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            supplier_id, supplier_name, supplier_type,
            contact_name, contact_phone, contact_email,
            insertion_date, modification_date
        ) VALUES (
            src.supplier_id, src.supplier_name, src.supplier_type,
            src.contact_name, src.contact_phone, src.contact_email,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );
    """
    
    execute_sql(target_engine, sql)
    print("tgt_dim_supplier loaded successfully.")


def load_dim_return_reason(target_engine):
    """Loads data into tgt_dim_return_reason from stg_return_reason (SCD Type 1 like)."""
    print("Loading data into tgt_dim_return_reason...")
    
    sql = f"""
    MERGE INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_return_reason tgt
    USING (
        SELECT
            reason_code, reason_description, reason_category,
            impact_level, is_controllable
        FROM (
            SELECT
                reason_code, reason_description, reason_category,
                impact_level, is_controllable,
                ROW_NUMBER() OVER (
                    PARTITION BY reason_code 
                    ORDER BY reason_description ASC, reason_category ASC
                ) as rn
            FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_return_reason
        ) ranked
        WHERE rn = 1
    ) src
    ON tgt.reason_code = src.reason_code
    WHEN MATCHED THEN
        UPDATE SET
            tgt.reason_description = src.reason_description,
            tgt.reason_category = src.reason_category,
            tgt.impact_level = src.impact_level,
            tgt.is_controllable = src.is_controllable,
            tgt.modification_date = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            reason_code, reason_description, reason_category,
            impact_level, is_controllable,
            insertion_date, modification_date
        ) VALUES (
            src.reason_code, src.reason_description, src.reason_category,
            src.impact_level, src.is_controllable,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );
    """
    
    execute_sql(target_engine, sql)
    print("tgt_dim_return_reason loaded successfully.")

def load_dim_product_scd2(target_engine):
    """Loads data into tgt_dim_product using SCD Type 2 logic with proper handling of current records only."""
    print("Loading data into tgt_dim_product (SCD Type 2) using current records only...")

    # Step 1: Create staging table with ONLY CURRENT/LATEST records from ODS
    staging_sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE temp_product_stage AS
    SELECT 
        product_id, product_name, product_category, product_sub_category,
        product_container, unit_price, price_tier, product_base_margin,
        margin_percentage, is_high_margin, supplier_id, supplier_name,
        etl_batch_id, etl_timestamp
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY etl_timestamp DESC) as rn
        FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_product
    ) ranked
    WHERE rn = 1;  -- Only get the latest version of each product
    """

    # Step 2: Expire old records for changed products
    expire_old_records_sql = f"""
    UPDATE {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product
    SET 
        is_current = FALSE,
        expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}',
        modification_date = CURRENT_TIMESTAMP()
    WHERE product_id IN (
        SELECT s.product_id
        FROM temp_product_stage s
        JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product d
          ON s.product_id = d.product_id AND d.is_current = TRUE
        WHERE 
            -- Check if any tracked columns have changed
            CONCAT(COALESCE(s.product_name, ''), '|', COALESCE(s.product_category, ''), '|', COALESCE(s.product_sub_category, ''), '|',
                   COALESCE(s.product_container, ''), '|', COALESCE(s.unit_price::VARCHAR, ''), '|', COALESCE(s.price_tier, ''), '|',
                   COALESCE(s.product_base_margin::VARCHAR, ''), '|', COALESCE(s.margin_percentage::VARCHAR, ''), '|',
                   COALESCE(s.is_high_margin::VARCHAR, ''), '|', COALESCE(s.supplier_id, ''), '|', COALESCE(s.supplier_name, ''))
            <>
            CONCAT(COALESCE(d.product_name, ''), '|', COALESCE(d.product_category, ''), '|', COALESCE(d.product_sub_category, ''), '|',
                   COALESCE(d.product_container, ''), '|', COALESCE(d.unit_price::VARCHAR, ''), '|', COALESCE(d.price_tier, ''), '|',
                   COALESCE(d.product_base_margin::VARCHAR, ''), '|', COALESCE(d.margin_percentage::VARCHAR, ''), '|',
                   COALESCE(d.is_high_margin::VARCHAR, ''), '|', COALESCE(d.supplier_id, ''), '|', COALESCE(d.supplier_name, ''))
    )
    AND is_current = TRUE;
    """

    # Step 3: Insert new versions for changed products and completely new products
    insert_current_records_sql = f"""
    INSERT INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product (
        product_id, product_name, product_category, product_sub_category,
        product_container, unit_price, price_tier, product_base_margin,
        margin_percentage, is_high_margin, supplier_id, supplier_name,
        effective_date, expiry_date, is_current, version,
        insertion_date, modification_date
    )
    SELECT
        s.product_id, s.product_name, s.product_category, s.product_sub_category,
        s.product_container, s.unit_price, s.price_tier, s.product_base_margin,
        s.margin_percentage, s.is_high_margin, s.supplier_id, s.supplier_name,
        CASE 
            WHEN expired_products.product_id IS NOT NULL THEN '{TODAY_DATE.isoformat()}'  -- Changed product
            ELSE '2000-01-01'  -- New product
        END as effective_date,
        '9999-12-31' as expiry_date,
        TRUE as is_current,
        COALESCE(max_versions.max_version, 0) + 1 as version,
        CURRENT_TIMESTAMP() as insertion_date,
        CURRENT_TIMESTAMP() as modification_date
    FROM temp_product_stage s
    LEFT JOIN (
        -- Products that were just expired (changed products)
        SELECT DISTINCT product_id 
        FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product 
        WHERE is_current = FALSE 
        AND expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}'
    ) expired_products ON s.product_id = expired_products.product_id
    LEFT JOIN (
        -- Get the highest version for each product
        SELECT 
            product_id, 
            MAX(version) as max_version
        FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product
        GROUP BY product_id
    ) max_versions ON s.product_id = max_versions.product_id
    WHERE 
        -- Insert if it's a new product OR if it's a changed existing product
        max_versions.product_id IS NULL  -- Completely new product
        OR expired_products.product_id IS NOT NULL;  -- Changed existing product
    """

    # Step 4: Cleanup staging table
    cleanup_sql = "DROP TABLE IF EXISTS temp_product_stage;"

    # Execute all steps
    with target_engine.connect() as connection:
        transaction = connection.begin()
        try:
            print("Step 1: Creating temporary staging table with latest records only...")
            connection.execute(text(staging_sql))
            
            # --- LOGGING: Inspect temp_product_stage ---
            print("\n--- temp_product_stage content (latest records only) ---")
            sample_data_query = "SELECT product_id, product_name, unit_price, supplier_name, etl_timestamp FROM temp_product_stage ORDER BY product_id;"
            sample_data = connection.execute(text(sample_data_query)).fetchall()
            for row in sample_data:
                print(f"  {row}")
            staging_count = connection.execute(text('SELECT COUNT(*) FROM temp_product_stage')).scalar()
            print(f"Total LATEST records in temp_product_stage: {staging_count}")
            
            # Show what's in the original staging table for comparison
            original_staging_count = connection.execute(text(f'SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_product')).scalar()
            print(f"Total records in original stg_product (includes history): {original_staging_count}")
            print("-------------------------------------------\n")

            # --- LOGGING: Pre-operation counts in target ---
            print("\n--- Pre-operation counts in tgt_dim_product ---")
            initial_current_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product WHERE is_current = TRUE")).scalar()
            initial_total_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product")).scalar()
            print(f"  Initial current records: {initial_current_count}")
            print(f"  Initial total records: {initial_total_count}")
            
            # Show current products in dim table
            if initial_current_count > 0:
                print("  Current products in dim table:")
                current_products = connection.execute(text(f"SELECT product_id, product_name, version FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product WHERE is_current = TRUE ORDER BY product_id")).fetchall()
                for row in current_products:
                    print(f"    {row}")
            print("-------------------------------------------\n")

            print("Step 2: Expiring old records for changed products...")
            expire_result = connection.execute(text(expire_old_records_sql))
            print(f"Records expired: {expire_result.rowcount}")

            # --- LOGGING: Post-expire counts ---
            print("\n--- Post-expire counts in tgt_dim_product ---")
            expired_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product WHERE is_current = FALSE AND expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}'")).scalar()
            current_after_expire_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product WHERE is_current = TRUE")).scalar()
            print(f"  Records expired in this run: {expired_count}")
            print(f"  Current records after expire: {current_after_expire_count}")
            
            if expired_count > 0:
                print("  Products that were expired:")
                expired_products = connection.execute(text(f"SELECT product_id, product_name, version FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product WHERE is_current = FALSE AND expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}'")).fetchall()
                for row in expired_products:
                    print(f"    {row}")
            print("-------------------------------------------\n")

            print("Step 3: Inserting current records (new and updated versions)...")
            insert_result = connection.execute(text(insert_current_records_sql))
            print(f"Records inserted: {insert_result.rowcount}")

            # --- LOGGING: Final counts ---
            print("\n--- Final counts in tgt_dim_product ---")
            final_current_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product WHERE is_current = TRUE")).scalar()
            final_total_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product")).scalar()
            new_products_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product WHERE is_current = TRUE AND effective_date = '2000-01-01' AND version = 1")).scalar()
            updated_products_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product WHERE is_current = TRUE AND effective_date = '{TODAY_DATE.isoformat()}'")).scalar()
            
            print(f"  Final current records: {final_current_count}")
            print(f"  Final total records: {final_total_count}")
            print(f"  New products added: {new_products_count}")
            print(f"  Updated product versions: {updated_products_count}")
            
            print("  Final current products in dim table:")
            final_products = connection.execute(text(f"SELECT product_id, product_name, version, effective_date, is_current FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product WHERE is_current = TRUE ORDER BY product_id, version")).fetchall()
            for row in final_products:
                print(f"    {row}")
            print("-------------------------------------------\n")
            
            print("Step 4: Cleaning up staging table...")
            connection.execute(text(cleanup_sql))
            
            transaction.commit()
            print("tgt_dim_product (SCD Type 2) loaded successfully.")
            
        except Exception as e:
            print(f"Error during SCD Type 2 operation for tgt_dim_product. Rolling back transaction.")
            transaction.rollback()
            print(f"Error details: {e}")
            raise



def load_dim_store_scd2(target_engine):
    """Loads data into tgt_dim_store using SCD Type 2 logic with proper handling of current records only."""
    print("Loading data into tgt_dim_store (SCD Type 2) using current records only...")

    # Step 1: Create staging table with ONLY CURRENT/LATEST records from ODS
    staging_sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE temp_store_stage AS
    SELECT 
        store_id, store_name, location, city, state, zip_code, region, market,
        etl_batch_id, etl_timestamp
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY etl_timestamp DESC) as rn
        FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_store
    ) ranked
    WHERE rn = 1;  -- Only get the latest version of each store
    """

    # Step 2: Expire old records for changed stores
    expire_old_records_sql = f"""
    UPDATE {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store
    SET 
        is_current = FALSE,
        expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}',
        modification_date = CURRENT_TIMESTAMP()
    WHERE store_id IN (
        SELECT s.store_id
        FROM temp_store_stage s
        JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store d
          ON s.store_id = d.store_id AND d.is_current = TRUE
        WHERE 
            -- Check if any tracked columns have changed
            CONCAT(COALESCE(s.store_name, ''), '|', COALESCE(s.location, ''), '|', COALESCE(s.city, ''), '|',
                   COALESCE(s.state, ''), '|', COALESCE(s.zip_code, ''), '|', COALESCE(s.region, ''), '|',
                   COALESCE(s.market, ''))
            <>
            CONCAT(COALESCE(d.store_name, ''), '|', COALESCE(d.location, ''), '|', COALESCE(d.city, ''), '|',
                   COALESCE(d.state, ''), '|', COALESCE(d.zip_code, ''), '|', COALESCE(d.region, ''), '|',
                   COALESCE(d.market, ''))
    )
    AND is_current = TRUE;
    """

    # Step 3: Insert new versions for changed stores and completely new stores
    insert_current_records_sql = f"""
    INSERT INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store (
        store_id, store_name, location, city, state, zip_code, region, market,
        effective_date, expiry_date, is_current, version,
        insertion_date, modification_date
    )
    SELECT
        s.store_id, s.store_name, s.location, s.city, s.state, s.zip_code, 
        s.region, s.market,
        CASE 
            WHEN expired_stores.store_id IS NOT NULL THEN '{TODAY_DATE.isoformat()}'  -- Changed store
            ELSE '2000-01-01'  -- New store
        END as effective_date,
        '9999-12-31' as expiry_date,
        TRUE as is_current,
        COALESCE(max_versions.max_version, 0) + 1 as version,
        CURRENT_TIMESTAMP() as insertion_date,
        CURRENT_TIMESTAMP() as modification_date
    FROM temp_store_stage s
    LEFT JOIN (
        -- Stores that were just expired (changed stores)
        SELECT DISTINCT store_id 
        FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store 
        WHERE is_current = FALSE 
        AND expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}'
    ) expired_stores ON s.store_id = expired_stores.store_id
    LEFT JOIN (
        -- Get the highest version for each store
        SELECT 
            store_id, 
            MAX(version) as max_version
        FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store
        GROUP BY store_id
    ) max_versions ON s.store_id = max_versions.store_id
    WHERE 
        -- Insert if it's a new store OR if it's a changed existing store
        max_versions.store_id IS NULL  -- Completely new store
        OR expired_stores.store_id IS NOT NULL;  -- Changed existing store
    """

    # Step 4: Cleanup staging table
    cleanup_sql = "DROP TABLE IF EXISTS temp_store_stage;"

    # Execute all steps
    with target_engine.connect() as connection:
        transaction = connection.begin()
        try:
            print("Step 1: Creating temporary staging table with latest records only...")
            connection.execute(text(staging_sql))
            
            # --- LOGGING: Inspect temp_store_stage ---
            print("\n--- temp_store_stage content (latest records only) ---")
            sample_data_query = "SELECT store_id, store_name, city, state, etl_timestamp FROM temp_store_stage ORDER BY store_id;"
            sample_data = connection.execute(text(sample_data_query)).fetchall()
            for row in sample_data:
                print(f"  {row}")
            staging_count = connection.execute(text('SELECT COUNT(*) FROM temp_store_stage')).scalar()
            print(f"Total LATEST records in temp_store_stage: {staging_count}")
            
            # Show what's in the original staging table for comparison
            original_staging_count = connection.execute(text(f'SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_store')).scalar()
            print(f"Total records in original stg_store (includes history): {original_staging_count}")
            print("-------------------------------------------\n")

            # --- LOGGING: Pre-operation counts in target ---
            print("\n--- Pre-operation counts in tgt_dim_store ---")
            initial_current_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store WHERE is_current = TRUE")).scalar()
            initial_total_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store")).scalar()
            print(f"  Initial current records: {initial_current_count}")
            print(f"  Initial total records: {initial_total_count}")
            
            # Show current stores in dim table
            if initial_current_count > 0:
                print("  Current stores in dim table:")
                current_stores = connection.execute(text(f"SELECT store_id, store_name, city, version FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store WHERE is_current = TRUE ORDER BY store_id")).fetchall()
                for row in current_stores:
                    print(f"    {row}")
            print("-------------------------------------------\n")

            print("Step 2: Expiring old records for changed stores...")
            expire_result = connection.execute(text(expire_old_records_sql))
            print(f"Records expired: {expire_result.rowcount}")

            # --- LOGGING: Post-expire counts ---
            print("\n--- Post-expire counts in tgt_dim_store ---")
            expired_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store WHERE is_current = FALSE AND expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}'")).scalar()
            current_after_expire_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store WHERE is_current = TRUE")).scalar()
            print(f"  Records expired in this run: {expired_count}")
            print(f"  Current records after expire: {current_after_expire_count}")
            
            if expired_count > 0:
                print("  Stores that were expired:")
                expired_stores = connection.execute(text(f"SELECT store_id, store_name, city, version FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store WHERE is_current = FALSE AND expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}'")).fetchall()
                for row in expired_stores:
                    print(f"    {row}")
            print("-------------------------------------------\n")

            print("Step 3: Inserting current records (new and updated versions)...")
            insert_result = connection.execute(text(insert_current_records_sql))
            print(f"Records inserted: {insert_result.rowcount}")

            # --- LOGGING: Final counts ---
            print("\n--- Final counts in tgt_dim_store ---")
            final_current_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store WHERE is_current = TRUE")).scalar()
            final_total_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store")).scalar()
            new_stores_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store WHERE is_current = TRUE AND effective_date = '2000-01-01' AND version = 1")).scalar()
            updated_stores_count = connection.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store WHERE is_current = TRUE AND effective_date = '{TODAY_DATE.isoformat()}'")).scalar()
            
            print(f"  Final current records: {final_current_count}")
            print(f"  Final total records: {final_total_count}")
            print(f"  New stores added: {new_stores_count}")
            print(f"  Updated store versions: {updated_stores_count}")
            
            print("  Final current stores in dim table:")
            final_stores = connection.execute(text(f"SELECT store_id, store_name, city, version, effective_date, is_current FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store WHERE is_current = TRUE ORDER BY store_id, version")).fetchall()
            for row in final_stores:
                print(f"    {row}")
            print("-------------------------------------------\n")
            
            print("Step 4: Cleaning up staging table...")
            connection.execute(text(cleanup_sql))
            
            transaction.commit()
            print("tgt_dim_store (SCD Type 2) loaded successfully.")
            
        except Exception as e:
            print(f"Error during SCD Type 2 operation for tgt_dim_store. Rolling back transaction.")
            transaction.rollback()
            print(f"Error details: {e}")
            raise

def load_fact_sales(target_engine):
    """Loads data into tgt_fact_sales from stg_sales."""
    print("Loading data into tgt_fact_sales...")
    
    # Debug information - Check if tables have data
    with target_engine.begin() as conn:
        # Check staging sales count
        stg_sales_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_sales")).scalar()
        print(f"Staging sales table has {stg_sales_count} records")
        
        # Check staging date dimension count
        stg_date_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date")).scalar()
        print(f"Staging date dimension has {stg_date_count} records")
        
        # Check target date dimension count
        tgt_date_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date")).scalar()
        print(f"Target date dimension has {tgt_date_count} records")
        
        # Check if date keys match between staging and target
        if stg_date_count > 0 and tgt_date_count > 0:
            # Get sample staging date records
            stg_date_sample = conn.execute(text(f"""
                SELECT date_key, date_id, full_date FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date 
                ORDER BY date_id LIMIT 5
            """)).fetchall()
            print(f"Sample staging date records: {stg_date_sample}")
            
            # Get sample target date records
            tgt_date_sample = conn.execute(text(f"""
                SELECT date_key, date_id, full_date FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date 
                ORDER BY date_id LIMIT 5
            """)).fetchall()
            print(f"Sample target date records: {tgt_date_sample}")
            
            # Check if any staging date IDs exist in target
            date_match_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd
                JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date td
                ON sd.date_id = td.date_id
            """)).scalar()
            print(f"Number of matching date IDs between staging and target: {date_match_count}")
            
            # Check if any transaction dates in sales match target dates
            trans_date_match_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_sales s
                JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd
                    ON s.transaction_date_key = sd.date_key
                JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date td
                    ON sd.date_id = td.date_id
            """)).scalar()
            print(f"Number of sales records with transaction dates matching target: {trans_date_match_count}")
    
    # Modify the SQL to use date_id instead of date_key for joining between staging and target
    sql = f"""
    INSERT INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_fact_sales (
        sale_id, order_id, row_id,
        transaction_date_key, product_key, store_key, customer_key,
        order_priority, order_quantity, sales_amount, discount, discount_amount,
        shipping_cost, gross_revenue, net_revenue, profit, profit_margin, is_profitable,
        ship_date_key, ship_mode,
        insertion_date, modification_date
    )
    SELECT
        s.sale_id, s.order_id, s.row_id,
        tgt_dt_trans.date_key AS transaction_date_key,
        tgt_p.product_key AS product_key,
        tgt_st.store_key AS store_key,
        tgt_c.customer_key AS customer_key,
        s.order_priority, s.order_quantity, s.sales_amount, s.discount, s.discount_amount,
        s.shipping_cost, s.gross_revenue, s.net_revenue, s.profit, s.profit_margin, s.is_profitable,
        tgt_dt_ship.date_key AS ship_date_key,
        s.ship_mode,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_sales s
    -- Joins to resolve surrogate keys from staging to natural keys/dates, then to target keys
    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd_trans
        ON s.transaction_date_key = sd_trans.date_key -- s.transaction_date_key is surrogate from stg_date
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date tgt_dt_trans
        ON sd_trans.date_id = tgt_dt_trans.date_id -- Join on natural key (YYYYMMDD)

    LEFT JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd_ship
        ON s.ship_date_key = sd_ship.date_key -- s.ship_date_key is surrogate from stg_date
    LEFT JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date tgt_dt_ship
        ON sd_ship.date_id = tgt_dt_ship.date_id -- Join on natural key (YYYYMMDD)

    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_customer sc
        ON s.customer_key = sc.customer_key
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_customer tgt_c
        ON sc.customer_id = tgt_c.customer_id -- sc.customer_id is natural key

    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_product sp
        ON s.product_key = sp.product_key
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product tgt_p
        ON sp.product_id = tgt_p.product_id -- sp.product_id is natural key
        AND sd_trans.full_date >= tgt_p.effective_date AND sd_trans.full_date <= tgt_p.expiry_date -- SCD2 logic

    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_store sst
        ON s.store_key = sst.store_key
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store tgt_st
        ON sst.store_id = tgt_st.store_id -- sst.store_id is natural key
        AND sd_trans.full_date >= tgt_st.effective_date AND sd_trans.full_date <= tgt_st.expiry_date; -- SCD2 logic
    """
    
    # Execute the SQL and get the row count
    with target_engine.begin() as conn:
        result = conn.execute(text(sql))
        row_count = result.rowcount
        print(f"Inserted {row_count} records into tgt_fact_sales")
        
        # Check if any records were actually inserted
        if row_count == 0:
            print("WARNING: No records were inserted into tgt_fact_sales!")
            # Check if target fact table has any data
            fact_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_fact_sales")).scalar()
            print(f"Target fact_sales table has {fact_count} records after insert")
        else:
            print("tgt_fact_sales loaded successfully.")
    
    return row_count

def load_fact_inventory(target_engine):
    """Loads data into tgt_fact_inventory from stg_inventory."""
    print("Loading data into tgt_fact_inventory...")
    
    # Debug information - Check if tables have data
    with target_engine.begin() as conn:
        # Check staging inventory count
        stg_inventory_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_inventory")).scalar()
        print(f"Staging inventory table has {stg_inventory_count} records")
        
        # Check staging date dimension count
        stg_date_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date")).scalar()
        print(f"Staging date dimension has {stg_date_count} records")
        
        # Check target date dimension count
        tgt_date_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date")).scalar()
        print(f"Target date dimension has {tgt_date_count} records")
        
        # Check if date keys match between staging and target
        if stg_date_count > 0 and tgt_date_count > 0:
            # Get sample staging date records
            stg_date_sample = conn.execute(text(f"""
                SELECT date_key, date_id, full_date FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date 
                ORDER BY date_id LIMIT 5
            """)).fetchall()
            print(f"Sample staging date records: {stg_date_sample}")
            
            # Get sample target date records
            tgt_date_sample = conn.execute(text(f"""
                SELECT date_key, date_id, full_date FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date 
                ORDER BY date_id LIMIT 5
            """)).fetchall()
            print(f"Sample target date records: {tgt_date_sample}")
            
            # Check if any staging date IDs exist in target
            date_match_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd
                JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date td
                ON sd.date_id = td.date_id
            """)).scalar()
            print(f"Number of matching date IDs between staging and target: {date_match_count}")
            
            # Check if any inventory dates match target dates
            inv_date_match_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_inventory i
                JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd
                    ON i.date_key = sd.date_key
                JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date td
                    ON sd.date_id = td.date_id
            """)).scalar()
            print(f"Number of inventory records with dates matching target: {inv_date_match_count}")
    
    # Modify the SQL to use date_id instead of date_key for joining between staging and target
    sql = f"""
    INSERT INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_fact_inventory (
        inventory_id, date_key, product_key, store_key,
        stock_level, min_stock_level, max_stock_level, reorder_point,
        last_restock_date_key, days_of_supply, stock_status, is_in_stock,
        insertion_date, modification_date
    )
    SELECT
        i.inventory_id,
        tgt_dt_inv.date_key AS date_key,
        tgt_p.product_key AS product_key,
        tgt_st.store_key AS store_key,
        i.stock_level, i.min_stock_level, i.max_stock_level, i.reorder_point,
        tgt_dt_restock.date_key AS last_restock_date_key,
        i.days_of_supply, i.stock_status, i.is_in_stock,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_inventory i
    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd_inv
        ON i.date_key = sd_inv.date_key -- i.date_key is surrogate stg_date.date_key
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date tgt_dt_inv
        ON sd_inv.date_id = tgt_dt_inv.date_id

    LEFT JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd_restock
        ON i.last_restock_date_key = sd_restock.date_key
    LEFT JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date tgt_dt_restock
        ON sd_restock.date_id = tgt_dt_restock.date_id

    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_product sp
        ON i.product_key = sp.product_key
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product tgt_p
        ON sp.product_id = tgt_p.product_id
        AND sd_inv.full_date >= tgt_p.effective_date AND sd_inv.full_date <= tgt_p.expiry_date -- SCD2 logic

    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_store sst
        ON i.store_key = sst.store_key
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store tgt_st
        ON sst.store_id = tgt_st.store_id
        AND sd_inv.full_date >= tgt_st.effective_date AND sd_inv.full_date <= tgt_st.expiry_date; -- SCD2 logic
    """
    
    # Execute the SQL and get the number of rows affected
    with target_engine.begin() as conn:
        result = conn.execute(text(sql))
        rows_inserted = result.rowcount
        print(f"Inserted {rows_inserted} records into tgt_fact_inventory")
        
        if rows_inserted == 0:
            print("WARNING: No records were inserted into tgt_fact_inventory. Check the join conditions and data consistency.")
        else:
            print("tgt_fact_inventory loaded successfully.")

def load_fact_returns(target_engine):
    """Loads data into tgt_fact_returns from stg_returns."""
    print("Loading data into tgt_fact_returns...")
    
    # Debug information - Check if tables have data
    with target_engine.begin() as conn:
        # Check staging returns count
        stg_returns_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_returns")).scalar()
        print(f"Staging returns table has {stg_returns_count} records")
        
        # Check staging date dimension count
        stg_date_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date")).scalar()
        print(f"Staging date dimension has {stg_date_count} records")
        
        # Check target date dimension count
        tgt_date_count = conn.execute(text(f"SELECT COUNT(*) FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date")).scalar()
        print(f"Target date dimension has {tgt_date_count} records")
        
        # Check if date keys match between staging and target
        if stg_date_count > 0 and tgt_date_count > 0:
            # Get sample staging date records
            stg_date_sample = conn.execute(text(f"""
                SELECT date_key, date_id, full_date FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date 
                ORDER BY date_id LIMIT 5
            """)).fetchall()
            print(f"Sample staging date records: {stg_date_sample}")
            
            # Get sample target date records
            tgt_date_sample = conn.execute(text(f"""
                SELECT date_key, date_id, full_date FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date 
                ORDER BY date_id LIMIT 5
            """)).fetchall()
            print(f"Sample target date records: {tgt_date_sample}")
            
            # Check if any staging date IDs exist in target
            date_match_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd
                JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date td
                ON sd.date_id = td.date_id
            """)).scalar()
            print(f"Number of matching date IDs between staging and target: {date_match_count}")
            
            # Check if any return dates match target dates
            return_date_match_count = conn.execute(text(f"""
                SELECT COUNT(*) FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_returns r
                JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd
                    ON r.return_date_key = sd.date_key
                JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date td
                    ON sd.date_id = td.date_id
            """)).scalar()
            print(f"Number of returns records with return dates matching target: {return_date_match_count}")
    
    # Modify the SQL to use date_id instead of date_key for joining between staging and target
    sql = f"""
    INSERT INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_fact_returns (
        return_id, return_date_key, product_key, store_key, reason_key,
        reason_code, return_amount, quantity_returned, avg_return_price,
        original_sale_id, original_sale_date_key, days_since_sale,
        is_within_30_days, return_condition,
        insertion_date, modification_date
    )
    SELECT
        r.return_id,
        tgt_dt_return.date_key AS return_date_key,
        tgt_p.product_key AS product_key,
        tgt_st.store_key AS store_key,
        tgt_rr.reason_key AS reason_key,
        r.reason_code, -- Staging already has natural key for reason_code
        r.return_amount, r.quantity_returned, r.avg_return_price,
        r.original_sale_id,
        tgt_dt_orig_sale.date_key AS original_sale_date_key,
        r.days_since_sale, r.is_within_30_days, r.return_condition,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_returns r
    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd_return
        ON r.return_date_key = sd_return.date_key
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date tgt_dt_return
        ON sd_return.date_id = tgt_dt_return.date_id

    LEFT JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd_orig_sale
        ON r.original_sale_date_key = sd_orig_sale.date_key
    LEFT JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date tgt_dt_orig_sale
        ON sd_orig_sale.date_id = tgt_dt_orig_sale.date_id

    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_product sp
        ON r.product_key = sp.product_key
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product tgt_p
        ON sp.product_id = tgt_p.product_id
        AND sd_return.full_date >= tgt_p.effective_date AND sd_return.full_date <= tgt_p.expiry_date

    JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_store sst
        ON r.store_key = sst.store_key
    JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store tgt_st
        ON sst.store_id = tgt_st.store_id
        AND sd_return.full_date >= tgt_st.effective_date AND sd_return.full_date <= tgt_st.expiry_date
    
    LEFT JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_return_reason s_rr
        ON r.reason_key = s_rr.reason_key -- r.reason_key is surrogate from stg_return_reason
    LEFT JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_return_reason tgt_rr
        ON s_rr.reason_code = tgt_rr.reason_code; -- s_rr.reason_code is natural key
    """
    
    # Execute the SQL and get the number of rows affected
    with target_engine.begin() as conn:
        result = conn.execute(text(sql))
        rows_inserted = result.rowcount
        print(f"Inserted {rows_inserted} records into tgt_fact_returns")
        
        if rows_inserted == 0:
            print("WARNING: No records were inserted into tgt_fact_returns. Check the join conditions and data consistency.")
        else:
            print("tgt_fact_returns loaded successfully.")

# --- Main Execution ---
def load_target_layer():
    main()

def main():
    """Main function to orchestrate the ETL process from Staging to Target."""
    print(f"Starting Target layer ETL process with Batch ID: {ETL_BATCH_ID}")
    
    # Get database engines
    staging_engine = get_snowflake_staging_engine()
    target_engine = get_snowflake_target_engine()
    
    # Load Dimension Tables
    # Dimensions should be loaded first since facts will reference them

    # Note: Cleaning/truncating target tables before load is not included here.
    # This script assumes an append/SCD update model. For re-runnability with cleanup,
    # add TRUNCATE statements or more sophisticated deletion logic.
    print("\n--- Loading Dimension Tables ---")
    load_dim_date(target_engine)
    load_dim_customer(target_engine)
    load_dim_supplier(target_engine)
    load_dim_return_reason(target_engine)
    load_dim_product_scd2(target_engine)
    load_dim_store_scd2(target_engine)

    # Load Fact Tables
    # Ensure that facts are loaded after ALL dimensions are up-to-date.
    # Add logic here to clear fact tables if this is a full refresh
    # e.g. execute_sql(target_engine, f"TRUNCATE TABLE {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_fact_sales;") etc.
    # For this example, we'll assume appends or that idempotency is handled by the INSERT INTO SELECT structure (if it were MERGE).
    # Since these are plain INSERT INTO SELECT, running multiple times will duplicate fact data without prior cleanup.

    print("\n--- Loading Fact Tables ---")
    print("IMPORTANT: Ensure fact tables are appropriately cleaned if this is a re-run to avoid duplicates.")
    # Example: execute_sql(target_engine, f"DELETE FROM {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_fact_sales WHERE 1=1;") # Or more specific
    
    load_fact_sales(target_engine)
    load_fact_inventory(target_engine)
    load_fact_returns(target_engine)

    print(f"\nTarget layer ETL process completed successfully for Batch ID: {ETL_BATCH_ID}")

if __name__ == "__main__":
    main()