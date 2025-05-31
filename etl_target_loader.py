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
    # Assuming tgt_dim_date.date_key is the YYYYMMDD integer from stg_date.date_id
    # and tgt_dim_date.date_id is also populated with the same YYYYMMDD value.
    # The target table definition has date_key as PK (not auto-increment).
    sql = f"""
    MERGE INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date AS tgt
    USING (
        SELECT
            date_id,          -- This is the YYYYMMDD value
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
            etl_timestamp AS insertion_date, -- Use staging timestamp or NOW()
            etl_timestamp AS modification_date
        FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date
    ) AS src
    ON tgt.date_key = src.date_id -- Match on the YYYYMMDD value
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
            date_key, date_id, full_date, day_of_week, day_of_month,
            month, month_name, quarter, year, is_weekend, is_holiday,
            fiscal_year, fiscal_quarter, insertion_date, modification_date
        ) VALUES (
            src.date_id, src.date_id, src.full_date, src.day_of_week, src.day_of_month,
            src.month, src.month_name, src.quarter, src.year, src.is_weekend, src.is_holiday,
            src.fiscal_year, src.fiscal_quarter, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );
    """
    execute_sql(target_engine, sql)
    print("tgt_dim_date loaded successfully.")

def load_dim_customer(target_engine):
    """Loads data into tgt_dim_customer from stg_customer (SCD Type 1 like)."""
    print("Loading data into tgt_dim_customer...")
    # tgt_dim_customer.customer_key is auto-increment.
    # We match on customer_id (natural key).
    sql = f"""
    MERGE INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_customer tgt
    USING (
        SELECT
            customer_id, customer_name, customer_age, age_group,
            customer_segment, city, state, zip_code, region
        FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_customer
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
        FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_supplier
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
        FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_return_reason
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
    """Loads data into tgt_dim_product using SCD Type 2 logic."""
    print("Loading data into tgt_dim_product (SCD Type 2)...")

    # Attributes to check for changes in Product dimension
    # From etl_staging_loader product transformations
    attributes_to_check = [
        ("s.product_name", "t.product_name"),
        ("s.product_category", "t.product_category"),
        ("s.product_sub_category", "t.product_sub_category"),
        ("s.product_container", "t.product_container"),
        ("s.unit_price", "t.unit_price"),
        ("s.price_tier", "t.price_tier"), # Derived in staging
        ("s.product_base_margin", "t.product_base_margin"),
        ("s.margin_percentage", "t.margin_percentage"), # Derived in staging
        ("s.is_high_margin", "t.is_high_margin"), # Derived in staging
        ("s.supplier_id", "t.supplier_id"),
        ("s.supplier_name", "t.supplier_name") # From join in staging
    ]
    change_conditions = " OR ".join([f"NVL(TRIM({s_col}),'') != NVL(TRIM({t_col}),'')" for s_col, t_col in attributes_to_check])
    # For numeric/boolean, NVL might not be needed or different comparison needed
    # Simplified for now, assuming string-like comparisons after casting or direct numeric/boolean compare
    # Snowflake handles type conversion in comparisons generally well.
    # A more robust comparison would be: (s.col != t.col OR (s.col IS NULL AND t.col IS NOT NULL) OR (s.col IS NOT NULL AND t.col IS NULL))

    # Step 1: Stage changes (New products and products with changed attributes)
    # product_key in tgt_dim_product is auto-increment.
    # version is managed manually for SCD2.
    temp_changes_sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE temp_product_changes AS
    SELECT
        s.product_id, s.product_name, s.product_category, s.product_sub_category,
        s.product_container, s.unit_price, s.price_tier, s.product_base_margin,
        s.margin_percentage, s.is_high_margin, s.supplier_id, s.supplier_name,
        t.product_key AS current_target_key,
        t.version AS current_version,
        CASE WHEN t.product_key IS NULL THEN 'NEW' ELSE 'CHANGED' END AS change_type
    FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_product s
    LEFT JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product t
      ON s.product_id = t.product_id AND t.is_current = TRUE
    WHERE t.product_key IS NULL OR ({change_conditions});
    """
    execute_sql(target_engine, temp_changes_sql)

    # Step 2: Expire old records for 'CHANGED' products
    expire_sql = f"""
    UPDATE {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product
    SET is_current = FALSE,
        expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}',
        modification_date = CURRENT_TIMESTAMP()
    WHERE product_key IN (SELECT current_target_key FROM temp_product_changes WHERE change_type = 'CHANGED');
    """
    execute_sql(target_engine, expire_sql)

    # Step 3: Insert new records and new versions of changed records
    insert_sql = f"""
    INSERT INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_product (
        product_id, product_name, product_category, product_sub_category,
        product_container, unit_price, price_tier, product_base_margin,
        margin_percentage, is_high_margin, supplier_id, supplier_name,
        effective_date, expiry_date, is_current, version,
        insertion_date, modification_date
    )
    SELECT
        src.product_id, src.product_name, src.product_category, src.product_sub_category,
        src.product_container, src.unit_price, src.price_tier, src.product_base_margin,
        src.margin_percentage, src.is_high_margin, src.supplier_id, src.supplier_name,
        '{TODAY_DATE.isoformat()}' AS effective_date,
        '{FAR_FUTURE_EXPIRY_DATE.isoformat()}' AS expiry_date,
        TRUE AS is_current,
        COALESCE(src.current_version, 0) + 1 AS version,
        CURRENT_TIMESTAMP() AS insertion_date,
        CURRENT_TIMESTAMP() AS modification_date
    FROM temp_product_changes src;
    """
    execute_sql(target_engine, insert_sql)
    print("tgt_dim_product (SCD Type 2) loaded successfully.")

def load_dim_store_scd2(target_engine):
    """Loads data into tgt_dim_store using SCD Type 2 logic."""
    print("Loading data into tgt_dim_store (SCD Type 2)...")

    attributes_to_check = [
        ("s.store_name", "t.store_name"),
        ("s.location", "t.location"),
        ("s.city", "t.city"),
        ("s.state", "t.state"),
        ("s.zip_code", "t.zip_code"),
        ("s.region", "t.region"),
        ("s.market", "t.market") # Derived in staging
    ]
    change_conditions = " OR ".join([f"NVL(TRIM({s_col}),'') != NVL(TRIM({t_col}),'')" for s_col, t_col in attributes_to_check])

    temp_changes_sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE temp_store_changes AS
    SELECT
        s.store_id, s.store_name, s.location, s.city, s.state,
        s.zip_code, s.region, s.market,
        t.store_key AS current_target_key,
        t.version AS current_version,
        CASE WHEN t.store_key IS NULL THEN 'NEW' ELSE 'CHANGED' END AS change_type
    FROM {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_store s
    LEFT JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store t
      ON s.store_id = t.store_id AND t.is_current = TRUE
    WHERE t.store_key IS NULL OR ({change_conditions});
    """
    execute_sql(target_engine, temp_changes_sql)

    expire_sql = f"""
    UPDATE {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store
    SET is_current = FALSE,
        expiry_date = '{EXPIRY_DATE_FOR_OLD_RECORDS.isoformat()}',
        modification_date = CURRENT_TIMESTAMP()
    WHERE store_key IN (SELECT current_target_key FROM temp_store_changes WHERE change_type = 'CHANGED');
    """
    execute_sql(target_engine, expire_sql)

    insert_sql = f"""
    INSERT INTO {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_store (
        store_id, store_name, location, city, state, zip_code, region, market,
        effective_date, expiry_date, is_current, version,
        insertion_date, modification_date
    )
    SELECT
        src.store_id, src.store_name, src.location, src.city, src.state, src.zip_code, src.region, src.market,
        '{TODAY_DATE.isoformat()}' AS effective_date,
        '{FAR_FUTURE_EXPIRY_DATE.isoformat()}' AS expiry_date,
        TRUE AS is_current,
        COALESCE(src.current_version, 0) + 1 AS version,
        CURRENT_TIMESTAMP() AS insertion_date,
        CURRENT_TIMESTAMP() AS modification_date
    FROM temp_store_changes src;
    """
    execute_sql(target_engine, insert_sql)
    print("tgt_dim_store (SCD Type 2) loaded successfully.")


# --- Fact Table Loading Functions ---

def load_fact_sales(target_engine):
    """Loads data into tgt_fact_sales from stg_sales."""
    print("Loading data into tgt_fact_sales...")
    # Fact key is auto-increment.
    # This query joins staging fact with staging dimensions to get natural keys/dates,
    # then joins with target dimensions to get target surrogate keys.
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
        ON sd_trans.date_id = tgt_dt_trans.date_key -- sd_trans.date_id is YYYYMMDD

    LEFT JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd_ship
        ON s.ship_date_key = sd_ship.date_key -- s.ship_date_key is surrogate from stg_date
    LEFT JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date tgt_dt_ship
        ON sd_ship.date_id = tgt_dt_ship.date_key -- sd_ship.date_id is YYYYMMDD

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
    execute_sql(target_engine, sql)
    print("tgt_fact_sales loaded successfully.")

def load_fact_inventory(target_engine):
    """Loads data into tgt_fact_inventory from stg_inventory."""
    print("Loading data into tgt_fact_inventory...")
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
        ON sd_inv.date_id = tgt_dt_inv.date_key

    LEFT JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd_restock
        ON i.last_restock_date_key = sd_restock.date_key
    LEFT JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date tgt_dt_restock
        ON sd_restock.date_id = tgt_dt_restock.date_key

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
    execute_sql(target_engine, sql)
    print("tgt_fact_inventory loaded successfully.")

def load_fact_returns(target_engine):
    """Loads data into tgt_fact_returns from stg_returns."""
    print("Loading data into tgt_fact_returns...")
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
        ON sd_return.date_id = tgt_dt_return.date_key

    LEFT JOIN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.stg_date sd_orig_sale
        ON r.original_sale_date_key = sd_orig_sale.date_key
    LEFT JOIN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.tgt_dim_date tgt_dt_orig_sale
        ON sd_orig_sale.date_id = tgt_dt_orig_sale.date_key

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
    execute_sql(target_engine, sql)
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