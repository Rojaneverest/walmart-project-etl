#!/usr/bin/env python3
"""
Script to recreate all tables with the updated surrogate key implementation.
This will drop all existing tables and recreate them with auto-incrementing surrogate keys.
"""

from sqlalchemy import create_engine, text
import os
import time

# Import configuration
from config import get_connection_string

# Create SQLAlchemy engine
engine = create_engine(get_connection_string())

def drop_all_tables():
    """Drop all tables in the database."""
    print("Dropping all tables...")
    
    # Drop tables in the correct order to avoid foreign key constraint issues
    with engine.begin() as conn:
        # Drop fact tables first
        conn.execute(text("DROP TABLE IF EXISTS tgt_fact_sales CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_fact_inventory CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_fact_returns CASCADE"))
        
        # Drop staging tables
        conn.execute(text("DROP TABLE IF EXISTS stg_sales CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_inventory CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_returns CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_product CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_store CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_customer CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_date CASCADE"))
        
        # Drop dimension tables
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_product CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_store CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_customer CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_supplier CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_return_reason CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_date CASCADE"))
        
        # Drop ODS tables
        conn.execute(text("DROP TABLE IF EXISTS ods_sales CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_inventory CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_returns CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_product CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_store CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_customer CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_date CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_supplier CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_return_reason CASCADE"))
    
    print("All tables dropped successfully.")

def recreate_tables():
    """Recreate all tables with the updated surrogate key implementation."""
    print("Recreating tables...")
    
    # Import the table creation modules
    from etl_tables_setup import main as create_ods_tables
    from etl_staging_tables import main as create_staging_tables
    from etl_target_tables import main as create_target_tables
    
    # Create tables in the correct order
    create_ods_tables()
    create_staging_tables()
    create_target_tables()
    
    print("All tables recreated successfully.")

def main():
    """Main function to recreate all tables."""
    print("Starting table recreation process...")
    
    # Auto-confirm
    print("Automatically confirming table recreation...")
    confirm = 'y'
    
    try:
        # Drop all tables
        drop_all_tables()
        
        # Wait a moment to ensure all connections are closed
        time.sleep(1)
        
        # Recreate tables
        recreate_tables()
        
        print("Table recreation completed successfully.")
    
    except Exception as e:
        print(f"Error recreating tables: {e}")
        raise

if __name__ == "__main__":
    main()
