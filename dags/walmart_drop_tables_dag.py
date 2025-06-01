#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG for dropping all tables in the Walmart ETL database.
This is a separate utility DAG that can be run independently of the main ETL pipeline.
"""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# Add parent directory to sys.path to allow imports from parent modules
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

# Define database connection parameters based on environment
# Using Snowflake for all database connections
USE_SNOWFLAKE = True  # Always use Snowflake

# Common Snowflake connection parameters
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

# Comment out PostgreSQL connection parameters
'''
IN_DOCKER = os.environ.get('IN_DOCKER', 'False').lower() in ('true', '1', 't')
USE_LOCAL_POSTGRES = os.environ.get('USE_LOCAL_POSTGRES', 'False').lower() in ('true', '1', 't')

if IN_DOCKER and not USE_LOCAL_POSTGRES:
    # Docker environment (default)
    DB_USER = os.environ.get('POSTGRES_USER', 'airflow')
    DB_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
    DB_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
    DB_PORT = os.environ.get('POSTGRES_PORT', '5432')
    DB_NAME = os.environ.get('POSTGRES_DB', 'airflow')
elif IN_DOCKER and USE_LOCAL_POSTGRES:
    # Running in Docker, but want to use local Postgres
    DB_USER = "postgres"
    DB_PASSWORD = "root"
    DB_HOST = "host.docker.internal"
    DB_PORT = "5432"
    DB_NAME = "walmart_etl"
else:
    # Local development environment
    DB_USER = "postgres"
    DB_PASSWORD = "root"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "walmart_etl"
'''

from sqlalchemy import create_engine

# Comment out PostgreSQL connection function
'''
def get_connection_string():
    """Return the database connection string based on environment."""
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
'''

# Functions to get Snowflake connection engines for each database
def get_snowflake_ods_engine():
    """Return the Snowflake engine for ODS database."""
    connection_string = (
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
        f"{SNOWFLAKE_DB_ODS}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )
    return create_engine(connection_string)

def get_snowflake_staging_engine():
    """Return the Snowflake engine for Staging database."""
    connection_string = (
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
        f"{SNOWFLAKE_DB_STAGING}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )
    return create_engine(connection_string)

def get_snowflake_target_engine():
    """Return the Snowflake engine for Target database."""
    connection_string = (
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
        f"{SNOWFLAKE_DB_TARGET}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
    )
    return create_engine(connection_string)


# Import the drop_all_tables function
try:
    from drop_all_tables import drop_all_tables
except ImportError as e:
    print(f"Warning: Could not import drop_all_tables module: {e}")
    def drop_all_tables():
        print("Fallback: Using local drop_all_tables function")
        try:
            # Use the Snowflake engines
            from sqlalchemy import text
            ods_engine = get_snowflake_ods_engine()
            staging_engine = get_snowflake_staging_engine()
            target_engine = get_snowflake_target_engine()
            
            # Implement basic drop tables functionality
            print("Dropping all tables in the databases...")
            
            # Drop staging tables
            print("Dropping staging tables...")
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
                
                # Drop temporary tables in staging database
                print("Dropping temporary tables in staging database...")
                try:
                    # Get all tables in the schema
                    tables = conn.execute(text(f"""
                        SHOW TABLES IN {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}
                    """)).fetchall()
                    
                    # Drop specific named temporary tables
                    named_temp_tables = [
                        "temp_product_changes",
                        "temp_store_changes",
                        "temp_customer_changes",
                        "temp_supplier_changes",
                        "temp_return_reason_changes"
                    ]
                    
                    for table_name in named_temp_tables:
                        conn.execute(text(f"DROP TABLE IF EXISTS {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.{table_name}"))
                        print(f"Dropped temporary table: {table_name}")
                    
                    # Drop dynamically named temporary tables using pattern matching
                    for table in tables:
                        table_name = table[1] if len(table) > 1 else table[0]  # Adjust based on actual column index
                        if table_name.upper().startswith('TEMP_'):
                            conn.execute(text(f"DROP TABLE IF EXISTS {SNOWFLAKE_DB_STAGING}.{SNOWFLAKE_SCHEMA}.\"{table_name}\""))
                            print(f"Dropped dynamic temporary table: {table_name}")
                except Exception as e:
                    print(f"Error dropping temporary tables in staging database: {e}")
            
            # Drop ODS tables
            print("Dropping ODS tables...")
            with ods_engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS ods_sales CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_inventory CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_returns CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_product CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_customer CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_store CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_supplier CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_return_reason CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_date CASCADE"))
                
                # Drop temporary tables in ODS database
                print("Dropping temporary tables in ODS database...")
                try:
                    # Get all tables in the schema
                    tables = conn.execute(text(f"""
                        SHOW TABLES IN {SNOWFLAKE_DB_ODS}.{SNOWFLAKE_SCHEMA}
                    """)).fetchall()
                    
                    # Drop specific named temporary tables
                    named_temp_tables = [
                        "temp_product_changes",
                        "temp_store_changes",
                        "temp_customer_changes",
                        "temp_supplier_changes",
                        "temp_return_reason_changes"
                    ]
                    
                    for table_name in named_temp_tables:
                        conn.execute(text(f"DROP TABLE IF EXISTS {SNOWFLAKE_DB_ODS}.{SNOWFLAKE_SCHEMA}.{table_name}"))
                        print(f"Dropped temporary table: {table_name}")
                    
                    # Drop dynamically named temporary tables using pattern matching
                    for table in tables:
                        table_name = table[1] if len(table) > 1 else table[0]  # Adjust based on actual column index
                        if table_name.upper().startswith('TEMP_'):
                            conn.execute(text(f"DROP TABLE IF EXISTS {SNOWFLAKE_DB_ODS}.{SNOWFLAKE_SCHEMA}.\"{table_name}\""))
                            print(f"Dropped dynamic temporary table: {table_name}")
                except Exception as e:
                    print(f"Error dropping temporary tables in ODS database: {e}")
            
            # Drop target tables
            print("Dropping target tables...")
            with target_engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS tgt_fact_sales CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_fact_inventory CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_fact_returns CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_product CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_customer CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_store CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_supplier CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_return_reason CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_date CASCADE"))
                
                # Drop temporary tables in target database
                print("Dropping temporary tables in target database...")
                try:
                    # Get all tables in the schema
                    tables = conn.execute(text(f"""
                        SHOW TABLES IN {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}
                    """)).fetchall()
                    
                    # Drop specific named temporary tables
                    named_temp_tables = [
                        "temp_product_changes",
                        "temp_store_changes",
                        "temp_customer_changes",
                        "temp_supplier_changes",
                        "temp_return_reason_changes"
                    ]
                    
                    for table_name in named_temp_tables:
                        conn.execute(text(f"DROP TABLE IF EXISTS {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.{table_name}"))
                        print(f"Dropped temporary table: {table_name}")
                    
                    # Drop dynamically named temporary tables using pattern matching
                    for table in tables:
                        table_name = table[1] if len(table) > 1 else table[0]  # Adjust based on actual column index
                        if table_name.upper().startswith('TEMP_'):
                            conn.execute(text(f"DROP TABLE IF EXISTS {SNOWFLAKE_DB_TARGET}.{SNOWFLAKE_SCHEMA}.\"{table_name}\""))
                            print(f"Dropped dynamic temporary table: {table_name}")
                except Exception as e:
                    print(f"Error dropping temporary tables in target database: {e}")
            print("All tables dropped successfully!")
        except Exception as e:
            print(f"Error dropping tables in fallback function: {e}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 5, 30),
}

# Create the DAG
dag = DAG(
    'walmart_drop_tables',
    default_args=default_args,
    description='Drop all tables in the Walmart ETL database',
    schedule_interval=None,  # Only run when manually triggered
    catchup=False,
)

# Start task
start = DummyOperator(
    task_id='start_drop_tables',
    dag=dag,
)

# Drop tables task
drop_tables_task = PythonOperator(
    task_id='drop_all_tables',
    python_callable=drop_all_tables,
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end_drop_tables',
    dag=dag,
)

# Task dependencies
start >> drop_tables_task >> end

if __name__ == "__main__":
    dag.cli()
