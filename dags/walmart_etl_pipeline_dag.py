#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airflow DAG for the complete Walmart ETL pipeline.
This DAG orchestrates the entire ETL process by sequencing individual module function calls:
1. Create ODS tables
2. Load ODS data
3. Create staging tables
4. Load staging data
5. Create target tables
6. Load target data
7. Clear staging tables

Note: Table dropping is handled by a separate DAG (walmart_drop_tables_dag.py)
"""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook

# Add parent directory to sys.path to allow imports from parent modules
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

# Define database connection parameters based on environment
# These match the parameters in config.py
IN_DOCKER = os.environ.get('IN_DOCKER', 'True').lower() in ('true', '1', 't')
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

# Function to get connection string (matches the one in config.py)
def get_connection_string():
    """Return the database connection string based on environment."""
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Function to get database engine using Airflow connection
def get_engine():
    """Create and return a SQLAlchemy engine for PostgreSQL using Airflow connection."""
    try:
        # First try to use Airflow's connection
        from sqlalchemy import create_engine
        conn = BaseHook.get_connection('postgres_default')
        connection_string = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        return create_engine(connection_string)
    except Exception as e:
        print(f"Warning: Could not get Airflow connection: {e}")
        # Fall back to environment variables
        connection_string = get_connection_string()
        from sqlalchemy import create_engine
        return create_engine(connection_string)

# Function to clear staging tables
def clear_staging_tables():
    """Clear all staging tables after target tables are loaded."""
    try:
        print("Clearing staging tables...")
        from sqlalchemy import text
        engine = get_engine()
        
        with engine.begin() as conn:
            # Truncate all staging tables
            conn.execute(text("TRUNCATE TABLE stg_sales CASCADE"))
            conn.execute(text("TRUNCATE TABLE stg_inventory CASCADE"))
            conn.execute(text("TRUNCATE TABLE stg_returns CASCADE"))
            conn.execute(text("TRUNCATE TABLE stg_product CASCADE"))
            conn.execute(text("TRUNCATE TABLE stg_customer CASCADE"))
            conn.execute(text("TRUNCATE TABLE stg_store CASCADE"))
            conn.execute(text("TRUNCATE TABLE stg_supplier CASCADE"))
            conn.execute(text("TRUNCATE TABLE stg_return_reason CASCADE"))
            conn.execute(text("TRUNCATE TABLE stg_date CASCADE"))
        
        print("All staging tables cleared successfully!")
    except Exception as e:
        print(f"Error clearing staging tables: {e}")
        # Print more detailed connection information for debugging
        print(f"Connection parameters: Host={DB_HOST}, Port={DB_PORT}, DB={DB_NAME}, User={DB_USER}")
        print(f"IN_DOCKER={IN_DOCKER}, USE_LOCAL_POSTGRES={USE_LOCAL_POSTGRES}")

try:
    from etl_ods_tables import main as create_ods_tables
except ImportError as e:
    print(f"Warning: Could not import etl_ods_tables module: {e}")
    def create_ods_tables():
        print("Failed to import create_ods_tables function")
        # No fallback implementation provided to avoid complexity
        # This would require recreating the entire module's functionality

try:
    from etl_ods_loader import main as load_ods_data
except ImportError as e:
    print(f"Warning: Could not import etl_ods_loader module: {e}")
    def load_ods_data():
        print("Failed to import load_ods_data function")
        # No fallback implementation provided to avoid complexity

try:
    from etl_staging_tables import main as create_staging_tables
except ImportError as e:
    print(f"Warning: Could not import etl_staging_tables module: {e}")
    def create_staging_tables():
        print("Failed to import create_staging_tables function")
        # No fallback implementation provided to avoid complexity

try:
    from etl_staging_loader import main as load_staging_data
except ImportError as e:
    print(f"Warning: Could not import etl_staging_loader module: {e}")
    def load_staging_data():
        print("Failed to import load_staging_data function")
        # No fallback implementation provided to avoid complexity

try:
    from etl_target_tables import main as create_target_tables
except ImportError as e:
    print(f"Warning: Could not import etl_target_tables module: {e}")
    def create_target_tables():
        print("Failed to import create_target_tables function")
        # No fallback implementation provided to avoid complexity

try:
    from etl_target_loader import main as load_target_data
except ImportError as e:
    print(f"Warning: Could not import etl_target_loader module: {e}")
    def load_target_data():
        print("Failed to import load_target_data function")
        # No fallback implementation provided to avoid complexity

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
    'walmart_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for Walmart data',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
)

# Start task
start = DummyOperator(
    task_id='start_etl_pipeline',
    dag=dag,
)

# Create ODS tables task
create_ods_tables_task = PythonOperator(
    task_id='create_ods_tables',
    python_callable=create_ods_tables,
    dag=dag,
)

# Load ODS data task
load_ods_data_task = PythonOperator(
    task_id='load_ods_data',
    python_callable=load_ods_data,
    dag=dag,
)

# Create staging tables task
create_staging_tables_task = PythonOperator(
    task_id='create_staging_tables',
    python_callable=create_staging_tables,
    dag=dag,
)

# Load staging data task
load_staging_data_task = PythonOperator(
    task_id='load_staging_data',
    python_callable=load_staging_data,
    dag=dag,
)

# Create target tables task
create_target_tables_task = PythonOperator(
    task_id='create_target_tables',
    python_callable=create_target_tables,
    dag=dag,
)

# Load target data task
load_target_data_task = PythonOperator(
    task_id='load_target_data',
    python_callable=load_target_data,
    dag=dag,
)

# Clear staging tables task
clear_staging_tables_task = PythonOperator(
    task_id='clear_staging_tables',
    python_callable=clear_staging_tables,
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end_etl_pipeline',
    dag=dag,
)

# Task dependencies - define the execution order
start >> create_ods_tables_task >> load_ods_data_task >> create_staging_tables_task >> load_staging_data_task >> create_target_tables_task >> load_target_data_task >> clear_staging_tables_task >> end

if __name__ == "__main__":
    dag.cli()
