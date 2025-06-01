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

# Add paths for module imports
# First, add the parent directory (for local development)
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Add the Docker mount path (for Docker environment)
docker_project_root = "/opt/airflow/walmart-etl"
if os.path.exists(docker_project_root) and docker_project_root not in sys.path:
    sys.path.insert(0, docker_project_root)
    
# Add the Airflow dags folder parent (another way modules might be found)
dags_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dags_parent not in sys.path:
    sys.path.insert(0, dags_parent)
    
# Add the current working directory
cwd = os.getcwd()
if cwd not in sys.path:
    sys.path.insert(0, cwd)

# Print debug info
print(f"Current working directory: {os.getcwd()}")
print(f"Files in DAGs directory: {os.listdir(os.path.dirname(__file__))}")
print(f"Parent directory: {parent_dir}")
print(f"Parent directory contents: {os.listdir(parent_dir)}")
print(f"Docker project root exists: {os.path.exists(docker_project_root)}")
if os.path.exists(docker_project_root):
    print(f"Docker project root contents: {os.listdir(docker_project_root)}")
print(f"Python path: {sys.path}")

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
'''

from sqlalchemy import create_engine

# Comment out PostgreSQL connection functions
'''
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

# Function to clear staging tables
def clear_staging_tables():
    """Clear all staging tables after target tables are loaded."""
    try:
        print("Clearing staging tables...")
        from sqlalchemy import text
        # Use the Snowflake staging engine
        engine = get_snowflake_staging_engine()
        
        with engine.begin() as conn:
            # Truncate all staging tables
            conn.execute(text("TRUNCATE TABLE stg_sales"))
            conn.execute(text("TRUNCATE TABLE stg_inventory"))
            conn.execute(text("TRUNCATE TABLE stg_returns"))
            conn.execute(text("TRUNCATE TABLE stg_product"))
            conn.execute(text("TRUNCATE TABLE stg_customer"))
            conn.execute(text("TRUNCATE TABLE stg_store"))
            conn.execute(text("TRUNCATE TABLE stg_supplier"))
            conn.execute(text("TRUNCATE TABLE stg_return_reason"))
            conn.execute(text("TRUNCATE TABLE stg_date"))
        
        print("All staging tables cleared successfully!")
    except Exception as e:
        print(f"Error clearing staging tables: {e}")
        # Print more detailed connection information for debugging
        print(f"Snowflake connection parameters: Account={SNOWFLAKE_ACCOUNT}, DB={SNOWFLAKE_DB_STAGING}, User={SNOWFLAKE_USER}")
        print(f"USE_SNOWFLAKE={USE_SNOWFLAKE}")

# Direct import of ETL modules
# First try to import directly using the sys.path we've set up
try:
    # Try to import the modules directly
    print("Attempting direct imports of ETL modules...")
    import etl_ods_tables
    import etl_ods_loader
    import etl_staging_tables
    import etl_staging_loader
    import etl_target_tables
    import etl_target_loader
    
    # Get the main functions with their correct names
    # Note: Each module has a different main function name
    create_ods_tables = etl_ods_tables.main
    load_ods_data = etl_ods_loader.main
    create_staging_tables = etl_staging_tables.main
    load_staging_data = etl_staging_loader.load_staging_layer  # This module uses load_staging_layer
    create_target_tables = etl_target_tables.main
    load_target_data = etl_target_loader.main  # This module uses main function
    
    print("Successfully imported all ETL modules directly!")
    
except ImportError as e:
    print(f"Direct import failed: {e}")
    print("Falling back to manual module loading...")
    
    # If direct import fails, try to load the modules manually
    import importlib.util
    import os.path
    
    def load_module_from_file(module_name):
        """Load a module from a file path"""
        # Try different possible locations for the module
        possible_paths = [
            # Docker path
            os.path.join('/opt/airflow/walmart-etl', f"{module_name}.py"),
            # Local path relative to DAG file
            os.path.join(parent_dir, f"{module_name}.py"),
            # Current working directory
            os.path.join(os.getcwd(), f"{module_name}.py")
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                print(f"Loading {module_name} from {path}")
                spec = importlib.util.spec_from_file_location(module_name, path)
                if spec:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    return module
        
    # Load all modules
    etl_ods_tables_module = load_module_from_file('etl_ods_tables')
    etl_ods_loader_module = load_module_from_file('etl_ods_loader')
    etl_staging_tables_module = load_module_from_file('etl_staging_tables')
    etl_staging_loader_module = load_module_from_file('etl_staging_loader')
    etl_target_tables_module = load_module_from_file('etl_target_tables')
    etl_target_loader_module = load_module_from_file('etl_target_loader')
    
    # Get the main functions with their correct names
    create_ods_tables = etl_ods_tables_module.main
    load_ods_data = etl_ods_loader_module.main
    create_staging_tables = etl_staging_tables_module.main
    load_staging_data = etl_staging_loader_module.load_staging_layer  # This module uses load_staging_layer
    create_target_tables = etl_target_tables_module.main
    load_target_data = etl_target_loader_module.main  # This module uses main function

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
