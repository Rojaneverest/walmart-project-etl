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
# These match the parameters in config.py
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

# Function to get connection string (matches the one in config.py)
def get_connection_string():
    """Return the database connection string based on environment."""
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Import the drop_all_tables function
try:
    from drop_all_tables import drop_all_tables
except ImportError as e:
    print(f"Warning: Could not import drop_all_tables module: {e}")
    def drop_all_tables():
        print("Fallback: Using local drop_all_tables function")
        try:
            # Use the local get_connection_string function
            from sqlalchemy import create_engine, text
            connection_string = get_connection_string()
            engine = create_engine(connection_string)
            
            # Implement basic drop tables functionality
            print("Dropping all tables in the database...")
            with engine.begin() as conn:
                # Drop staging tables first
                conn.execute(text("DROP TABLE IF EXISTS stg_sales CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS stg_inventory CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS stg_returns CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS stg_product CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS stg_customer CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS stg_store CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS stg_supplier CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS stg_return_reason CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS stg_date CASCADE"))
                
                # Drop ODS tables
                conn.execute(text("DROP TABLE IF EXISTS ods_sales CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_inventory CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_returns CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_product CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_customer CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_store CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_supplier CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_return_reason CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS ods_date CASCADE"))
                
                # Drop target tables
                conn.execute(text("DROP TABLE IF EXISTS tgt_fact_sales CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_fact_inventory CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_fact_returns CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_product CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_customer CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_store CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_supplier CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_return_reason CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS tgt_dim_date CASCADE"))
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
