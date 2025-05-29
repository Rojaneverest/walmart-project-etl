"""
Walmart ETL Project - Synthetic Data Generation DAG
This DAG generates synthetic data for the Walmart data warehouse and loads it to the ODS layer only.
It includes tasks for:
1. Generating supplier data
2. Generating return reason data

Note: The transform_and_load_to_staging and load_to_target operations are handled by the walmart_etl_dag.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add the project directory to the Python path
sys.path.append('/opt/airflow/walmart-etl')

# Set environment variable to indicate we're running in Docker
os.environ['IN_DOCKER'] = 'True'

# Import the synthetic data generation functions
from generate_synthetic_data import (
    generate_supplier_data,
    generate_return_reason_data,
    main as generate_all_synthetic_data
)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'synthetic_data_generation',
    default_args=default_args,
    description='Generate synthetic data for the Walmart ETL project',
    schedule_interval=None,  # Only run manually
    start_date=days_ago(1),
    tags=['walmart', 'etl', 'synthetic_data'],
)

# Task: Generate all synthetic data in one go
generate_data_task = PythonOperator(
    task_id='generate_synthetic_data',
    python_callable=generate_all_synthetic_data,
    dag=dag,
)

# No dependencies needed since we only have one task
