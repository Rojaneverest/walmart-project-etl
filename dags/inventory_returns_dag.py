"""
Walmart ETL Project - Inventory and Returns Data Generation DAG
This DAG generates synthetic inventory and returns data for the Walmart data warehouse.
It depends on products and stores data already being available in the ODS tables.
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

# Import the inventory and returns data generation functions
from generate_inventory_returns_data import (
    get_existing_data,
    generate_inventory_data,
    generate_returns_data,
    main as generate_all_inventory_returns
)

# Import ETL functions for transformation and loading
from etl_data_loader import (
    transform_to_staging,
    load_to_target
)

# Import the get_engine function
from etl_tables_setup import get_engine

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
    'inventory_returns_generation',
    default_args=default_args,
    description='Generate inventory and returns data for the Walmart ETL project',
    schedule_interval=None,  # Only run manually
    start_date=days_ago(1),
    tags=['walmart', 'etl', 'inventory', 'returns'],
)

# Task 1: Get existing data
get_existing_data_task = PythonOperator(
    task_id='get_existing_data',
    python_callable=get_existing_data,
    dag=dag,
)

# Task 2: Generate inventory data
def generate_inventory_wrapper(**context):
    existing_data = context['ti'].xcom_pull(task_ids='get_existing_data')
    return generate_inventory_data(existing_data, num_records=500)

generate_inventory_task = PythonOperator(
    task_id='generate_inventory_data',
    python_callable=generate_inventory_wrapper,
    provide_context=True,
    dag=dag,
)

# Task 3: Generate returns data
def generate_returns_wrapper(**context):
    existing_data = context['ti'].xcom_pull(task_ids='get_existing_data')
    return generate_returns_data(existing_data, num_records=200)

generate_returns_task = PythonOperator(
    task_id='generate_returns_data',
    python_callable=generate_returns_wrapper,
    provide_context=True,
    dag=dag,
)

# Task 4: Generate all inventory and returns data (comprehensive task)
generate_all_task = PythonOperator(
    task_id='generate_all_inventory_returns',
    python_callable=generate_all_inventory_returns,
    dag=dag,
)

# Task 5: Transform data from ODS to staging
def transform_to_staging_task():
    engine = get_engine()
    batch_id = transform_to_staging(engine)
    print(f"Transformed data to staging with batch ID: {batch_id}")
    return batch_id

transform_staging_task = PythonOperator(
    task_id='transform_to_staging',
    python_callable=transform_to_staging_task,
    dag=dag,
)

# Task 6: Load data from staging to target
def load_to_target_task(**context):
    engine = get_engine()
    batch_id = context['ti'].xcom_pull(task_ids='transform_to_staging')
    load_to_target(engine, batch_id)
    print(f"Data loaded to target layer with batch ID: {batch_id}")
    return "Target layer loading completed"

load_target_task = PythonOperator(
    task_id='load_to_target',
    python_callable=load_to_target_task,
    provide_context=True,
    dag=dag,
)

# Set task dependencies for individual tasks
get_existing_data_task >> generate_inventory_task
get_existing_data_task >> generate_returns_task

# Set task dependencies for ETL process
generate_inventory_task >> transform_staging_task
generate_returns_task >> transform_staging_task
transform_staging_task >> load_target_task

# The generate_all_task can be used as an alternative to the individual generation tasks
# but should be followed by the transformation and loading tasks
generate_all_task >> transform_staging_task
