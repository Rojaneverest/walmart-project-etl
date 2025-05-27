"""
Walmart ETL Project - Airflow DAG
This DAG orchestrates the ETL process for the Walmart data warehouse.
It includes tasks for:
1. Table setup (if needed)
2. Synthetic data generation
3. Main ETL process
4. Data quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add the project directory to the Python path
sys.path.append('/opt/airflow/walmart-etl')

# Set environment variable to indicate we're running in Docker
os.environ['IN_DOCKER'] = 'True'

# Import the ETL functions
from etl_tables_setup import get_engine
from recreate_tables import recreate_tables
from generate_synthetic_data import main as generate_synthetic_data
from etl_data_loader import (
    load_csv_to_dataframe,
    clean_dataframe,
    generate_surrogate_keys,
    load_ods_layer,
    transform_to_staging,
    load_to_target,
    clean_staging_tables
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
    'walmart_etl_dag',
    default_args=default_args,
    description='Walmart ETL Process',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['walmart', 'etl', 'data_warehouse'],
)

# Task 1: Recreate tables (if needed)
# This is commented out by default to avoid accidental data loss
# Uncomment to include table recreation in the DAG
"""
recreate_tables_task = PythonOperator(
    task_id='recreate_tables',
    python_callable=recreate_tables,
    op_kwargs={'confirm': True},
    dag=dag,
)
"""

# Task 2: Generate synthetic data
generate_synthetic_data_task = PythonOperator(
    task_id='generate_synthetic_data',
    python_callable=generate_synthetic_data,
    dag=dag,
)

# Task 3: Load data from CSV to ODS layer
def etl_load_to_ods():
    engine = get_engine()
    df = load_csv_to_dataframe()
    df = clean_dataframe(df)
    df = generate_surrogate_keys(df)
    load_ods_layer(engine, df)
    return "ODS layer loading completed"

load_to_ods_task = PythonOperator(
    task_id='load_to_ods',
    python_callable=etl_load_to_ods,
    dag=dag,
)

# Task 4: Transform data from ODS to staging layer
def etl_transform_to_staging():
    engine = get_engine()
    batch_id = transform_to_staging(engine)
    return batch_id

transform_to_staging_task = PythonOperator(
    task_id='transform_to_staging',
    python_callable=etl_transform_to_staging,
    dag=dag,
)

# Task 5: Load data from staging to target layer
def etl_load_to_target(**context):
    engine = get_engine()
    batch_id = context['ti'].xcom_pull(task_ids='transform_to_staging')
    load_to_target(engine, batch_id)
    return batch_id

load_to_target_task = PythonOperator(
    task_id='load_to_target',
    python_callable=etl_load_to_target,
    provide_context=True,
    dag=dag,
)

# Task 6: Clean up staging tables
def etl_clean_staging(**context):
    engine = get_engine()
    batch_id = context['ti'].xcom_pull(task_ids='transform_to_staging')
    clean_staging_tables(engine, batch_id)
    return f"Staging tables cleaned for batch {batch_id}"

clean_staging_task = PythonOperator(
    task_id='clean_staging',
    python_callable=etl_clean_staging,
    provide_context=True,
    dag=dag,
)

# Task 7: Data quality checks
def run_data_quality_checks():
    """Run data quality checks on the target layer."""
    engine = get_engine()
    
    with engine.connect() as conn:
        # Check product dimension
        product_count = conn.execute("SELECT COUNT(*) FROM tgt_dim_product").scalar()
        if product_count == 0:
            print("WARNING: No data in tgt_dim_product")
        else:
            print(f"Found {product_count} records in tgt_dim_product")
        
        # Check store dimension
        store_count = conn.execute("SELECT COUNT(*) FROM tgt_dim_store").scalar()
        if store_count == 0:
            print("WARNING: No data in tgt_dim_store")
        else:
            print(f"Found {store_count} records in tgt_dim_store")
        
        # Check sales fact
        sales_count = conn.execute("SELECT COUNT(*) FROM tgt_fact_sales").scalar()
        if sales_count == 0:
            print("WARNING: No data in tgt_fact_sales")
        else:
            print(f"Found {sales_count} records in tgt_fact_sales")
    
    return f"Data quality checks completed. Products: {product_count}, Stores: {store_count}, Sales: {sales_count}"

data_quality_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

# Set task dependencies
# If table recreation is enabled, add it to the beginning of the chain
# recreate_tables_task >> generate_synthetic_data_task

generate_synthetic_data_task >> load_to_ods_task >> transform_to_staging_task >> load_to_target_task >> clean_staging_task >> data_quality_task
