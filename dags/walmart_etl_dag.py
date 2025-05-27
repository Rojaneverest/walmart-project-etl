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

# Task 2: Generate synthetic data - REMOVED
# Synthetic data generation is now handled manually outside the DAG

# Task 3: Load data from CSV to ODS layer
def etl_load_to_ods():
    engine = get_engine()
    
    # Check if we have inventory and returns data in ODS
    with engine.connect() as conn:
        inventory_count = conn.execute("SELECT COUNT(*) FROM ods_inventory").scalar()
        returns_count = conn.execute("SELECT COUNT(*) FROM ods_returns").scalar()
        print(f"Found {inventory_count} inventory records and {returns_count} returns records in ODS")
    
    # Load sales and dimension data from CSV
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

# Task 4: Generate inventory and returns data - REMOVED
# Inventory and returns data generation is now handled manually outside the DAG

# Task 4: Transform data from ODS to staging layer
def etl_transform_to_staging():
    engine = get_engine()
    
    # Transform data from ODS to staging
    batch_id = transform_to_staging(engine)
    
    # Log the batch ID for tracking
    print(f"Transformed data to staging with batch ID: {batch_id}")
    
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
    
    # Load data from staging to target
    load_to_target(engine, batch_id)
    
    # Log completion message
    print(f"Data loaded to target layer with batch ID: {batch_id}")
    
    return "Target layer loading completed"

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
    results = {}
    
    # Tables to check
    tables = [
        "tgt_dim_product", 
        "tgt_dim_store", 
        "tgt_fact_sales", 
        "tgt_fact_inventory", 
        "tgt_fact_returns"
    ]
    
    with engine.connect() as conn:
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").scalar()
            results[table] = count
            if count == 0:
                print(f"WARNING: No data in {table}")
            else:
                print(f"Found {count} records in {table}")
    
    # Check for SCD Type 2 historical records
    with engine.connect() as conn:
        historical_product_count = conn.execute(
            "SELECT COUNT(*) FROM tgt_dim_product WHERE current_indicator = FALSE"
        ).scalar()
        print(f"Found {historical_product_count} historical product records")
        
        historical_store_count = conn.execute(
            "SELECT COUNT(*) FROM tgt_dim_store WHERE current_indicator = FALSE"
        ).scalar()
        print(f"Found {historical_store_count} historical store records")
    
    return f"Data quality checks completed: {results}"

data_quality_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

# Set task dependencies
# If table recreation is enabled, add it to the beginning of the chain
# recreate_tables_task >> generate_synthetic_data_task

# Updated task dependencies - synthetic data generation and inventory/returns generation removed
load_to_ods_task >> transform_to_staging_task >> load_to_target_task >> clean_staging_task >> data_quality_task
