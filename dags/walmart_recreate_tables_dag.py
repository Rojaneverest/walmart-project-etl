"""
Walmart ETL Project - Table Recreation DAG
This DAG handles the complete recreation of all database tables:
1. Drops all existing tables in the correct order
2. Recreates all tables (ODS, Staging, Target)

This DAG should be manually triggered when table structure changes are needed.
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

# Import the ETL functions
from recreate_tables import drop_all_tables, recreate_tables
from walmart_etl_utils import get_airflow_engine, log_etl_run

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
    'walmart_recreate_tables_dag',
    default_args=default_args,
    description='Walmart Database Table Recreation',
    # No schedule - this DAG should only be triggered manually
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['walmart', 'maintenance', 'data_warehouse', 'schema'],
)

# Task 1: Drop all tables
def drop_tables_task(**context):
    """Drop all tables in the database."""
    try:
        drop_all_tables()
        log_etl_run(
            get_airflow_engine(), 
            context['dag'].dag_id, 
            context['task'].task_id, 
            'success', 
            message='All tables dropped successfully'
        )
        return "All tables dropped successfully"
    except Exception as e:
        log_etl_run(
            get_airflow_engine(), 
            context['dag'].dag_id, 
            context['task'].task_id, 
            'failed', 
            message=f"Error dropping tables: {str(e)}"
        )
        raise

drop_tables = PythonOperator(
    task_id='drop_all_tables',
    python_callable=drop_tables_task,
    provide_context=True,
    dag=dag,
)

# Task 2: Recreate all tables
def recreate_tables_task(**context):
    """Recreate all tables."""
    try:
        recreate_tables()
        log_etl_run(
            get_airflow_engine(), 
            context['dag'].dag_id, 
            context['task'].task_id, 
            'success', 
            message='All tables recreated successfully'
        )
        return "All tables recreated successfully"
    except Exception as e:
        log_etl_run(
            get_airflow_engine(), 
            context['dag'].dag_id, 
            context['task'].task_id, 
            'failed', 
            message=f"Error recreating tables: {str(e)}"
        )
        raise

recreate_tables_op = PythonOperator(
    task_id='recreate_all_tables',
    python_callable=recreate_tables_task,
    provide_context=True,
    dag=dag,
)

# Task 3: Verify tables were created successfully
def verify_tables_task(**context):
    """Verify that all tables were created successfully."""
    engine = get_airflow_engine()
    tables_to_check = [
        'ods_customer', 'ods_product', 'ods_store', 'ods_sales',
        'ods_supplier', 'ods_return_reason', 'ods_date',
        'stg_customer', 'stg_product', 'stg_store', 'stg_sales',
        'stg_supplier', 'stg_return_reason', 'stg_date',
        'tgt_dim_customer', 'tgt_dim_product', 'tgt_dim_store',
        'tgt_dim_supplier', 'tgt_dim_return_reason', 'tgt_dim_date',
        'tgt_fact_sales', 'tgt_fact_inventory', 'tgt_fact_returns'
    ]
    
    try:
        missing_tables = []
        with engine.connect() as conn:
            for table in tables_to_check:
                result = conn.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{table}'
                    )
                """).scalar()
                
                if not result:
                    missing_tables.append(table)
            
            if missing_tables:
                message = f"Missing tables: {', '.join(missing_tables)}"
                log_etl_run(
                    get_airflow_engine(), 
                    context['dag'].dag_id, 
                    context['task'].task_id, 
                    'failed', 
                    message=message
                )
                raise ValueError(message)
            
            # Log success
            log_etl_run(
                get_airflow_engine(), 
                context['dag'].dag_id, 
                context['task'].task_id, 
                'success', 
                message='All tables verified successfully'
            )
            return "All tables verified successfully"
    except Exception as e:
        log_etl_run(
            get_airflow_engine(), 
            context['dag'].dag_id, 
            context['task'].task_id, 
            'failed', 
            message=f"Error verifying tables: {str(e)}"
        )
        raise

verify_tables = PythonOperator(
    task_id='verify_tables',
    python_callable=verify_tables_task,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
drop_tables >> recreate_tables_op >> verify_tables
