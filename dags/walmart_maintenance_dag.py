"""
Walmart ETL Project - Maintenance DAG
This DAG handles database maintenance operations such as:
1. Table recreation
2. Database cleanup
3. Database health checks
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
from etl_data_loader import clean_staging_tables

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
    'walmart_maintenance_dag',
    default_args=default_args,
    description='Walmart Database Maintenance',
    # Run weekly on Sunday
    schedule_interval='0 0 * * 0',
    start_date=days_ago(1),
    tags=['walmart', 'maintenance', 'data_warehouse'],
)

# Task 1: Clean all staging tables
clean_all_staging_task = PythonOperator(
    task_id='clean_all_staging',
    python_callable=lambda: clean_staging_tables(get_engine(), None),
    dag=dag,
)

# Task 2: Database health check
def check_database_health():
    engine = get_engine()
    with engine.connect() as conn:
        # Check if all tables exist
        tables_to_check = [
            'ods_customer', 'ods_product', 'ods_store', 'ods_sales',
            'ods_supplier', 'ods_return_reason', 'ods_date',
            'stg_customer', 'stg_product', 'stg_store', 'stg_sales',
            'stg_supplier', 'stg_return_reason', 'stg_date',
            'tgt_dim_customer', 'tgt_dim_product', 'tgt_dim_store',
            'tgt_dim_supplier', 'tgt_dim_return_reason', 'tgt_dim_date',
            'tgt_fact_sales', 'tgt_fact_inventory', 'tgt_fact_returns'
        ]
        
        missing_tables = []
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
            raise ValueError(f"Missing tables: {', '.join(missing_tables)}")
        
        # Check database size
        db_size = conn.execute("""
            SELECT pg_size_pretty(pg_database_size(current_database()))
        """).scalar()
        
        return f"Database health check passed. Current database size: {db_size}"

health_check_task = PythonOperator(
    task_id='database_health_check',
    python_callable=check_database_health,
    dag=dag,
)

# Task 3: Recreate tables (only if explicitly triggered)
def conditional_recreate_tables(**context):
    # Only recreate tables if the 'recreate_tables' parameter is set to True
    if context['dag_run'].conf and context['dag_run'].conf.get('recreate_tables'):
        print("Recreating tables as requested")
        recreate_tables(confirm=True)
        return "Tables recreated successfully"
    else:
        print("Skipping table recreation as it was not explicitly requested")
        return "Table recreation skipped"

recreate_tables_task = PythonOperator(
    task_id='conditional_recreate_tables',
    python_callable=conditional_recreate_tables,
    provide_context=True,
    dag=dag,
)

# Task 4: Vacuum database (optimize storage)
vacuum_db_task = BashOperator(
    task_id='vacuum_database',
    bash_command="""
    PGPASSWORD=$POSTGRES_PASSWORD psql -h postgres -U $POSTGRES_USER -d $POSTGRES_DB -c "VACUUM FULL ANALYZE;"
    """,
    env={
        'POSTGRES_USER': '{{ conn.postgres_default.login }}',
        'POSTGRES_PASSWORD': '{{ conn.postgres_default.password }}',
        'POSTGRES_DB': '{{ conn.postgres_default.schema }}'
    },
    dag=dag,
)

# Set task dependencies
recreate_tables_task >> clean_all_staging_task >> vacuum_db_task >> health_check_task
