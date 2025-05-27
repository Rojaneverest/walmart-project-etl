"""
Walmart ETL Project - Airflow Utilities
This module contains helper functions for the Airflow DAGs.
"""

import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text

# Database connection parameters
DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'airflow')
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'airflow')

def get_airflow_engine():
    """
    Create a SQLAlchemy engine for connecting to the database.
    This version is adapted for Airflow environment.
    """
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def log_etl_run(engine, dag_id, task_id, status, batch_id=None, message=None):
    """
    Log ETL run details to a tracking table.
    
    Args:
        engine: SQLAlchemy engine
        dag_id: Airflow DAG ID
        task_id: Airflow task ID
        status: Status of the task (success, failed)
        batch_id: ETL batch ID (if applicable)
        message: Additional message or error details
    """
    # Create ETL log table if it doesn't exist
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_run_log (
                log_id SERIAL PRIMARY KEY,
                dag_id VARCHAR(100) NOT NULL,
                task_id VARCHAR(100) NOT NULL,
                batch_id VARCHAR(100),
                status VARCHAR(20) NOT NULL,
                message TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Insert log entry
        conn.execute(text("""
            INSERT INTO etl_run_log (dag_id, task_id, batch_id, status, message, created_at)
            VALUES (:dag_id, :task_id, :batch_id, :status, :message, :created_at)
        """), {
            'dag_id': dag_id,
            'task_id': task_id,
            'batch_id': batch_id,
            'status': status,
            'message': message,
            'created_at': datetime.now()
        })

def create_airflow_connection_tables(engine):
    """
    Create tables needed for Airflow connections if they don't exist.
    This ensures the database has all required tables for ETL.
    """
    with engine.begin() as conn:
        # Check if we need to create the connection tracking table
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'airflow_connection_tracking'
            )
        """)).scalar()
        
        if not result:
            conn.execute(text("""
                CREATE TABLE airflow_connection_tracking (
                    id SERIAL PRIMARY KEY,
                    connection_id VARCHAR(100) NOT NULL,
                    connection_type VARCHAR(50) NOT NULL,
                    last_used TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Insert default connection
            conn.execute(text("""
                INSERT INTO airflow_connection_tracking 
                (connection_id, connection_type)
                VALUES ('postgres_default', 'postgres')
            """))
            
            print("Created Airflow connection tracking table")
