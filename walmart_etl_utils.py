#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Walmart ETL Project - Utility Functions
This module provides utility functions for the Walmart ETL project.
"""

from sqlalchemy import create_engine, text
from datetime import datetime
from config import get_connection_string

def get_airflow_engine():
    """
    Get a SQLAlchemy engine for use in Airflow.
    
    Returns:
        SQLAlchemy engine connected to the database
    """
    return create_engine(get_connection_string())

def log_etl_run(engine, dag_id, task_id, status, message=None):
    """
    Log ETL run information to a logging table.
    
    Args:
        engine: SQLAlchemy engine
        dag_id: The ID of the DAG
        task_id: The ID of the task
        status: The status of the run ('success', 'failed', etc.)
        message: Optional message to log
    """
    # Create the etl_log table if it doesn't exist
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_log (
                log_id SERIAL PRIMARY KEY,
                dag_id VARCHAR(200),
                task_id VARCHAR(200),
                status VARCHAR(50),
                message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Insert the log entry
        conn.execute(text("""
            INSERT INTO etl_log (dag_id, task_id, status, message, timestamp)
            VALUES (:dag_id, :task_id, :status, :message, :timestamp)
        """), {
            'dag_id': dag_id,
            'task_id': task_id,
            'status': status,
            'message': message,
            'timestamp': datetime.now()
        })
