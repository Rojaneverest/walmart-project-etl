"""
Walmart ETL Project - Configuration
This module provides configuration settings for the ETL process,
with support for both local development and Docker environments.
"""

import os

# Determine if running in Docker
IN_DOCKER = os.environ.get('IN_DOCKER', 'False').lower() in ('true', '1', 't')

# Database connection parameters
if IN_DOCKER:
    # Docker environment
    DB_USER = os.environ.get('POSTGRES_USER', 'airflow')
    DB_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'airflow')
    DB_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
    DB_PORT = os.environ.get('POSTGRES_PORT', '5432')
    DB_NAME = os.environ.get('POSTGRES_DB', 'airflow')
    
    # File paths for Docker
    DATA_DIR = "/opt/airflow/walmart-etl/data"
    CSV_FILE = os.path.join(DATA_DIR, "walmart Retail Data.csv")
    # CSV_FILE = os.path.join(DATA_DIR, "scd_test_corrected.csv")
else:
    # Local development environment
    DB_USER = "postgres"
    DB_PASSWORD = "root"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "walmart_etl"
    
    # File paths for local development
    PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(PROJECT_DIR, "data")
    # CSV_FILE = os.path.join(DATA_DIR, "walmart Retail Data.csv")
    CSV_FILE = os.path.join(DATA_DIR, "scd_test_corrected.csv")

# Create database connection string
def get_connection_string():
    """Return the database connection string based on environment."""
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ETL batch settings
BATCH_SIZE = 10000  # Number of records to process in each batch
