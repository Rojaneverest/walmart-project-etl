#!/usr/bin/env python3
"""
Script to set up all database tables in the Docker environment.
This script should be run before executing the Airflow DAGs.
"""

import os
import sys

# Set environment variable to indicate we're running in Docker
os.environ['IN_DOCKER'] = 'True'

# Import the main function from recreate_tables
from recreate_tables import main as recreate_all_tables

def setup_docker_database():
    """Create all database tables in the Docker environment."""
    print("Setting up database tables in Docker environment...")
    
    # Use the main function from recreate_tables.py
    recreate_all_tables()
    
    print("Database setup completed successfully!")

if __name__ == "__main__":
    setup_docker_database()
