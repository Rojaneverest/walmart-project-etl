# Walmart ETL Project - Airflow Integration

This directory contains Airflow DAGs for orchestrating the Walmart ETL process. The DAGs automate the ETL workflow, provide scheduling capabilities, and include monitoring and error handling.

## DAG Files

### 1. walmart_etl_dag.py

The main ETL DAG that orchestrates the entire data pipeline:

- **Task 1**: Generate synthetic data (suppliers, return reasons)
- **Task 2**: Load data from CSV to ODS layer
- **Task 3**: Transform data from ODS to staging layer
- **Task 4**: Load data from staging to target layer
- **Task 5**: Clean up staging tables
- **Task 6**: Run data quality checks

This DAG is scheduled to run daily by default.

### 2. walmart_maintenance_dag.py

A maintenance DAG for database administration tasks:

- **Task 1**: Clean all staging tables
- **Task 2**: Database health check
- **Task 3**: Conditional table recreation (only when explicitly triggered)
- **Task 4**: Vacuum database (optimize storage)

This DAG is scheduled to run weekly on Sundays.

### 3. walmart_etl_utils.py

Utility functions for the Airflow environment:

- Database connection handling
- ETL run logging
- Airflow connection management

## Setup Instructions

1. **Start Airflow**: 
   ```bash
   cd /Users/rojan/Desktop/walmart-etl
   docker-compose up -d
   ```

2. **Access Airflow UI**:
   - Open a browser and navigate to: http://localhost:8080
   - Default login: username=airflow, password=airflow

3. **Configure Database Connection**:
   - In the Airflow UI, go to Admin > Connections
   - Add a new connection with ID `postgres_default`
   - Set the connection type to `Postgres`
   - Host: `postgres`
   - Schema: `airflow`
   - Login: `airflow`
   - Password: `airflow`
   - Port: `5432`

## Running the DAGs

### Regular ETL Process

To run the main ETL process:

1. In the Airflow UI, go to DAGs
2. Find the `walmart_etl_dag` and toggle it on
3. Click on the "Trigger DAG" button to run it manually, or wait for the scheduled run

### Maintenance Operations

To run maintenance tasks:

1. In the Airflow UI, go to DAGs
2. Find the `walmart_maintenance_dag` and toggle it on
3. To run without recreating tables, just trigger the DAG
4. To recreate all tables (CAUTION: this will delete all data), trigger with configuration:
   ```json
   {
     "recreate_tables": true
   }
   ```

## Monitoring and Troubleshooting

- **View Task Logs**: Click on a task in the Airflow UI and select "Log" to view detailed logs
- **Check ETL Run Log**: Query the `etl_run_log` table in the database for a history of ETL runs
- **Database Health**: The maintenance DAG includes a health check task that validates the database state

## Notes

- The ETL DAG is designed to handle dependencies between tasks automatically
- Data is passed between tasks using Airflow's XCom feature
- The staging tables are automatically cleaned up after each successful ETL run
- Data quality checks ensure that the target tables contain valid data
