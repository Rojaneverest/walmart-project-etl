# Walmart ETL Process Documentation

This document provides a detailed explanation of the ETL (Extract, Transform, Load) process implemented for the Walmart data warehouse project, including the Airflow DAGs and data flow through the system.

## Overview

The ETL process follows a three-layer architecture:

1. **ODS Layer** (Operational Data Store): Initial landing zone for raw data with minimal transformations
2. **Staging Layer**: Intermediate processing area where business transformations occur
3. **Target Layer**: Final dimensional model with star schema design for analytics

The process is orchestrated using Apache Airflow DAGs that automate the entire pipeline, ensuring proper sequencing and dependency management.

## ETL Process Flow


### Step 1: Extract Data from Source Systems

Raw data is extracted from various source systems (e.g., point-of-sale systems, inventory management systems, customer databases) and prepared for loading into the ODS layer.

### Step 2: Load Data into ODS Layer

Data is loaded into the ODS (Operational Data Store) layer with minimal transformations. This layer preserves the original data structure while standardizing formats and applying basic validations.

### Step 3: Transform Data in Staging Layer

Data from the ODS layer is transformed in the staging layer, where business rules are applied:
- Surrogate keys are generated for dimensional modeling
- Derived fields are calculated
- Data quality checks are performed
- Business logic is applied
- Data is prepared for the dimensional model

### Step 4: Load Data into Target Layer

Transformed data is loaded into the target layer, which implements a star schema dimensional model:
- Dimension tables are loaded first
- Fact tables are loaded with proper foreign key relationships
- SCD Type 2 logic is applied for historical tracking of dimension changes

### Step 5: Clear Staging Tables

After successful loading of the target layer, staging tables are cleared to free up space and prepare for the next ETL run.

## Airflow DAGs

The ETL process is orchestrated using two main Apache Airflow DAGs:

### 1. Main ETL Pipeline DAG (`walmart_etl_pipeline_dag.py`)

This DAG orchestrates the entire ETL process in sequence:

```
start → create_ods_tables → load_ods_data → create_staging_tables → load_staging_data → create_target_tables → load_target_data → clear_staging_tables → end
```

**Key Components:**

- **create_ods_tables**: Creates the ODS layer tables if they don't exist
- **load_ods_data**: Extracts data from source systems and loads it into the ODS layer
- **create_staging_tables**: Creates the staging layer tables if they don't exist
- **load_staging_data**: Transforms data from ODS and loads it into the staging layer
- **create_target_tables**: Creates the target layer tables if they don't exist
- **load_target_data**: Loads transformed data from staging into the target layer
- **clear_staging_tables**: Clears staging tables after successful target loading

**Schedule**: Runs daily at midnight (`schedule_interval='0 0 * * *'`)

### 2. Drop Tables DAG (`walmart_drop_tables_dag.py`)

This utility DAG is used to drop all tables in the database when needed (e.g., for a full reset or schema changes).

```
start → drop_all_tables → end
```

**Key Components:**

- **drop_all_tables**: Drops all tables in the database in the correct order to handle dependencies

**Schedule**: Manual trigger only (`schedule_interval=None`)

## ETL Modules

The ETL process is implemented through several Python modules that are called by the Airflow DAGs:

### Table Creation Modules

1. **etl_ods_tables.py**: Defines and creates the ODS layer tables
2. **etl_staging_tables.py**: Defines and creates the staging layer tables
3. **etl_target_tables.py**: Defines and creates the target layer tables with SCD Type 2 implementation

### Data Loading Modules

1. **etl_ods_loader.py**: Loads data from source systems into the ODS layer
2. **etl_staging_loader.py**: Transforms and loads data from ODS to staging layer
3. **etl_target_loader.py**: Loads data from staging to target layer with dimensional modeling

### Utility Modules

1. **drop_all_tables.py**: Utility to drop all tables in the database
2. **config.py**: Configuration settings including database connection parameters

## Database Connection

The ETL process supports multiple database connection methods:

1. **Airflow Connection**: Uses Airflow's connection management when running in Airflow
2. **Environment Variables**: Falls back to environment variables for connection parameters
3. **Local Development**: Uses hardcoded local development settings when needed

Connection parameters are configured based on the environment:
- `IN_DOCKER`: Whether the process is running in a Docker container
- `USE_LOCAL_POSTGRES`: Whether to use a local PostgreSQL instance

## Error Handling and Logging

The ETL process includes comprehensive error handling and logging:

1. **Import Error Handling**: Falls back to local implementations if module imports fail
2. **Database Connection Errors**: Provides informative error messages for connection issues
3. **Data Transformation Errors**: Logs detailed information about transformation failures
4. **Airflow Task Retries**: Configures automatic retries for failed tasks

## Running the ETL Process

### Using Airflow UI

1. Access the Airflow UI (typically at http://localhost:8080)
2. Navigate to the DAGs page
3. Enable the `walmart_etl_pipeline` DAG
4. Trigger the DAG manually or wait for the scheduled run

### Using Command Line

To run the ETL process from the command line:

```bash
# Navigate to the project directory
cd walmart-project-etl

# Run the ETL pipeline
python -m airflow dags trigger walmart_etl_pipeline
```

To drop all tables (use with caution):

```bash
python -m airflow dags trigger walmart_drop_tables
```

## Development and Testing

For local development and testing:

1. Set up a local PostgreSQL database
2. Configure connection parameters in `config.py` or environment variables
3. Run individual modules for testing:

```bash
# Test ODS table creation
python etl_ods_tables.py

# Test staging table creation
python etl_staging_tables.py

# Test target table creation
python etl_target_tables.py
```

## Monitoring and Maintenance

The ETL process can be monitored through:

1. **Airflow UI**: View task status, logs, and execution history
2. **Database Queries**: Check record counts and data quality in each layer
3. **Custom Logging**: Review detailed logs generated during the ETL process

Regular maintenance tasks include:

1. **Performance Tuning**: Optimize slow-running transformations
2. **Schema Updates**: Modify tables as business requirements change
3. **Data Quality Checks**: Implement additional validations as needed
