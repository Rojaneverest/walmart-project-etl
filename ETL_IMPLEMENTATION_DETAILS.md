# Walmart ETL Implementation Details

## Staging Loader Implementation

### Key Transformations and Logic

#### 1. Data Extraction and Initial Processing
- **Source Connection**: Direct connection to ODS layer in Snowflake
- **Data Selection**: Extracts only the most current records using window functions
- **Data Type Conversion**: Converts data types to match target schema requirements
- **Null Handling**: Standardizes null values and applies default values where appropriate

#### 2. Surrogate Key Generation
- **Purpose**: Create system-generated keys that are independent of business keys
- **Implementation**: 
  - Uses MD5 hashing of business keys for deterministic key generation
  - Ensures consistent keys across multiple ETL runs
  - Handles composite keys by concatenating key components
- **Example**: 
  ```python
  def generate_surrogate_key(*args):
      key_string = '|'.join(str(arg) for arg in args if arg is not None)
      return hashlib.md5(key_string.encode()).hexdigest()[:20]
  ```

#### 3. Dimension Mapping
- **Business to Technical Mapping**: Translates business keys to surrogate keys
- **Lookup Process**:
  - Maintains in-memory dictionaries for fast lookups
  - Handles late-arriving dimensions
  - Logs unmapped keys for data quality monitoring
- **Performance Optimization**:
  - Batch processing of lookups
  - Caching of frequently accessed dimensions

#### 4. Data Quality Checks
- **Referential Integrity**: Validates relationships between facts and dimensions
- **Data Validation**: Applies business rules and constraints
- **Error Handling**: Implements comprehensive error logging and notification

## Target Loader Implementation

### SCD (Slowly Changing Dimension) Implementation

#### 1. Staging Table Creation
- **Purpose**: Prepare data for SCD processing
- **Process**:
  - Creates temporary staging tables with current data
  - Includes only the most recent version of each record from source
  - Applies any necessary transformations

#### 2. SCD Type 2 Implementation

##### a) Identify Changes
```sql
-- Compare current target with staging to identify changes
SELECT 
    t.*
FROM target_table t
JOIN staging_table s ON t.business_key = s.business_key
WHERE t.is_current = TRUE
AND (
    t.attr1 != s.attr1 OR
    t.attr2 != s.attr2 OR
    -- Other attributes
    (t.attr1 IS NULL AND s.attr1 IS NOT NULL) OR
    (t.attr1 IS NOT NULL AND s.attr1 IS NULL)
)
```

##### b) Expire Old Versions
```sql
-- Set expiry date for changed records
UPDATE target_table
SET 
    is_current = FALSE,
    expiry_date = CURRENT_DATE - 1,
    modification_date = CURRENT_TIMESTAMP
WHERE business_key IN (/* changed records */)
AND is_current = TRUE;
```

##### c) Insert New Versions
```sql
-- Insert new versions with new surrogate keys
INSERT INTO target_table (
    business_key, attr1, attr2, 
    effective_date, expiry_date, 
    is_current, version
)
SELECT 
    s.business_key, s.attr1, s.attr2,
    CURRENT_DATE as effective_date,
    '9999-12-31' as expiry_date,
    TRUE as is_current,
    COALESCE(max_v.max_version, 0) + 1 as version
FROM staging_table s
LEFT JOIN (
    SELECT business_key, MAX(version) as max_version
    FROM target_table
    GROUP BY business_key
) max_v ON s.business_key = max_v.business_key
```

### Fact Table Loading

#### 1. Dimension Key Lookup
- **Process**:
  - Joins staging data with dimension tables to resolve surrogate keys
  - Handles late-arriving dimensions
  - Validates all required dimension keys exist

#### 2. Fact Table Updates
- **Incremental Loading**: Processes only new or changed records
- **Performance Optimization**:
  - Batch processing
  - Parallel loading where possible
  - Minimal logging for large inserts

### Performance Optimizations

#### 1. Batch Processing
- Processes data in configurable batch sizes
- Reduces memory footprint
- Enables better transaction management

#### 2. Index Management
- Drops and recreates indexes during load
- Disables constraints temporarily
- Rebuilds statistics after load

#### 3. Parallel Processing
- Processes independent dimensions in parallel
- Uses Snowflake's MPP capabilities
- Implements workload management

## Error Handling and Logging

### Comprehensive Logging
- Detailed step-by-step logging
- Performance metrics collection
- Data quality metrics

### Error Recovery
- Transaction management
- Checkpoint restart capability
- Detailed error messages with context

## Monitoring and Maintenance

### Performance Monitoring
- ETL duration tracking
- Row counts at each stage
- Resource utilization

### Data Quality Monitoring
- Record counts by status
- Data validation results
- Business rule violations

This implementation ensures data integrity, supports historical tracking, and provides optimal performance for the Walmart ETL pipeline.
