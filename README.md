# Walmart ETL Project: Schema Relationships & Improvement Analysis

## Database Schema Design

### Three-Layer Architecture: Logical Reasoning

The project implements a three-layer data warehouse architecture for specific data management purposes:

1. **ODS Layer (Operational Data Store)**
   - **Purpose**: Preserve source data integrity without modifications
   - **Logical Reasoning**: By storing data in its original form, we maintain a historical record of exactly what was received, enabling data lineage tracking and providing a fallback option if transformation errors occur
   - **Table Naming Convention**: `ods_*` (e.g., `ods_sales`, `ods_product`)

2. **Staging Layer**
   - **Purpose**: Intermediate transformation area
   - **Logical Reasoning**: Separating transformation from loading allows for complex business logic application without affecting source data or final tables, creating a buffer zone where data quality issues can be addressed before reaching the target layer
   - **Table Naming Convention**: `stg_*` (e.g., `stg_sales`, `stg_product`)
   - **Temporary Nature**: These tables are cleaned after successful target loading to conserve storage space and prevent stale data accumulation

3. **Target Layer**
   - **Purpose**: Dimensional model optimized for analytical queries
   - **Logical Reasoning**: Star schema design with surrogate keys improves query performance, simplifies joins, and enables proper historical tracking through SCD implementations
   - **Table Naming Convention**: `tgt_dim_*` for dimensions, `tgt_fact_*` for facts

### Key Table Relationships

#### Fact-to-Dimension Relationships

1. **tgt_fact_sales**
   - **Foreign Keys**: `date_key`, `product_key`, `store_key`, `customer_key`, `ship_date_key`
   - **Relationship Type**: Many-to-one with each dimension
   - **Logical Reasoning**: Each sales transaction relates to exactly one product, store, customer, and date, but each dimension member can participate in many sales transactions

2. **tgt_fact_inventory**
   - **Foreign Keys**: `date_key`, `product_key`, `store_key`, `last_restock_date_key`
   - **Relationship Type**: Many-to-one with each dimension
   - **Logical Reasoning**: Inventory levels are recorded for specific product-store combinations at specific points in time

3. **tgt_fact_returns**
   - **Foreign Keys**: `date_key`, `product_key`, `store_key`, `return_reason_key`, `original_sale_date_key`
   - **Relationship Type**: Many-to-one with each dimension
   - **Logical Reasoning**: Return transactions relate to specific products, stores, and return reasons, with additional context about the original sale

#### Dimension-to-Dimension Relationships

1. **tgt_dim_product to tgt_dim_supplier**
   - **Relationship**: Each product has exactly one supplier (`supplier_key` foreign key)
   - **Logical Reasoning**: Products are sourced from specific suppliers, and tracking this relationship enables supply chain analysis

2. **SCD Type 2 Implementation in Product and Store Dimensions**
   - **Key Fields**: `effective_date`, `expiry_date`, `current_indicator`
   - **Logical Reasoning**: Product and store attributes change over time (prices, categories, locations), and business analysis requires both current and historical views of these entities

## Schema Design Improvements Needed

### 1. Product-Supplier Relationship Issue

**Current Implementation**:
- All products are assigned the same default supplier (supplier_key=1)
- Code in `etl_data_loader.py` retrieves the first supplier record and uses it for all products:
```python
default_supplier = conn.execute(text("""
    SELECT supplier_key FROM tgt_dim_supplier LIMIT 1
""")).fetchone()
```

**Problem Analysis**:
- This creates an artificial relationship that doesn't reflect business reality
- It prevents accurate supply chain analysis
- It undermines the purpose of having a supplier dimension

**Logical Solution**:
- Modify ETL process to map products to their actual suppliers
- Create a proper product-supplier mapping in the source data or staging layer
- Update the loading logic to use the correct supplier key for each product

### 2. SCD Type 2 Implementation Limitations

**Current Implementation**:
- Basic SCD Type 2 for product and store dimensions
- Changes trigger new versions with updated effective/expiry dates

**Problem Analysis**:
- Current implementation treats all attribute changes equally
- No mechanism to selectively track changes to specific attributes
- No mini-dimension approach for frequently changing attributes

**Logical Solution**:
- Implement attribute-specific change tracking
- Consider hybrid SCD approaches (Type 1 for some attributes, Type 2 for others)
- Add change reason tracking to provide context for historical changes

### 3. Foreign Key Constraint Management

**Current Implementation**:
- Foreign key constraints are defined in the target layer
- Default values are used when proper relationships can't be established

**Problem Analysis**:
- Using default values (like supplier_key=1) undermines data integrity
- It creates artificial relationships that don't reflect business reality

**Logical Solution**:
- Implement proper data lineage tracking from source to target
- Enhance ETL error handling to identify and address missing relationships
- Consider implementing slowly changing dimension bridges for complex relationships

### 4. Fact Table Granularity

**Current Implementation**:
- Fact tables store data at the transaction level

**Problem Analysis**:
- High granularity provides detail but can impact query performance
- Analytical queries often need aggregated data

**Logical Solution**:
- Consider implementing aggregate fact tables for common analysis patterns
- Maintain transaction-level detail but add summary tables
- Implement materialized views for frequently accessed aggregations

### 5. Data Quality Enforcement

**Current Implementation**:
- Basic constraints at the database level
- Limited data validation during ETL

**Problem Analysis**:
- Data quality issues can propagate through the layers
- Inconsistent handling of null values, duplicates, and outliers

**Logical Solution**:
- Implement comprehensive data quality rules at each layer
- Add data profiling to identify patterns and anomalies
- Create data quality dimension to track quality metrics over time

## Conclusion

The current schema design provides a solid foundation for a retail data warehouse with its three-layer architecture and dimensional model. However, the identified improvements, particularly in the product-supplier relationship and SCD implementation, would significantly enhance data integrity and analytical capabilities. These improvements would better align the technical implementation with the business reality it aims to represent.
