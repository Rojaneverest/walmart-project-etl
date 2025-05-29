# Walmart ETL Project

## Overview

This project implements a comprehensive ETL (Extract, Transform, Load) pipeline for Walmart retail data. The architecture follows a classic three-layer data warehouse design:

1. **ODS Layer** (Operational Data Store) - Initial data landing area
2. **Staging Layer** - Intermediate transformation layer
3. **Target Layer** - Final dimensional model with star schema

The data model supports retail analytics for sales, inventory, and returns data, enabling business intelligence and reporting across multiple dimensions.

## Database Schema

The database schema is designed as a star schema in the target layer, with dimension tables surrounding fact tables. The schema progresses through three layers of increasing refinement and business value.

### ODS Layer Tables

The ODS (Operational Data Store) layer serves as the initial landing zone for data from source systems. These tables closely mirror the source data structure with minimal transformations.

#### Dimension Tables

| Table Name | Description | Key Fields |
|------------|-------------|------------|
| `ods_date` | Calendar dates for all transactions | `date_id` (PK), `full_date`, `day_of_week`, `month`, `year` |
| `ods_customer` | Customer information | `customer_id` (PK), `customer_name`, `customer_segment`, `region` |
| `ods_product` | Product catalog | `product_id` (PK), `product_name`, `product_category`, `unit_price` |
| `ods_store` | Store locations | `store_id` (PK), `store_name`, `location`, `region` |
| `ods_supplier` | Supplier information | `supplier_id` (PK), `supplier_name`, `contact_person` |
| `ods_return_reason` | Reasons for product returns | `reason_code` (PK), `reason_description`, `category` |

#### Fact Tables

| Table Name | Description | Key Fields | Relationships |
|------------|-------------|------------|---------------|
| `ods_sales` | Sales transactions | `sale_id` (PK), `order_id`, `transaction_date`, `sales_amount` | Links to `product_id`, `store_id`, `customer_id` |
| `ods_inventory` | Inventory levels | `inventory_id` (PK), `inventory_date`, `stock_level` | Links to `product_id`, `store_id` |
| `ods_returns` | Product returns | `return_id` (PK), `return_date`, `return_amount` | Links to `product_id`, `store_id`, `reason_code`, `original_sale_id` |

### Target Layer Tables

The Target layer implements a dimensional model optimized for analytics and reporting. It features surrogate keys, slowly changing dimensions (SCD Type 2 for products and stores), and proper referential integrity.

#### Dimension Tables

| Table Name | Description | Key Features |
|------------|-------------|-------------|
| `tgt_dim_date` | Time dimension | Enhanced with fiscal periods, holidays, weekends |
| `tgt_dim_customer` | Customer dimension | Customer segmentation, demographics, loyalty information |
| `tgt_dim_product` | Product dimension (SCD Type 2) | Tracks changes to product attributes over time with `effective_date`, `expiry_date`, and `current_indicator` |
| `tgt_dim_store` | Store dimension (SCD Type 2) | Tracks changes to store attributes over time |
| `tgt_dim_supplier` | Supplier dimension | Enhanced with supplier ratings and contract information |
| `tgt_dim_return_reason` | Return reason dimension | Categorized return reasons with impact assessment |

#### Fact Tables

| Table Name | Description | Measures | Dimensions |
|------------|-------------|----------|------------|
| `tgt_fact_sales` | Sales transactions | `sales_amount`, `quantity_sold`, `profit_margin`, `discount_amount` | Date, Product, Store, Customer |
| `tgt_fact_inventory` | Inventory snapshots | `stock_level`, `days_of_supply`, `is_in_stock` | Date, Product, Store |
| `tgt_fact_returns` | Product returns | `return_amount`, `quantity_returned`, `days_since_purchase` | Date, Product, Store, Return Reason |

## Relationships and Logical Design

### Core Relationships

1. **Sales Analysis**
   - Each sales record in `tgt_fact_sales` is associated with:
     - When the sale occurred (`tgt_dim_date`)
     - What product was sold (`tgt_dim_product`)
     - Where the sale took place (`tgt_dim_store`)
     - Who purchased the item (`tgt_dim_customer`)
   - This enables analysis by time, product, location, and customer segments

2. **Inventory Management**
   - Each inventory record in `tgt_fact_inventory` captures:
     - Inventory level at a specific date (`tgt_dim_date`)
     - For a specific product (`tgt_dim_product`)
     - At a specific store (`tgt_dim_store`)
   - This enables stock level monitoring, reorder point analysis, and out-of-stock tracking

3. **Returns Analysis**
   - Each return record in `tgt_fact_returns` links:
     - When the return occurred (`tgt_dim_date`)
     - What product was returned (`tgt_dim_product`)
     - Where the return was processed (`tgt_dim_store`)
     - Why the product was returned (`tgt_dim_return_reason`)
     - When the original sale occurred (`original_sale_date_key` → `tgt_dim_date`)
   - This enables return rate analysis, reason categorization, and time-to-return metrics

### Slowly Changing Dimensions (SCD Type 2)

The model implements SCD Type 2 for product and store dimensions to track historical changes:

- **Product Changes**: When product attributes change (price, category, etc.), a new record is created with:
  - Same `product_id` but new `product_key`
  - New `effective_date` and previous record's `expiry_date` updated
  - Current record has `current_indicator = true`, historical records have `current_indicator = false`

- **Store Changes**: Similarly tracks changes to store attributes (name, type, size, etc.)

This historical tracking enables accurate point-in-time analysis, such as:
- Sales analysis with the product price that was active at the time of sale
- Inventory analysis with the store configuration that was active at the time of inventory count

### Date Dimension Relationships

The date dimension (`tgt_dim_date`) is heavily utilized throughout the model:

1. **Multiple Date References in Sales**:
   - `date_key`: Transaction date
   - `ship_date_key`: When the product was shipped

2. **Multiple Date References in Returns**:
   - `date_key`: Return processing date
   - `original_sale_date_key`: When the product was originally purchased

3. **Multiple Date References in Inventory**:
   - `date_key`: Inventory snapshot date
   - `last_restock_date_key`: When the product was last restocked

This enables time-based analysis such as:
- Order-to-ship lag time
- Purchase-to-return duration
- Days since last restock

## Business Value

This data model enables various business analytics capabilities:

1. **Sales Performance Analysis**
   - Sales trends over time (daily, weekly, monthly, quarterly)
   - Product category performance
   - Store performance by region
   - Customer segment analysis

2. **Inventory Optimization**
   - Stock level monitoring
   - Reorder point optimization
   - Identification of slow-moving inventory
   - Out-of-stock analysis

3. **Returns Management**
   - Return rate by product category
   - Return reason analysis
   - Impact of returns on profitability
   - Identification of problematic products or suppliers

4. **Cross-Functional Analysis**
   - Correlation between inventory levels and sales performance
   - Impact of stockouts on customer returns
   - Supplier quality impact on returns
   - Regional variations in sales, inventory, and returns patterns

## ETL Process Flow

The ETL process moves data through the three layers:

1. **Source to ODS**: Initial data extraction with minimal transformation
2. **ODS to Staging**: Data cleansing, standardization, and enrichment
3. **Staging to Target**: Final dimensional modeling with business rules applied

Each layer adds value through:
- Data quality improvements
- Addition of derived attributes
- Implementation of business rules
- Creation of surrogate keys and relationships

## Technical Implementation

The project is implemented using:
- Python for ETL processing
- SQLAlchemy for database interactions
- PostgreSQL as the database engine
- Docker for containerization

The database schema is defined using SQLAlchemy's declarative syntax, ensuring consistency and maintainability across all layers.
