# Walmart ETL Project - Target Schema Documentation

This document provides a detailed explanation of the Target layer schema for the Walmart ETL project, including table structures and relationships.

## Overview

The Target layer represents the final dimensional model with proper dimension and fact tables. It implements a star schema design with:
- Dimension tables (prefixed with `tgt_dim_`)
- Fact tables (prefixed with `tgt_fact_`)

The schema includes Slowly Changing Dimension (SCD) Type 2 implementation for product and store dimensions to track historical changes.

## Dimension Tables

### tgt_dim_date

The Date dimension serves as a central reference for all time-based analysis.

**Structure:**
- `date_key` (PK): Surrogate key for the date dimension
- `date_id`: Original date identifier from source system
- `full_date`: Actual date value
- Time-related attributes: `day_of_week`, `day_of_month`, `month`, `month_name`, `quarter`, `year`
- `is_weekend`: Flag indicating if the date falls on a weekend
- `is_holiday`: Flag indicating if the date is a holiday
- `fiscal_year` and `fiscal_quarter`: For fiscal period analysis
- `insertion_date` and `modification_date`: Audit timestamps

**Relationships:**
- Referenced by `transaction_date_key`, `ship_date_key` in `tgt_fact_sales`
- Referenced by `date_key`, `last_restock_date_key` in `tgt_fact_inventory`
- Referenced by `return_date_key`, `original_sale_date_key` in `tgt_fact_returns`

### tgt_dim_customer

The Customer dimension contains customer demographic information.

**Structure:**
- `customer_key` (PK): Surrogate key for the customer dimension
- `customer_id`: Original customer identifier from source system
- Customer attributes: `customer_name`, `customer_age`, `age_group`, `customer_segment`
- Location attributes: `city`, `state`, `zip_code`, `region`
- `insertion_date` and `modification_date`: Audit timestamps

**Relationships:**
- Referenced by `customer_key` in `tgt_fact_sales`

### tgt_dim_supplier

The Supplier dimension contains information about product suppliers.

**Structure:**
- `supplier_key` (PK): Surrogate key for the supplier dimension
- `supplier_id`: Original supplier identifier from source system
- `supplier_name`: Name of the supplier
- `supplier_type`: Classification of supplier (Standard, Premium, etc.)
- Contact information: `contact_name`, `contact_phone`, `contact_email`
- `insertion_date` and `modification_date`: Audit timestamps

**Relationships:**
- Referenced by `supplier_key` in `tgt_dim_product`

### tgt_dim_return_reason

The Return Reason dimension categorizes the reasons for product returns.

**Structure:**
- `reason_key` (PK): Surrogate key for the return reason dimension
- `reason_code`: Original reason code from source system
- `reason_description`: Detailed description of the return reason
- `category`: Category of the return reason
- `reason_type`: Type classification of the return reason
- `insertion_date` and `modification_date`: Audit timestamps

**Relationships:**
- Referenced by `reason_key` in `tgt_fact_returns`

### tgt_dim_product (SCD Type 2)

The Product dimension implements SCD Type 2 to track historical changes to product attributes.

**Structure:**
- `product_key` (PK): Surrogate key for the product dimension
- `product_id`: Original product identifier from source system (not unique in this table due to SCD Type 2)
- Product attributes: `product_name`, `product_category`, `product_sub_category`, `product_container`
- Price attributes: `unit_price`, `price_tier`
- Margin attributes: `product_base_margin`, `margin_percentage`, `is_high_margin`
- `supplier_key` (FK): Reference to the supplier dimension
- SCD Type 2 attributes:
  - `is_current`: Flag indicating if this is the current version of the product
  - `start_date`: Date when this version became effective
  - `end_date`: Date when this version was superseded (null for current version)
  - `version`: Version number of the product record
- `insertion_date` and `modification_date`: Audit timestamps

**Relationships:**
- References `supplier_key` in `tgt_dim_supplier`
- Referenced by `product_key` in `tgt_fact_sales`, `tgt_fact_inventory`, and `tgt_fact_returns`

### tgt_dim_store (SCD Type 2)

The Store dimension implements SCD Type 2 to track historical changes to store attributes.

**Structure:**
- `store_key` (PK): Surrogate key for the store dimension
- `store_id`: Original store identifier from source system (not unique in this table due to SCD Type 2)
- Store attributes: `store_name`, `location`
- Location attributes: `city`, `state`, `zip_code`, `region`, `market`
- SCD Type 2 attributes:
  - `is_current`: Flag indicating if this is the current version of the store
  - `start_date`: Date when this version became effective
  - `end_date`: Date when this version was superseded (null for current version)
  - `version`: Version number of the store record
- `insertion_date` and `modification_date`: Audit timestamps

**Relationships:**
- Referenced by `store_key` in `tgt_fact_sales`, `tgt_fact_inventory`, and `tgt_fact_returns`

## Fact Tables

### tgt_fact_sales

The Sales fact table contains transactional sales data.

**Structure:**
- `sales_key` (PK): Surrogate key for the sales fact
- `sale_id`: Original sale identifier from source system
- `order_id`: Identifier for the order
- Dimension keys:
  - `transaction_date_key` (FK): Reference to the date dimension
  - `product_key` (FK): Reference to the product dimension
  - `store_key` (FK): Reference to the store dimension
  - `customer_key` (FK): Reference to the customer dimension
- Order attributes: `order_priority`, `order_quantity`
- Financial measures:
  - `sales_amount`: Total sales amount
  - `discount`: Discount percentage
  - `discount_amount`: Calculated discount amount
  - `shipping_cost`: Cost of shipping
  - `gross_revenue`: Revenue before discounts
  - `net_revenue`: Revenue after discounts
  - `profit`: Net profit
  - `profit_margin`: Profit as a percentage of sales
  - `is_profitable`: Flag indicating if the sale was profitable
- Shipping attributes:
  - `ship_date_key` (FK): Reference to the date dimension
  - `ship_mode`: Shipping method used
- `insertion_date` and `modification_date`: Audit timestamps

**Relationships:**
- References `transaction_date_key` and `ship_date_key` in `tgt_dim_date`
- References `product_key` in `tgt_dim_product`
- References `store_key` in `tgt_dim_store`
- References `customer_key` in `tgt_dim_customer`

### tgt_fact_inventory

The Inventory fact table contains inventory level data.

**Structure:**
- `inventory_key` (PK): Surrogate key for the inventory fact
- `inventory_id`: Original inventory identifier from source system
- Dimension keys:
  - `date_key` (FK): Reference to the date dimension
  - `product_key` (FK): Reference to the product dimension
  - `store_key` (FK): Reference to the store dimension
- Inventory measures:
  - `stock_level`: Current stock level
  - `min_stock_level`: Minimum stock level threshold
  - `max_stock_level`: Maximum stock level threshold
  - `reorder_point`: Stock level at which to reorder
- `last_restock_date_key` (FK): Reference to the date dimension
- Derived measures:
  - `days_of_supply`: Estimated days of inventory supply
  - `stock_status`: Status indicator (In Stock, Low Stock, Out of Stock)
  - `is_in_stock`: Flag indicating if the item is in stock
- `insertion_date` and `modification_date`: Audit timestamps

**Relationships:**
- References `date_key` and `last_restock_date_key` in `tgt_dim_date`
- References `product_key` in `tgt_dim_product`
- References `store_key` in `tgt_dim_store`

### tgt_fact_returns

The Returns fact table contains product return data.

**Structure:**
- `return_key` (PK): Surrogate key for the return fact
- `return_id`: Original return identifier from source system
- Dimension keys:
  - `return_date_key` (FK): Reference to the date dimension
  - `product_key` (FK): Reference to the product dimension
  - `store_key` (FK): Reference to the store dimension
  - `reason_key` (FK): Reference to the return reason dimension
- `reason_code`: Original reason code from source system
- Return measures:
  - `return_amount`: Total amount returned
  - `quantity_returned`: Quantity of items returned
  - `avg_return_price`: Average price per returned item
- Original sale attributes:
  - `original_sale_id`: Reference to the original sale
  - `original_sale_date_key` (FK): Reference to the date dimension
- Derived measures:
  - `days_since_sale`: Number of days between sale and return
  - `is_within_30_days`: Flag indicating if return was within 30 days of sale
- `return_condition`: Condition of the returned item
- `insertion_date` and `modification_date`: Audit timestamps

**Relationships:**
- References `return_date_key` and `original_sale_date_key` in `tgt_dim_date`
- References `product_key` in `tgt_dim_product`
- References `store_key` in `tgt_dim_store`
- References `reason_key` in `tgt_dim_return_reason`

## Key Design Features

1. **Star Schema Design**: Dimension tables connect to fact tables through surrogate keys, enabling efficient querying and analysis.

2. **SCD Type 2 Implementation**: The product and store dimensions implement SCD Type 2, allowing historical tracking of changes with effective dating.

3. **Denormalized Dimensions**: Dimensions contain denormalized attributes to reduce the need for joins in analytical queries.

4. **Audit Columns**: All tables include insertion and modification timestamps for data lineage tracking.

5. **Default Values**: Critical columns have default values to ensure data quality and prevent null values in important attributes.

6. **Derived Measures**: The schema includes pre-calculated measures to improve query performance for common analytical needs.
