-- ODS Layer Tables
-- These tables store raw data as-is without any transformations or business logic

-- Date dimension in ODS layer
CREATE TABLE ods_date (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    day_of_month INT,
    month INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_holiday BOOLEAN,
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer dimension in ODS layer
CREATE TABLE ods_customer (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    customer_since DATE,
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Supplier dimension in ODS layer
CREATE TABLE ods_supplier (
    supplier_id VARCHAR(20) PRIMARY KEY,
    supplier_name VARCHAR(100),
    contact_person VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    contract_start_date DATE,
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Return reason dimension in ODS layer
CREATE TABLE ods_return_reason (
    reason_code VARCHAR(20) PRIMARY KEY,
    reason_description VARCHAR(200),
    category VARCHAR(50),
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product dimension in ODS layer
CREATE TABLE ods_product (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2),
    supplier_id VARCHAR(20),
    effective_date DATE,
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Store dimension in ODS layer
CREATE TABLE ods_store (
    store_id VARCHAR(20) PRIMARY KEY,
    store_name VARCHAR(100),
    location VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    store_size_sqft INT,
    effective_date DATE,
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sales fact in ODS layer
CREATE TABLE ods_sales (
    sale_id VARCHAR(20) PRIMARY KEY,
    transaction_date DATE,
    product_id VARCHAR(20),
    store_id VARCHAR(20),
    customer_id VARCHAR(20),
    sales_amount DECIMAL(12,2),
    quantity_sold INT,
    unit_price DECIMAL(10,2),
    total_cost DECIMAL(12,2),
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory fact in ODS layer
CREATE TABLE ods_inventory (
    inventory_id VARCHAR(20) PRIMARY KEY,
    inventory_date DATE,
    product_id VARCHAR(20),
    store_id VARCHAR(20),
    stock_level INT,
    min_stock_level INT,
    max_stock_level INT,
    reorder_point INT,
    last_restock_date DATE,
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Returns fact in ODS layer
CREATE TABLE ods_returns (
    return_id VARCHAR(20) PRIMARY KEY,
    return_date DATE,
    product_id VARCHAR(20),
    store_id VARCHAR(20),
    reason_code VARCHAR(20),
    return_amount DECIMAL(12,2),
    quantity_returned INT,
    original_sale_id VARCHAR(20),
    return_condition VARCHAR(50),
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

