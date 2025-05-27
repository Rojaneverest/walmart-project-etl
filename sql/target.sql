-- Target Layer Tables
-- These are the final tables where SCD is implemented with flags

-- Date dimension in target layer
CREATE TABLE tgt_dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(50),
    fiscal_year INT,
    fiscal_quarter INT,
    dw_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_modified_date TIMESTAMP,
    dw_version_number INT DEFAULT 1
);

-- Customer dimension in target layer
CREATE TABLE tgt_dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    region VARCHAR(50),
    country VARCHAR(50),
    customer_since DATE,
    customer_type VARCHAR(20),
    loyalty_segment VARCHAR(20),
    dw_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_modified_date TIMESTAMP,
    dw_version_number INT DEFAULT 1
);

-- Supplier dimension in target layer
CREATE TABLE tgt_dim_supplier (
    supplier_key INT PRIMARY KEY,
    supplier_id VARCHAR(20) NOT NULL,
    supplier_name VARCHAR(100),
    contact_person VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    contract_start_date DATE,
    contract_end_date DATE,
    supplier_rating INT,
    supplier_status VARCHAR(20),
    dw_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_modified_date TIMESTAMP,
    dw_version_number INT DEFAULT 1
);

-- Return reason dimension in target layer
CREATE TABLE tgt_dim_return_reason (
    return_reason_key INT PRIMARY KEY,
    reason_code VARCHAR(20) NOT NULL,
    reason_description VARCHAR(200),
    category VARCHAR(50),
    impact_level VARCHAR(20),
    is_controllable BOOLEAN,
    dw_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_modified_date TIMESTAMP,
    dw_version_number INT DEFAULT 1
);

-- Product dimension in target layer (SCD Type 2)
CREATE TABLE tgt_dim_product (
    product_key INT PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    department VARCHAR(50),
    brand VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    supplier_key INT,
    is_active BOOLEAN,
    introduction_date DATE,
    discontinuation_date DATE,
    effective_date DATE NOT NULL,
    expiry_date DATE,
    current_indicator BOOLEAN DEFAULT TRUE,
    dw_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_modified_date TIMESTAMP,
    dw_version_number INT DEFAULT 1,
    FOREIGN KEY (supplier_key) REFERENCES tgt_dim_supplier(supplier_key)
);

-- Store dimension in target layer (SCD Type 2)
CREATE TABLE tgt_dim_store (
    store_key INT PRIMARY KEY,
    store_id VARCHAR(20) NOT NULL,
    store_name VARCHAR(100),
    store_type VARCHAR(50),
    location VARCHAR(200),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    region VARCHAR(50),
    country VARCHAR(50),
    store_size_sqft INT,
    opening_date DATE,
    remodeling_date DATE,
    effective_date DATE NOT NULL,
    expiry_date DATE,
    current_indicator BOOLEAN DEFAULT TRUE,
    dw_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_modified_date TIMESTAMP,
    dw_version_number INT DEFAULT 1
);

-- Sales fact in target layer
CREATE TABLE tgt_fact_sales (
    sale_key INT PRIMARY KEY,
    sale_id VARCHAR(20) NOT NULL,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    customer_key INT NOT NULL,
    sales_amount DECIMAL(12,2) NOT NULL,
    quantity_sold INT NOT NULL,
    unit_price DECIMAL(10,2),
    total_cost DECIMAL(12,2),
    profit_margin DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    net_sales_amount DECIMAL(12,2),
    sales_channel VARCHAR(20),
    promotion_key INT,
    dw_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_modified_date TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES tgt_dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES tgt_dim_product(product_key),
    FOREIGN KEY (store_key) REFERENCES tgt_dim_store(store_key),
    FOREIGN KEY (customer_key) REFERENCES tgt_dim_customer(customer_key)
);

-- Inventory fact in target layer
CREATE TABLE tgt_fact_inventory (
    inventory_key INT PRIMARY KEY,
    inventory_id VARCHAR(20) NOT NULL,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    stock_level INT NOT NULL,
    min_stock_level INT,
    max_stock_level INT,
    reorder_point INT,
    last_restock_date_key INT,
    days_of_supply INT,
    stock_status VARCHAR(20),
    is_in_stock BOOLEAN,
    dw_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_modified_date TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES tgt_dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES tgt_dim_product(product_key),
    FOREIGN KEY (store_key) REFERENCES tgt_dim_store(store_key),
    FOREIGN KEY (last_restock_date_key) REFERENCES tgt_dim_date(date_key)
);

-- Returns fact in target layer
CREATE TABLE tgt_fact_returns (
    return_key INT PRIMARY KEY,
    return_id VARCHAR(20) NOT NULL,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    return_reason_key INT NOT NULL,
    return_amount DECIMAL(12,2) NOT NULL,
    quantity_returned INT NOT NULL,
    original_sale_id VARCHAR(20),
    original_sale_date_key INT,
    return_condition VARCHAR(50),
    days_since_purchase INT,
    refund_type VARCHAR(20),
    dw_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_modified_date TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES tgt_dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES tgt_dim_product(product_key),
    FOREIGN KEY (store_key) REFERENCES tgt_dim_store(store_key),
    FOREIGN KEY (return_reason_key) REFERENCES tgt_dim_return_reason(return_reason_key),
    FOREIGN KEY (original_sale_date_key) REFERENCES tgt_dim_date(date_key)
);

-- SCD Type 2 Management Procedures for Product dimension
CREATE PROCEDURE usp_update_product_scd2 (
    IN p_product_id VARCHAR(20),
    IN p_product_name VARCHAR(100),
    IN p_category VARCHAR(50),
    IN p_subcategory VARCHAR(50),
    IN p_department VARCHAR(50),
    IN p_brand VARCHAR(50),
    IN p_price DECIMAL(10,2),
    IN p_cost DECIMAL(10,2),
    IN p_supplier_key INT,
    IN p_is_active BOOLEAN,
    IN p_introduction_date DATE,
    IN p_discontinuation_date DATE
)
BEGIN
    DECLARE v_effective_date DATE;
    DECLARE v_new_product_key INT;
    
    -- Set current date as effective date
    SET v_effective_date = CURRENT_DATE();
    
    -- Get the next available Product_Key
    SELECT COALESCE(MAX(product_key), 0) + 1 INTO v_new_product_key FROM tgt_dim_product;
    
    -- Update current record to set expiry date and current indicator
    UPDATE tgt_dim_product
    SET expiry_date = v_effective_date - INTERVAL 1 DAY,
        current_indicator = FALSE,
        dw_modified_date = CURRENT_TIMESTAMP
    WHERE product_id = p_product_id
    AND current_indicator = TRUE;
    
    -- Insert new record
    INSERT INTO tgt_dim_product (
        product_key, product_id, product_name, category, subcategory, department, brand,
        price, cost, supplier_key, is_active, introduction_date, discontinuation_date,
        effective_date, expiry_date, current_indicator
    )
    VALUES (
        v_new_product_key, p_product_id, p_product_name, p_category, p_subcategory, p_department, p_brand,
        p_price, p_cost, p_supplier_key, p_is_active, p_introduction_date, p_discontinuation_date,
        v_effective_date, NULL, TRUE
    );
    
END;

-- SCD Type 2 Management Procedures for Store dimension
CREATE PROCEDURE usp_update_store_scd2 (
    IN p_store_id VARCHAR(20),
    IN p_store_name VARCHAR(100),
    IN p_store_type VARCHAR(50),
    IN p_location VARCHAR(200),
    IN p_address VARCHAR(200),
    IN p_city VARCHAR(50),
    IN p_state VARCHAR(50),
    IN p_zip_code VARCHAR(20),
    IN p_region VARCHAR(50),
    IN p_country VARCHAR(50),
    IN p_store_size_sqft INT,
    IN p_opening_date DATE,
    IN p_remodeling_date DATE
)
BEGIN
    DECLARE v_effective_date DATE;
    DECLARE v_new_store_key INT;
    
    -- Set current date as effective date
    SET v_effective_date = CURRENT_DATE();
    
    -- Get the next available Store_Key
    SELECT COALESCE(MAX(store_key), 0) + 1 INTO v_new_store_key FROM tgt_dim_store;
    
    -- Update current record to set expiry date and current indicator
    UPDATE tgt_dim_store
    SET expiry_date = v_effective_date - INTERVAL 1 DAY,
        current_indicator = FALSE,
        dw_modified_date = CURRENT_TIMESTAMP
    WHERE store_id = p_store_id
    AND current_indicator = TRUE;
    
    -- Insert new record
    INSERT INTO tgt_dim_store (
        store_key, store_id, store_name, store_type, location, address, city, state, zip_code,
        region, country, store_size_sqft, opening_date, remodeling_date, effective_date, expiry_date, current_indicator
    )
    VALUES (
        v_new_store_key, p_store_id, p_store_name, p_store_type, p_location, p_address, p_city, p_state, p_zip_code,
        p_region, p_country, p_store_size_sqft, p_opening_date, p_remodeling_date, v_effective_date, NULL, TRUE
    );
    
END;