-- Staging Layer Tables
-- These tables apply transformations and business logic to the ODS data

-- Date dimension in staging layer
CREATE TABLE stg_date (
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
    etl_batch_id VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer dimension in staging layer
CREATE TABLE stg_customer (
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
    etl_batch_id VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Supplier dimension in staging layer
CREATE TABLE stg_supplier (
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
    etl_batch_id VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Return reason dimension in staging layer
CREATE TABLE stg_return_reason (
    return_reason_key INT PRIMARY KEY,
    reason_code VARCHAR(20) NOT NULL,
    reason_description VARCHAR(200),
    category VARCHAR(50),
    impact_level VARCHAR(20),
    is_controllable BOOLEAN,
    etl_batch_id VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product dimension in staging layer
CREATE TABLE stg_product (
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
    current_flag CHAR(1),
    etl_batch_id VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Store dimension in staging layer
CREATE TABLE stg_store (
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
    current_flag CHAR(1),
    etl_batch_id VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sales fact in staging layer
CREATE TABLE stg_sales (
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
    etl_batch_id VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inventory fact in staging layer
CREATE TABLE stg_inventory (
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
    etl_batch_id VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Returns fact in staging layer
CREATE TABLE stg_returns (
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
    etl_batch_id VARCHAR(50),
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SCD Type 2 Management Procedures for Product dimension - Staging to Target
CREATE PROCEDURE usp_stg_update_product_scd2 (
    IN p_batch_id VARCHAR(50)
)
BEGIN
    DECLARE v_effective_date DATE;
    DECLARE v_new_product_key INT;
    DECLARE v_max_date DATE DEFAULT '9999-12-31';
    
    -- Set current date as effective date
    SET v_effective_date = CURRENT_DATE();
    
    -- Process each product in staging that needs to be inserted or updated in target
    -- First handle existing products that have changed (SCD Type 2)
    FOR product_rec IN (
        SELECT s.* 
        FROM stg_product s
        JOIN tgt_dim_product t ON s.product_id = t.product_id AND t.current_indicator = TRUE
        WHERE (
            s.product_name != t.product_name OR
            s.category != t.category OR
            s.subcategory != t.subcategory OR
            s.department != t.department OR
            s.brand != t.brand OR
            s.price != t.price OR
            s.cost != t.cost OR
            s.supplier_key != t.supplier_key OR
            s.is_active != t.is_active
        )
        AND s.etl_batch_id = p_batch_id
    ) DO
        -- Get the next available product_key in target
        SELECT COALESCE(MAX(product_key), 0) + 1 INTO v_new_product_key FROM tgt_dim_product;
        
        -- Update current record in target to set expiry date and current indicator
        UPDATE tgt_dim_product
        SET expiry_date = v_effective_date - INTERVAL 1 DAY,
            current_indicator = FALSE,
            dw_modified_date = CURRENT_TIMESTAMP
        WHERE product_id = product_rec.product_id
        AND current_indicator = TRUE;
        
        -- Insert new version in target
        INSERT INTO tgt_dim_product (
            product_key, product_id, product_name, category, subcategory, department, brand,
            price, cost, supplier_key, is_active, introduction_date, discontinuation_date,
            effective_date, expiry_date, current_indicator,
            dw_created_date, dw_version_number
        )
        VALUES (
            v_new_product_key, product_rec.product_id, product_rec.product_name, 
            product_rec.category, product_rec.subcategory, product_rec.department, product_rec.brand,
            product_rec.price, product_rec.cost, product_rec.supplier_key, product_rec.is_active,
            product_rec.introduction_date, product_rec.discontinuation_date,
            v_effective_date, v_max_date, TRUE,
            CURRENT_TIMESTAMP, 1
        );
    END FOR;
    
    -- Insert new products that don't exist in target yet
    INSERT INTO tgt_dim_product (
        product_key, product_id, product_name, category, subcategory, department, brand,
        price, cost, supplier_key, is_active, introduction_date, discontinuation_date,
        effective_date, expiry_date, current_indicator,
        dw_created_date, dw_version_number
    )
    SELECT 
        (SELECT COALESCE(MAX(product_key), 0) FROM tgt_dim_product) + ROW_NUMBER() OVER (ORDER BY s.product_id),
        s.product_id, s.product_name, s.category, s.subcategory, s.department, s.brand,
        s.price, s.cost, s.supplier_key, s.is_active, s.introduction_date, s.discontinuation_date,
        v_effective_date, v_max_date, TRUE,
        CURRENT_TIMESTAMP, 1
    FROM stg_product s
    LEFT JOIN tgt_dim_product t ON s.product_id = t.product_id
    WHERE t.product_id IS NULL
    AND s.etl_batch_id = p_batch_id;
    
    -- Mark processed records in staging
    UPDATE stg_product
    SET etl_timestamp = CURRENT_TIMESTAMP
    WHERE etl_batch_id = p_batch_id;
END;

-- SCD Type 2 Management Procedures for Store dimension - Staging to Target
CREATE PROCEDURE usp_stg_update_store_scd2 (
    IN p_batch_id VARCHAR(50)
)
BEGIN
    DECLARE v_effective_date DATE;
    DECLARE v_new_store_key INT;
    DECLARE v_max_date DATE DEFAULT '9999-12-31';
    
    -- Set current date as effective date
    SET v_effective_date = CURRENT_DATE();
    
    -- Process each store in staging that needs to be inserted or updated in target
    -- First handle existing stores that have changed (SCD Type 2)
    FOR store_rec IN (
        SELECT s.* 
        FROM stg_store s
        JOIN tgt_dim_store t ON s.store_id = t.store_id AND t.current_indicator = TRUE
        WHERE (
            s.store_name != t.store_name OR
            s.store_type != t.store_type OR
            s.location != t.location OR
            s.address != t.address OR
            s.city != t.city OR
            s.state != t.state OR
            s.zip_code != t.zip_code OR
            s.region != t.region OR
            s.country != t.country OR
            s.store_size_sqft != t.store_size_sqft
        )
        AND s.etl_batch_id = p_batch_id
    ) DO
        -- Get the next available store_key in target
        SELECT COALESCE(MAX(store_key), 0) + 1 INTO v_new_store_key FROM tgt_dim_store;
        
        -- Update current record in target to set expiry date and current indicator
        UPDATE tgt_dim_store
        SET expiry_date = v_effective_date - INTERVAL 1 DAY,
            current_indicator = FALSE,
            dw_modified_date = CURRENT_TIMESTAMP
        WHERE store_id = store_rec.store_id
        AND current_indicator = TRUE;
        
        -- Insert new version in target
        INSERT INTO tgt_dim_store (
            store_key, store_id, store_name, store_type, location, address, city, state, zip_code,
            region, country, store_size_sqft, opening_date, remodeling_date, 
            effective_date, expiry_date, current_indicator,
            dw_created_date, dw_version_number
        )
        VALUES (
            v_new_store_key, store_rec.store_id, store_rec.store_name, store_rec.store_type, 
            store_rec.location, store_rec.address, store_rec.city, store_rec.state, store_rec.zip_code,
            store_rec.region, store_rec.country, store_rec.store_size_sqft, 
            store_rec.opening_date, store_rec.remodeling_date,
            v_effective_date, v_max_date, TRUE,
            CURRENT_TIMESTAMP, 1
        );
    END FOR;
    
    -- Insert new stores that don't exist in target yet
    INSERT INTO tgt_dim_store (
        store_key, store_id, store_name, store_type, location, address, city, state, zip_code,
        region, country, store_size_sqft, opening_date, remodeling_date, 
        effective_date, expiry_date, current_indicator,
        dw_created_date, dw_version_number
    )
    SELECT 
        (SELECT COALESCE(MAX(store_key), 0) FROM tgt_dim_store) + ROW_NUMBER() OVER (ORDER BY s.store_id),
        s.store_id, s.store_name, s.store_type, s.location, s.address, s.city, s.state, s.zip_code,
        s.region, s.country, s.store_size_sqft, s.opening_date, s.remodeling_date,
        v_effective_date, v_max_date, TRUE,
        CURRENT_TIMESTAMP, 1
    FROM stg_store s
    LEFT JOIN tgt_dim_store t ON s.store_id = t.store_id
    WHERE t.store_id IS NULL
    AND s.etl_batch_id = p_batch_id;
    
    -- Mark processed records in staging
    UPDATE stg_store
    SET etl_timestamp = CURRENT_TIMESTAMP
    WHERE etl_batch_id = p_batch_id;
END;