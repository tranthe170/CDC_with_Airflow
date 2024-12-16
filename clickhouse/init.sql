-- ==========================
-- 1. Create Schemas (Databases)
-- ==========================
CREATE DATABASE IF NOT EXISTS stage;
CREATE DATABASE IF NOT EXISTS temp;
CREATE DATABASE IF NOT EXISTS data_model;

-- ==========================
-- 2. Staging Tables (Source Tables)
-- ==========================
-- Table: Customers
CREATE TABLE IF NOT EXISTS stage.customers
(
    customer_id String,
    customer_name String,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree()
ORDER BY customer_id;

-- Table: Products
CREATE TABLE IF NOT EXISTS stage.products
(
    product_id String,
    product_name String,
    cost String,
    original_sale_price String,
    discount String,
    current_price String,
    taxes String,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree()
ORDER BY product_id;

-- Table: Locations
CREATE TABLE IF NOT EXISTS stage.locations
(
    location_id String,
    name String,
    county String,
    state_code String,
    state String,
    type String,
    latitude Float64,
    longitude Float64,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree()
ORDER BY location_id;

-- Table: Sales
CREATE TABLE IF NOT EXISTS stage.sales
(
    order_id String,
    product_id String,
    location_id String,
    customer_id String,
    order_date Date,
    quantity Int32,
    price Float64,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree()
ORDER BY order_id;

-- ==========================
-- 3. Temporary Tables (ETL Processing)
-- ==========================
-- Table: Customers
DROP TABLE IF EXISTS temp.customers;

CREATE TABLE IF NOT EXISTS temp.customers
(
    etl_key String,
    customer_id String,
    customer_name String,
    etl_start_date Date,
    etl_end_date Date,
    etl_current_ind String,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (etl_key, customer_id);

-- Table: Products
DROP TABLE IF EXISTS temp.products;

CREATE TABLE IF NOT EXISTS temp.products
(
    etl_key String,
    product_id String,
    product_name String,
    cost String,
    original_sale_price String,
    discount String,
    current_price String,
    taxes String,
    etl_start_date Date,
    etl_end_date Date,
    etl_current_ind String,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (etl_key, product_id);

-- Table: Locations
DROP TABLE IF EXISTS temp.locations;

CREATE TABLE IF NOT EXISTS temp.locations
(
    etl_key String,
    location_id String,
    name String,
    county String,
    state_code String,
    state String,
    type String,
    latitude Float64,
    longitude Float64,
    etl_start_date Date,
    etl_end_date Date,
    etl_current_ind String,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (etl_key, location_id);

-- Table: Sales
DROP TABLE IF EXISTS temp.sales;

CREATE TABLE IF NOT EXISTS temp.sales
(
    etl_key String,
    order_id String,
    product_id String,
    location_id String,
    customer_id String,
    order_date Date,
    quantity Int32,
    price Float64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (etl_key, order_id);

-- ==========================
-- 4. Data Model Tables (Final Tables)
-- ==========================
-- Table: Customers
DROP TABLE IF EXISTS data_model.customers;

CREATE TABLE IF NOT EXISTS data_model.customers
(
    etl_key String,
    customer_id String,
    customer_name String,
    etl_start_date Date,
    etl_end_date Date,
    etl_current_ind String,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (etl_key, customer_id);

-- Table: Products
DROP TABLE IF EXISTS data_model.products;

CREATE TABLE IF NOT EXISTS data_model.products
(
    etl_key String,
    product_id String,
    product_name String,
    cost String,
    original_sale_price String,
    discount String,
    current_price String,
    taxes String,
    etl_start_date Date,
    etl_end_date Date,
    etl_current_ind String,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (etl_key, product_id);

-- Table: Locations
DROP TABLE IF EXISTS data_model.locations;

CREATE TABLE IF NOT EXISTS data_model.locations
(
    etl_key String,
    location_id String,
    name String,
    county String,
    state_code String,
    state String,
    type String,
    latitude Float64,
    longitude Float64,
    etl_start_date Date,
    etl_end_date Date,
    etl_current_ind String,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (etl_key, location_id);

-- Table: Sales
DROP TABLE IF EXISTS data_model.sales;

CREATE TABLE IF NOT EXISTS data_model.sales
(
    etl_key String,
    order_id String,
    product_id String,
    location_id String,
    customer_id String,
    order_date Date,
    quantity Int32,
    price Float64,
    etl_timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (etl_key, order_id);
