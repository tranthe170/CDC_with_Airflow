-- Create a database (if not already created)
CREATE DATABASE sales_db;

-- Connect to the database
\c sales_db;

-- Create the tables
-- Table: sales
CREATE TABLE sales (
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    location_id VARCHAR(50),
    sales_person_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date DATE,
    quantity INT,
    price NUMERIC,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Table: customers
CREATE TABLE customers (
    customer_id VARCHAR(50),
    customer_name VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Table: locations
CREATE TABLE locations (
    location_id VARCHAR(50),
    name VARCHAR(255),
    county VARCHAR(255),
    state_code VARCHAR(10),
    state VARCHAR(255),
    type VARCHAR(50),
    latitude NUMERIC,
    longitude NUMERIC,
    area_code INT,
    population INT,
    households INT,
    median_income NUMERIC,
    land_area NUMERIC,
    water_area NUMERIC,
    time_zone VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Table: products
CREATE TABLE products (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    cost VARCHAR(50),
    original_sale_price VARCHAR(50),
    discount VARCHAR(50),
    current_price VARCHAR(50),
    taxes VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Load data from CSV files
-- Use the mounted folder for CSV data
COPY sales (order_id, product_id, location_id, sales_person_id, customer_id, order_date, quantity, price, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/data/sales.csv' DELIMITER ',' CSV HEADER;

COPY customers (customer_id, customer_name, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/data/customers.csv' DELIMITER ',' CSV HEADER;

COPY locations (location_id, name, county, state_code, state, type, latitude, longitude, area_code, population, households, median_income, land_area, water_area, time_zone, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/data/locations.csv' DELIMITER ',' CSV HEADER;

COPY products (product_id, product_name, cost, original_sale_price, discount, current_price, taxes, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/data/products.csv' DELIMITER ',' CSV HEADER;

-- Verify the data has been loaded
SELECT * FROM sales LIMIT 10;
SELECT * FROM customers LIMIT 10;
SELECT * FROM locations LIMIT 10;
SELECT * FROM products LIMIT 10;
