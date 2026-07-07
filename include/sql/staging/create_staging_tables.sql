-- Create staging schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Drop tables if exist
DROP TABLE IF EXISTS staging.transactions;
DROP TABLE IF EXISTS staging.customers;
DROP TABLE IF EXISTS staging.accounts;
DROP TABLE IF EXISTS staging.branches;
DROP TABLE IF EXISTS staging.channels;
DROP TABLE IF EXISTS staging.fraud_labels;
DROP TABLE IF EXISTS staging.dim_date;

-- Create staging tables
CREATE TABLE staging.transactions (
    transaction_id INTEGER,
    account_id INTEGER,
    customer_id INTEGER,
    branch_id INTEGER,
    channel_id INTEGER,
    amount DECIMAL(15,2),
    type VARCHAR(20),
    status VARCHAR(20),
    transaction_date TIMESTAMP
);

CREATE TABLE staging.customers (
    customer_id INTEGER,
    full_name VARCHAR(100),
    gender VARCHAR(20),
    segment VARCHAR(20),
    city VARCHAR(50),
    province VARCHAR(50),
    credit_score INTEGER,
    salary DECIMAL(15,2)
);

CREATE TABLE staging.accounts (
    account_id INTEGER,
    account_no VARCHAR(20),
    account_type VARCHAR(20),
    product_name VARCHAR(50),
    currency VARCHAR(10),
    status VARCHAR(20),
    customer_id INTEGER
);

CREATE TABLE staging.branches (
    branch_id INTEGER,
    branch_code VARCHAR(10),
    branch_name VARCHAR(100),
    city VARCHAR(50),
    province VARCHAR(50),
    region VARCHAR(50),
    branch_type VARCHAR(50)
);

CREATE TABLE staging.channels (
    channel_id INTEGER,
    channel_code VARCHAR(10),
    channel_name VARCHAR(50),
    channel_category VARCHAR(50),
    is_digital INTEGER
);

CREATE TABLE staging.fraud_labels (
    transaction_id INTEGER,
    is_fraud INTEGER,
    fraud_type VARCHAR(100),
    fraud_score DECIMAL(3,2),
    flagged_at TIMESTAMP
);

CREATE TABLE staging.dim_date (
    date_id INTEGER,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    is_holiday INTEGER
);