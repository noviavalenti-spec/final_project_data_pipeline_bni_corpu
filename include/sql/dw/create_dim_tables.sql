-- Create DW schema
CREATE SCHEMA IF NOT EXISTS dw;

-- Drop dimension tables if exist
DROP TABLE IF EXISTS dw.dim_date;
DROP TABLE IF EXISTS dw.dim_customer;
DROP TABLE IF EXISTS dw.dim_account;
DROP TABLE IF EXISTS dw.dim_branch;
DROP TABLE IF EXISTS dw.dim_channel;

-- Create dimension tables
CREATE TABLE dw.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_holiday INTEGER DEFAULT 0
);

CREATE TABLE dw.dim_customer (
    customer_key INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    full_name VARCHAR(100) NOT NULL,
    gender VARCHAR(20),
    segment VARCHAR(20),
    city VARCHAR(50),
    province VARCHAR(50),
    credit_score INTEGER,
    salary DECIMAL(15,2),
    effective_date DATE DEFAULT CURRENT_DATE,
    is_current INTEGER DEFAULT 1
);

CREATE TABLE dw.dim_account (
    account_key INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL,
    account_no VARCHAR(20) NOT NULL,
    account_type VARCHAR(20),
    product_name VARCHAR(50),
    currency VARCHAR(10) DEFAULT 'IDR',
    status VARCHAR(20),
    customer_key INTEGER REFERENCES dw.dim_customer(customer_key),
    effective_date DATE DEFAULT CURRENT_DATE,
    is_current INTEGER DEFAULT 1
);

CREATE TABLE dw.dim_branch (
    branch_key INTEGER PRIMARY KEY,
    branch_id INTEGER NOT NULL,
    branch_code VARCHAR(10) NOT NULL,
    branch_name VARCHAR(100) NOT NULL,
    city VARCHAR(50),
    province VARCHAR(50),
    region VARCHAR(50),
    branch_type VARCHAR(50)
);

CREATE TABLE dw.dim_channel (
    channel_key INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    channel_code VARCHAR(10) NOT NULL,
    channel_name VARCHAR(50) NOT NULL,
    channel_category VARCHAR(50),
    is_digital INTEGER DEFAULT 0
);

-- Create indexes for dimension tables
CREATE INDEX idx_dim_customer_segment ON dw.dim_customer(segment);
CREATE INDEX idx_dim_customer_city ON dw.dim_customer(city);
CREATE INDEX idx_dim_account_product ON dw.dim_account(product_name);
CREATE INDEX idx_dim_account_status ON dw.dim_account(status);
CREATE INDEX idx_dim_branch_region ON dw.dim_branch(region);
CREATE INDEX idx_dim_channel_category ON dw.dim_channel(channel_category);

-- Create fact table