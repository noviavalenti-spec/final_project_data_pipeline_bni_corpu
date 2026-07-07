-- Transform staging data to DW dimension and fact tables

-- Load dim_date
INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, month_name, day_name, is_holiday)
SELECT 
    date_id as date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    day_name,
    is_holiday
FROM staging.dim_date
ON CONFLICT (date_key) DO NOTHING;

-- Load dim_customer
INSERT INTO dw.dim_customer (
    customer_key, customer_id, full_name, gender, segment, 
    city, province, credit_score, salary
)
SELECT 
    customer_id as customer_key,
    customer_id,
    full_name,
    gender,
    segment,
    city,
    province,
    credit_score,
    salary
FROM staging.customers
ON CONFLICT (customer_key) DO NOTHING;

-- Load dim_account
INSERT INTO dw.dim_account (
    account_key, account_id, account_no, account_type, 
    product_name, currency, status, customer_key
)
SELECT 
    a.account_id as account_key,
    a.account_id,
    a.account_no,
    a.account_type,
    a.product_name,
    a.currency,
    a.status,
    a.customer_id as customer_key
FROM staging.accounts a
ON CONFLICT (account_key) DO NOTHING;

-- Load dim_branch
INSERT INTO dw.dim_branch (
    branch_key, branch_id, branch_code, branch_name, 
    city, province, region, branch_type
)
SELECT 
    branch_id as branch_key,
    branch_id,
    branch_code,
    branch_name,
    city,
    province,
    region,
    branch_type
FROM staging.branches
ON CONFLICT (branch_key) DO NOTHING;

-- Load dim_channel
INSERT INTO dw.dim_channel (
    channel_key, channel_id, channel_code, channel_name,
    channel_category, is_digital
)
SELECT 
    channel_id as channel_key,
    channel_id,
    channel_code,
    channel_name,
    channel_category,
    is_digital
FROM staging.channels
ON CONFLICT (channel_key) DO NOTHING;

-- Load fact_transactions
INSERT INTO dw.fact_transactions (
    transaction_id, date_key, customer_key, account_key, 
    branch_key, channel_key, amount, transaction_type, 
    status, is_fraud, fraud_type
)
SELECT 
    t.transaction_id,
    EXTRACT(YEAR FROM t.transaction_date) * 10000 + 
    EXTRACT(MONTH FROM t.transaction_date) * 100 + 
    EXTRACT(DAY FROM t.transaction_date) as date_key,
    t.customer_id as customer_key,
    t.account_id as account_key,
    t.branch_id as branch_key,
    t.channel_id as channel_key,
    t.amount,
    t.type as transaction_type,
    t.status,
    COALESCE(f.is_fraud, 0) as is_fraud,
    f.fraud_type
FROM staging.transactions t
LEFT JOIN staging.fraud_labels f ON t.transaction_id = f.transaction_id
ON CONFLICT (transaction_id) DO NOTHING;