-- ============================================================================
-- 05 PRODUCT PERFORMANCE
-- Performa produk rekening (Tabungan/Giro/Deposito)
-- ============================================================================

-- 5.1 Product Performance Summary
SELECT 
    a.product_name,
    a.account_type,
    COUNT(DISTINCT a.account_id) as total_accounts,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT a.account_id), 0) as amount_per_account,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as transaction_share_pct
FROM dw.fact_transactions ft
JOIN dw.dim_account a ON ft.account_key = a.account_key
GROUP BY a.product_name, a.account_type
ORDER BY total_amount DESC;

-- 5.2 Product Usage by Customer Segment
SELECT 
    a.product_name,
    c.segment,
    COUNT(DISTINCT a.account_id) as total_accounts,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(AVG(c.salary), 0) as avg_customer_salary,
    ROUND(AVG(c.credit_score), 0) as avg_credit_score
FROM dw.fact_transactions ft
JOIN dw.dim_account a ON ft.account_key = a.account_key
JOIN dw.dim_customer c ON a.customer_key = c.customer_key
GROUP BY a.product_name, c.segment
ORDER BY a.product_name, total_amount DESC;

-- 5.3 Top Products by Revenue
SELECT 
    a.product_name,
    COUNT(DISTINCT a.account_id) as total_accounts,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_transaction_value,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as active_customers,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT a.account_id), 0) as revenue_per_account,
    RANK() OVER (ORDER BY SUM(ft.amount) DESC) as revenue_rank,
    RANK() OVER (ORDER BY COUNT(*) DESC) as volume_rank
FROM dw.fact_transactions ft
JOIN dw.dim_account a ON ft.account_key = a.account_key
GROUP BY a.product_name
ORDER BY total_transaction_value DESC;

-- 5.4 Product Growth Trend (Monthly)
SELECT 
    d.year,
    d.month,
    d.month_name,
    a.product_name,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    COUNT(DISTINCT a.account_id) as active_accounts,
    COUNT(DISTINCT ft.customer_key) as active_customers
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
JOIN dw.dim_account a ON ft.account_key = a.account_key
GROUP BY d.year, d.month, d.month_name, a.product_name
ORDER BY d.year, d.month, a.product_name;

-- 5.5 Product Performance by Region
SELECT 
    b.region,
    a.product_name,
    COUNT(DISTINCT a.account_id) as total_accounts,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT a.account_id), 0) as amount_per_account
FROM dw.fact_transactions ft
JOIN dw.dim_account a ON ft.account_key = a.account_key
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY b.region, a.product_name
ORDER BY b.region, total_amount DESC;

-- 5.6 Account Status Distribution by Product
SELECT 
    a.product_name,
    a.status,
    COUNT(DISTINCT a.account_id) as total_accounts,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    ROUND(COUNT(DISTINCT a.account_id) * 100.0 / 
          SUM(COUNT(DISTINCT a.account_id)) OVER (PARTITION BY a.product_name), 2) as account_pct
FROM dw.fact_transactions ft
JOIN dw.dim_account a ON ft.account_key = a.account_key
GROUP BY a.product_name, a.status
ORDER BY a.product_name, total_accounts DESC;

-- 5.7 Product Cross-Selling Analysis (Customers with Multiple Products)
WITH customer_products AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.segment,
        COUNT(DISTINCT a.product_name) as product_count,
        STRING_AGG(DISTINCT a.product_name, ', ') as products_owned
    FROM dw.dim_customer c
    JOIN dw.dim_account a ON c.customer_key = a.customer_key
    GROUP BY c.customer_id, c.full_name, c.segment
)
SELECT 
    product_count,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as customer_pct
FROM customer_products
GROUP BY product_count
ORDER BY product_count;

-- 5.8 Average Balance Estimation by Product
SELECT 
    a.product_name,
    COUNT(DISTINCT a.account_id) as total_accounts,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_volume,
    ROUND(AVG(ft.amount), 0) as avg_transaction,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT a.account_id), 0) as estimated_avg_balance,
    ROUND(SUM(CASE WHEN ft.transaction_type = 'Credit' THEN ft.amount ELSE 0 END) / 
          NULLIF(COUNT(DISTINCT a.account_id), 0), 0) as estimated_avg_deposit
FROM dw.fact_transactions ft
JOIN dw.dim_account a ON ft.account_key = a.account_key
GROUP BY a.product_name
ORDER BY estimated_avg_balance DESC;

-- 5.9 Product Performance by Quarter
SELECT 
    d.year,
    d.quarter,
    a.product_name,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    COUNT(DISTINCT a.account_id) as active_accounts,
    COUNT(DISTINCT ft.customer_key) as active_customers,
    ROUND(SUM(ft.amount) / COUNT(*), 0) as avg_transaction_value
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
JOIN dw.dim_account a ON ft.account_key = a.account_key
GROUP BY d.year, d.quarter, a.product_name
ORDER BY d.year, d.quarter DESC, total_amount DESC;

-- 5.10 Deposit Product Analysis (Tabungan vs Giro vs Deposito)
SELECT 
    a.product_name,
    CASE 
        WHEN a.product_name = 'Tabungan' THEN 'Savings'
        WHEN a.product_name = 'Giro' THEN 'Checking'
        WHEN a.product_name = 'Deposito' THEN 'Time Deposit'
    END as product_category,
    COUNT(DISTINCT a.account_id) as total_accounts,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT a.account_id), 0) as avg_per_account,
    ROUND(COUNT(DISTINCT ft.customer_key) * 100.0 / 
          (SELECT COUNT(DISTINCT customer_key) FROM dw.fact_transactions), 2) as customer_penetration_pct
FROM dw.fact_transactions ft
JOIN dw.dim_account a ON ft.account_key = a.account_key
GROUP BY a.product_name
ORDER BY total_amount DESC;