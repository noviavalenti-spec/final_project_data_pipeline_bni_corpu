-- ============================================================================
-- 03 BRANCH PERFORMANCE
-- Performa cabang berdasarkan jumlah dan nilai transaksi per region
-- ============================================================================

-- 3.1 Ranking Cabang Berdasarkan Performa
SELECT 
    b.branch_id,
    b.branch_code,
    b.branch_name,
    b.city,
    b.province,
    b.region,
    b.branch_type,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    COUNT(DISTINCT ft.account_key) as active_accounts,
    RANK() OVER (ORDER BY COUNT(*) DESC) as rank_by_volume,
    RANK() OVER (ORDER BY SUM(ft.amount) DESC) as rank_by_value,
    ROUND(SUM(ft.amount) / COUNT(*), 0) as revenue_per_transaction
FROM dw.fact_transactions ft
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY b.branch_id, b.branch_code, b.branch_name, b.city, b.province, b.region, b.branch_type
ORDER BY total_transactions DESC;

-- 3.2 Performa Berdasarkan Region
SELECT 
    b.region,
    COUNT(DISTINCT b.branch_id) as total_branches,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT b.branch_id), 0) as revenue_per_branch,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as transaction_share_pct
FROM dw.fact_transactions ft
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY b.region
ORDER BY total_amount DESC;

-- 3.3 Performa Berdasarkan Provinsi
SELECT 
    b.province,
    b.region,
    COUNT(DISTINCT b.branch_id) as total_branches,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(SUM(ft.amount) / NULLIF(COUNT(*), 0), 0) as revenue_per_transaction
FROM dw.fact_transactions ft
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY b.province, b.region
ORDER BY total_amount DESC;

-- 3.4 Analisis Berdasarkan Tipe Cabang
SELECT 
    b.branch_type,
    COUNT(DISTINCT b.branch_id) as total_branches,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT b.branch_id), 0) as amount_per_branch,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as transaction_pct
FROM dw.fact_transactions ft
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY b.branch_type
ORDER BY total_amount DESC;

-- 3.5 Top 10 Cabang dengan Performa Tertinggi
SELECT 
    b.branch_code,
    b.branch_name,
    b.city,
    b.province,
    b.branch_type,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as active_customers,
    ROUND(SUM(ft.amount) / COUNT(*), 0) as revenue_per_txn,
    RANK() OVER (ORDER BY SUM(ft.amount) DESC) as revenue_rank
FROM dw.fact_transactions ft
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY b.branch_code, b.branch_name, b.city, b.province, b.branch_type
ORDER BY total_amount DESC
LIMIT 10;

-- 3.6 Bottom 10 Cabang (Perlu Perhatian)
SELECT 
    b.branch_code,
    b.branch_name,
    b.city,
    b.province,
    b.branch_type,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as active_customers,
    RANK() OVER (ORDER BY SUM(ft.amount) ASC) as revenue_rank
FROM dw.fact_transactions ft
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY b.branch_code, b.branch_name, b.city, b.province, b.branch_type
ORDER BY total_amount ASC
LIMIT 10;

-- 3.7 Tren Performa Cabang per Bulan
SELECT 
    b.branch_code,
    b.branch_name,
    d.year,
    d.month,
    d.month_name,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    COUNT(DISTINCT ft.customer_key) as unique_customers
FROM dw.fact_transactions ft
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY b.branch_code, b.branch_name, d.year, d.month, d.month_name
ORDER BY b.branch_code, d.year, d.month;

-- 3.8 Branch Efficiency Metrics
SELECT 
    b.branch_code,
    b.branch_name,
    b.city,
    b.branch_type,
    COUNT(DISTINCT ft.customer_key) as total_customers,
    COUNT(*) as total_transactions,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT ft.customer_key), 2) as txn_per_customer,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT ft.customer_key), 0) as revenue_per_customer,
    ROUND(AVG(ft.amount), 0) as avg_transaction_size
FROM dw.fact_transactions ft
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY b.branch_code, b.branch_name, b.city, b.branch_type
HAVING COUNT(DISTINCT ft.customer_key) > 10
ORDER BY txn_per_customer DESC;

-- 3.9 Regional Comparison (Urban vs Suburban vs Rural)
SELECT 
    b.region,
    b.branch_type,
    COUNT(DISTINCT b.branch_id) as branch_count,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    ROUND(AVG(ft.amount), 0) as avg_transaction,
    ROUND(SUM(ft.amount) / COUNT(*), 0) as revenue_per_txn,
    COUNT(DISTINCT ft.customer_key) as total_customers
FROM dw.fact_transactions ft
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY b.region, b.branch_type
ORDER BY b.region, total_amount DESC;

-- 3.10 Branch Growth Rate (Month over Month)
WITH monthly_branch AS (
    SELECT 
        b.branch_code,
        b.branch_name,
        d.year,
        d.month,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount
    FROM dw.fact_transactions ft
    JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    GROUP BY b.branch_code, b.branch_name, d.year, d.month
)
SELECT 
    branch_code,
    branch_name,
    year,
    month,
    total_transactions,
    total_amount,
    LAG(total_transactions) OVER (PARTITION BY branch_code ORDER BY year, month) as prev_month_txns,
    ROUND(
        (total_transactions - LAG(total_transactions) OVER (PARTITION BY branch_code ORDER BY year, month)) * 100.0 / 
        NULLIF(LAG(total_transactions) OVER (PARTITION BY branch_code ORDER BY year, month), 0), 2
    ) as growth_rate_pct
FROM monthly_branch
ORDER BY total_amount DESC
LIMIT 20;