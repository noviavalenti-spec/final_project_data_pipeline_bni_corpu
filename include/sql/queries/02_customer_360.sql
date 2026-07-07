-- ============================================================================
-- 02 CUSTOMER 360
-- Profil nasabah, segmentasi, dan perilaku transaksi
-- ============================================================================

-- 2.1 Top 10 Nasabah Paling Aktif (Berdasarkan Frekuensi)
SELECT 
    c.customer_id,
    c.full_name,
    c.segment,
    c.city,
    c.province,
    c.gender,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    MIN(ft.amount) as min_transaction,
    MAX(ft.amount) as max_transaction,
    COUNT(DISTINCT ft.account_key) as total_accounts,
    COUNT(DISTINCT ft.branch_key) as branches_visited,
    MAX(d.full_date) as last_transaction_date
FROM dw.fact_transactions ft
JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY c.customer_id, c.full_name, c.segment, c.city, c.province, c.gender
ORDER BY total_transactions DESC
LIMIT 10;

-- 2.2 Top 10 Nasabah Berdasarkan Nilai Transaksi
SELECT 
    c.customer_id,
    c.full_name,
    c.segment,
    c.salary,
    c.credit_score,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.account_key) as total_accounts,
    CASE 
        WHEN SUM(ft.amount) > 100000000 THEN 'Platinum'
        WHEN SUM(ft.amount) > 50000000 THEN 'Gold'
        WHEN SUM(ft.amount) > 10000000 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_value_tier
FROM dw.fact_transactions ft
JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
GROUP BY c.customer_id, c.full_name, c.segment, c.salary, c.credit_score
ORDER BY total_amount DESC
LIMIT 10;

-- 2.3 Distribusi Nasabah per Segmen
SELECT 
    c.segment,
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT ft.transaction_id) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    ROUND(AVG(c.salary), 0) as avg_salary,
    ROUND(AVG(c.credit_score), 0) as avg_credit_score,
    ROUND(COUNT(DISTINCT ft.transaction_id) * 100.0 / 
          NULLIF(SUM(COUNT(DISTINCT ft.transaction_id)) OVER (), 0), 2) as transaction_share_pct
FROM dw.dim_customer c
LEFT JOIN dw.fact_transactions ft ON c.customer_key = ft.customer_key
GROUP BY c.segment
ORDER BY total_customers DESC;

-- 2.4 Nasabah Berdasarkan Distribusi Geografis (Provinsi)
SELECT 
    c.province,
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT ft.transaction_id) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    ROUND(SUM(ft.amount) / NULLIF(COUNT(DISTINCT c.customer_id), 0), 0) as revenue_per_customer
FROM dw.dim_customer c
LEFT JOIN dw.fact_transactions ft ON c.customer_key = ft.customer_key
GROUP BY c.province
ORDER BY total_customers DESC;

-- 2.5 Nasabah Berdasarkan Kota Top 20
SELECT 
    c.province,
    c.city,
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT ft.transaction_id) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction
FROM dw.dim_customer c
LEFT JOIN dw.fact_transactions ft ON c.customer_key = ft.customer_key
GROUP BY c.province, c.city
ORDER BY total_customers DESC
LIMIT 20;

-- 2.6 Customer Activity Segmentation (RFM Analysis - Simple)
WITH customer_stats AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.segment,
        COUNT(*) as frequency,
        SUM(ft.amount) as monetary,
        MAX(d.full_date) as last_transaction,
        CURRENT_DATE - MAX(d.full_date) as recency_days
    FROM dw.fact_transactions ft
    JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    GROUP BY c.customer_id, c.full_name, c.segment
)
SELECT 
    segment,
    COUNT(*) as customer_count,
    ROUND(AVG(frequency), 0) as avg_frequency,
    ROUND(AVG(monetary), 0) as avg_monetary,
    ROUND(AVG(recency_days), 0) as avg_recency_days,
    CASE 
        WHEN AVG(frequency) > 50 AND AVG(monetary) > 50000000 THEN 'Champions'
        WHEN AVG(frequency) > 30 AND AVG(monetary) > 20000000 THEN 'Loyal Customers'
        WHEN AVG(recency_days) < 30 THEN 'Recent Customers'
        WHEN AVG(recency_days) > 90 THEN 'At Risk'
        ELSE 'Regular'
    END as customer_category
FROM customer_stats
GROUP BY segment;

-- 2.7 Nasabah Baru vs Existing (per bulan)
WITH monthly_customers AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        ft.customer_key,
        MIN(d.full_date) as first_transaction
    FROM dw.fact_transactions ft
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name, ft.customer_key
)
SELECT 
    year,
    month,
    month_name,
    COUNT(DISTINCT customer_key) as total_active_customers,
    COUNT(DISTINCT CASE 
        WHEN first_transaction >= DATE_TRUNC('month', first_transaction) 
        THEN customer_key 
    END) as new_customers,
    COUNT(DISTINCT CASE 
        WHEN first_transaction < DATE_TRUNC('month', first_transaction) 
        THEN customer_key 
    END) as existing_customers
FROM monthly_customers
GROUP BY year, month, month_name
ORDER BY year, month DESC;

-- 2.8 Gender Distribution
SELECT 
    c.gender,
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT ft.transaction_id) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    ROUND(AVG(c.salary), 0) as avg_salary
FROM dw.dim_customer c
LEFT JOIN dw.fact_transactions ft ON c.customer_key = ft.customer_key
GROUP BY c.gender;

-- 2.9 High Net Worth Individuals (HNWI)
SELECT 
    c.customer_id,
    c.full_name,
    c.segment,
    c.salary,
    c.credit_score,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_transaction_value,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT a.account_id) as total_accounts,
    STRING_AGG(DISTINCT a.product_name, ', ') as products_owned
FROM dw.dim_customer c
JOIN dw.fact_transactions ft ON c.customer_key = ft.customer_key
JOIN dw.dim_account a ON c.customer_key = a.customer_key
WHERE c.salary > 50000000 OR c.segment = 'VIP'
GROUP BY c.customer_id, c.full_name, c.segment, c.salary, c.credit_score
ORDER BY total_transaction_value DESC
LIMIT 20;

-- 2.10 Customer Lifetime Value (CLV) Estimation
SELECT 
    c.segment,
    COUNT(DISTINCT c.customer_id) as total_customers,
    ROUND(AVG(
        (SUM(ft.amount) / NULLIF(COUNT(DISTINCT d.year || '-' || d.month), 0)) * 12 * 3
    ), 0) as estimated_3year_clv,
    ROUND(AVG(SUM(ft.amount) / NULLIF(COUNT(DISTINCT d.year || '-' || d.month), 0)), 0) as avg_monthly_value,
    ROUND(AVG(COUNT(ft.transaction_id)), 0) as avg_total_transactions
FROM dw.dim_customer c
JOIN dw.fact_transactions ft ON c.customer_key = ft.customer_key
JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY c.segment
ORDER BY estimated_3year_clv DESC;