-- ============================================================================
-- 01 TRANSACTION ANALYTICS
-- Total volume dan nilai transaksi per hari, minggu, bulan
-- Tren pertumbuhan transaksi
-- ============================================================================

-- 1.1 Total Transaksi Per Hari (Last 30 Days)
SELECT 
    d.full_date,
    d.day_name,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    MIN(ft.amount) as min_transaction,
    MAX(ft.amount) as max_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date DESC;

-- 1.2 Total Transaksi Per Minggu
SELECT 
    d.year,
    EXTRACT(WEEK FROM d.full_date) as week_number,
    MIN(d.full_date) as week_start,
    MAX(d.full_date) as week_end,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY d.year, EXTRACT(WEEK FROM d.full_date)
ORDER BY d.year, week_number DESC;

-- 1.3 Total Transaksi Per Bulan
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    MIN(ft.amount) as min_transaction,
    MAX(ft.amount) as max_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(SUM(ft.amount) / COUNT(*), 0) as revenue_per_transaction
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month DESC;

-- 1.4 Tren Pertumbuhan Bulanan (Month-over-Month Growth)
WITH monthly_data AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        COUNT(DISTINCT ft.customer_key) as active_customers
    FROM dw.fact_transactions ft
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
)
SELECT 
    year,
    month,
    month_name,
    total_transactions,
    total_amount,
    active_customers,
    LAG(total_transactions) OVER (ORDER BY year, month) as prev_month_transactions,
    LAG(total_amount) OVER (ORDER BY year, month) as prev_month_amount,
    ROUND(
        (total_transactions - LAG(total_transactions) OVER (ORDER BY year, month)) * 100.0 / 
        NULLIF(LAG(total_transactions) OVER (ORDER BY year, month), 0), 2
    ) as transaction_growth_pct,
    ROUND(
        (total_amount - LAG(total_amount) OVER (ORDER BY year, month)) * 100.0 / 
        NULLIF(LAG(total_amount) OVER (ORDER BY year, month), 0), 2
    ) as amount_growth_pct
FROM monthly_data
ORDER BY year, month DESC;

-- 1.5 Transaksi Berdasarkan Tipe (Credit vs Debit)
SELECT 
    ft.transaction_type,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as transaction_percentage
FROM dw.fact_transactions ft
GROUP BY ft.transaction_type;

-- 1.6 Transaksi Berdasarkan Status
SELECT 
    ft.status,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM dw.fact_transactions ft
GROUP BY ft.status;

-- 1.7 Top 10 Hari dengan Transaksi Tertinggi
SELECT 
    d.full_date,
    d.day_name,
    d.month_name,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    COUNT(DISTINCT ft.customer_key) as unique_customers
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY d.full_date, d.day_name, d.month_name
ORDER BY total_transactions DESC
LIMIT 10;

-- 1.8 Rata-rata Transaksi per Jam (Peak Hours Analysis)
SELECT 
    EXTRACT(HOUR FROM ft.transaction_date) as hour_of_day,
    CASE 
        WHEN EXTRACT(HOUR FROM ft.transaction_date) BETWEEN 6 AND 11 THEN 'Morning (6-11)'
        WHEN EXTRACT(HOUR FROM ft.transaction_date) BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
        WHEN EXTRACT(HOUR FROM ft.transaction_date) BETWEEN 18 AND 23 THEN 'Evening (18-23)'
        ELSE 'Night (0-5)'
    END as time_period,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction
FROM dw.fact_transactions ft
GROUP BY EXTRACT(HOUR FROM ft.transaction_date), time_period
ORDER BY hour_of_day;

-- 1.9 Volume Transaksi per Quarter
SELECT 
    d.year,
    d.quarter,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter DESC;

-- 1.10 Transaksi Weekend vs Weekday
SELECT 
    CASE 
        WHEN d.is_holiday = 1 OR EXTRACT(DOW FROM d.full_date) IN (0, 6) THEN 'Weekend/Holiday'
        ELSE 'Weekday'
    END as day_type,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY day_type;