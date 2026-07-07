-- ============================================================================
-- 04 CHANNEL ANALYSIS
-- Channel usage dan tren migrasi ke digital
-- ============================================================================

-- 4.1 Channel Usage Summary
SELECT 
    c.channel_code,
    c.channel_name,
    c.channel_category,
    CASE WHEN c.is_digital = 1 THEN 'Digital' ELSE 'Traditional' END as channel_type,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as transaction_pct,
    ROUND(SUM(ft.amount) * 100.0 / SUM(SUM(ft.amount)) OVER (), 2) as amount_pct
FROM dw.fact_transactions ft
JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
GROUP BY c.channel_code, c.channel_name, c.channel_category, c.is_digital
ORDER BY total_transactions DESC;

-- 4.2 Digital vs Traditional Channel Comparison
SELECT 
    CASE WHEN c.is_digital = 1 THEN 'Digital' ELSE 'Traditional' END as channel_type,
    COUNT(DISTINCT c.channel_id) as total_channels,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as transaction_pct,
    ROUND(SUM(ft.amount) * 100.0 / SUM(SUM(ft.amount)) OVER (), 2) as amount_pct,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT ft.customer_key), 0) as revenue_per_customer
FROM dw.fact_transactions ft
JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
GROUP BY c.is_digital
ORDER BY total_transactions DESC;

-- 4.3 Channel Trend Over Time (Monthly)
SELECT 
    d.year,
    d.month,
    d.month_name,
    c.channel_name,
    CASE WHEN c.is_digital = 1 THEN 'Digital' ELSE 'Traditional' END as channel_type,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    COUNT(DISTINCT ft.customer_key) as unique_customers
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
GROUP BY d.year, d.month, d.month_name, c.channel_name, c.is_digital
ORDER BY d.year, d.month, c.channel_name;

-- 4.4 Digital Migration Analysis (Adoption Rate)
WITH monthly_channel AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        c.is_digital,
        COUNT(*) as total_transactions
    FROM dw.fact_transactions ft
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
    GROUP BY d.year, d.month, d.month_name, c.is_digital
)
SELECT 
    year,
    month,
    month_name,
    SUM(CASE WHEN is_digital = 1 THEN total_transactions ELSE 0 END) as digital_transactions,
    SUM(CASE WHEN is_digital = 0 THEN total_transactions ELSE 0 END) as traditional_transactions,
    ROUND(
        SUM(CASE WHEN is_digital = 1 THEN total_transactions ELSE 0 END) * 100.0 / 
        NULLIF(SUM(total_transactions), 0), 2
    ) as digital_adoption_rate_pct
FROM monthly_channel
GROUP BY year, month, month_name
ORDER BY year, month;

-- 4.5 Channel Performance by Customer Segment
SELECT 
    c.channel_name,
    c.channel_category,
    seg.segment,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers
FROM dw.fact_transactions ft
JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
JOIN dw.dim_customer seg ON ft.customer_key = seg.customer_key
GROUP BY c.channel_name, c.channel_category, seg.segment
ORDER BY c.channel_name, total_amount DESC;

-- 4.6 Top Channels by Transaction Type
SELECT 
    ft.transaction_type,
    c.channel_name,
    CASE WHEN c.is_digital = 1 THEN 'Digital' ELSE 'Traditional' END as channel_type,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY ft.transaction_type), 2) as channel_share_pct
FROM dw.fact_transactions ft
JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
GROUP BY ft.transaction_type, c.channel_name, c.is_digital
ORDER BY ft.transaction_type, total_transactions DESC;

-- 4.7 Channel Usage by Time of Day
SELECT 
    c.channel_name,
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
JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
GROUP BY c.channel_name, time_period
ORDER BY c.channel_name, time_period;

-- 4.8 Mobile Banking vs Internet Banking Growth
SELECT 
    d.year,
    d.month,
    d.month_name,
    c.channel_name,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    COUNT(DISTINCT ft.customer_key) as unique_users
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
WHERE c.channel_code IN ('MOB', 'INT')  -- Mobile & Internet Banking
GROUP BY d.year, d.month, d.month_name, c.channel_name
ORDER BY d.year, d.month, c.channel_name;

-- 4.9 Channel Efficiency Metrics
SELECT 
    c.channel_name,
    c.channel_category,
    COUNT(DISTINCT ft.customer_key) as total_users,
    COUNT(*) as total_transactions,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT ft.customer_key), 2) as txn_per_user,
    SUM(ft.amount) as total_amount,
    ROUND(SUM(ft.amount) / COUNT(DISTINCT ft.customer_key), 0) as revenue_per_user,
    ROUND(AVG(ft.amount), 0) as avg_transaction_size
FROM dw.fact_transactions ft
JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
GROUP BY c.channel_name, c.channel_category
ORDER BY txn_per_user DESC;

-- 4.10 ATM vs Teller vs Digital (Traditional Migration)
SELECT 
    d.year,
    d.month,
    COUNT(*) FILTER (WHERE c.channel_code = 'ATM') as atm_transactions,
    COUNT(*) FILTER (WHERE c.channel_code = 'TEL') as teller_transactions,
    COUNT(*) FILTER (WHERE c.channel_code = 'BRN') as branch_transactions,
    COUNT(*) FILTER (WHERE c.is_digital = 1) as digital_transactions,
    ROUND(
        COUNT(*) FILTER (WHERE c.is_digital = 1) * 100.0 / COUNT(*), 2
    ) as digital_percentage,
    ROUND(
        COUNT(*) FILTER (WHERE c.channel_code IN ('TEL', 'BRN')) * 100.0 / COUNT(*), 2
    ) as traditional_branch_percentage
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;