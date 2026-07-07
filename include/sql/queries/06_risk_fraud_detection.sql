-- ============================================================================
-- 06 RISK & FRAUD DETECTION
-- Deteksi transaksi anomali dan fraud
-- ============================================================================

-- 6.1 Fraud Overview Dashboard
SELECT 
    COUNT(*) as total_transactions,
    SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) as fraud_transactions,
    ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as fraud_rate_pct,
    SUM(ft.amount) as total_amount,
    SUM(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE 0 END) as fraud_amount,
    ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE 0 END) * 100.0 / 
          NULLIF(SUM(ft.amount), 0), 4) as fraud_amount_pct,
    AVG(CASE WHEN ft.is_fraud = 1 THEN ft.amount END) as avg_fraud_amount,
    AVG(CASE WHEN ft.is_fraud = 0 THEN ft.amount END) as avg_normal_amount,
    COUNT(DISTINCT CASE WHEN ft.is_fraud = 1 THEN ft.customer_key END) as affected_customers
FROM dw.fact_transactions ft;

-- 6.2 Fraud by Type
SELECT 
    ft.fraud_type,
    COUNT(*) as fraud_count,
    SUM(ft.amount) as total_fraud_amount,
    AVG(ft.amount) as avg_fraud_amount,
    MIN(ft.amount) as min_fraud_amount,
    MAX(ft.amount) as max_fraud_amount,
    COUNT(DISTINCT ft.customer_key) as affected_customers,
    COUNT(DISTINCT ft.account_key) as affected_accounts,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as fraud_pct
FROM dw.fact_transactions ft
WHERE ft.is_fraud = 1
GROUP BY ft.fraud_type
ORDER BY fraud_count DESC;

-- 6.3 High-Value Transaction Anomalies (Statistical Outliers)
WITH stats AS (
    SELECT 
        AVG(amount) as avg_amount,
        STDDEV(amount) as stddev_amount,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_amount
    FROM dw.fact_transactions
)
SELECT 
    ft.transaction_id,
    c.full_name as customer_name,
    c.segment,
    ft.amount,
    ft.transaction_type,
    ft.status,
    ch.channel_name,
    b.branch_name,
    d.full_date,
    ROUND((ft.amount - s.avg_amount) / NULLIF(s.stddev_amount, 0), 2) as z_score,
    CASE 
        WHEN ft.amount > s.avg_amount + 3 * s.stddev_amount THEN 'Extreme Outlier'
        WHEN ft.amount > s.avg_amount + 2 * s.stddev_amount THEN 'High Outlier'
        ELSE 'Normal'
    END as anomaly_level
FROM dw.fact_transactions ft
JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
JOIN dw.dim_channel ch ON ft.channel_key = ch.channel_key
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
JOIN dw.dim_date d ON ft.date_key = d.date_key
CROSS JOIN stats s
WHERE ft.amount > (SELECT AVG(amount) + 3 * STDDEV(amount) FROM dw.fact_transactions)
ORDER BY ft.amount DESC
LIMIT 50;

-- 6.4 Suspicious Customer Behavior Patterns
SELECT 
    c.customer_id,
    c.full_name,
    c.segment,
    c.city,
    c.province,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
    SUM(CASE WHEN ft.status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
    COUNT(DISTINCT ch.channel_id) as channels_used,
    COUNT(DISTINCT b.branch_id) as branches_used,
    ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate,
    ROUND(SUM(CASE WHEN ft.status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate,
    CASE 
        WHEN SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) > 2 THEN 'High Risk'
        WHEN SUM(CASE WHEN ft.status = 'FAILED' THEN 1 ELSE 0 END) > 10 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_level
FROM dw.fact_transactions ft
JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
JOIN dw.dim_channel ch ON ft.channel_key = ch.channel_key
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY c.customer_id, c.full_name, c.segment, c.city, c.province
HAVING 
    SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) > 0 OR
    SUM(CASE WHEN ft.status = 'FAILED' THEN 1 ELSE 0 END) > 5 OR
    AVG(ft.amount) > (SELECT AVG(amount) * 5 FROM dw.fact_transactions)
ORDER BY fraud_count DESC, failed_count DESC
LIMIT 50;

-- 6.5 Fraud Trend Over Time
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) as fraud_transactions,
    ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as fraud_rate_pct,
    SUM(ft.amount) as total_amount,
    SUM(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE 0 END) as fraud_amount,
    COUNT(DISTINCT ft.customer_key) as affected_customers
FROM dw.fact_transactions ft
JOIN dw.dim_date d ON ft.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- 6.6 Fraud by Channel
SELECT 
    ch.channel_name,
    ch.channel_category,
    CASE WHEN ch.is_digital = 1 THEN 'Digital' ELSE 'Traditional' END as channel_type,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) as fraud_transactions,
    ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as fraud_rate_pct,
    SUM(ft.amount) as total_amount,
    SUM(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE 0 END) as fraud_amount,
    ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE 0 END) * 100.0 / 
          NULLIF(SUM(ft.amount), 0), 4) as fraud_amount_pct
FROM dw.fact_transactions ft
JOIN dw.dim_channel ch ON ft.channel_key = ch.channel_key
GROUP BY ch.channel_name, ch.channel_category, ch.is_digital
ORDER BY fraud_rate_pct DESC;

-- 6.7 Failed Transactions Analysis
SELECT 
    ft.status,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    AVG(ft.amount) as avg_transaction,
    COUNT(DISTINCT ft.customer_key) as unique_customers,
    COUNT(DISTINCT ch.channel_id) as channels_involved,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as transaction_pct
FROM dw.fact_transactions ft
JOIN dw.dim_channel ch ON ft.channel_key = ch.channel_key
GROUP BY ft.status
ORDER BY total_transactions DESC;

-- 6.8 Unusual Transaction Patterns (Velocity Check)
WITH customer_daily AS (
    SELECT 
        ft.customer_key,
        d.full_date,
        COUNT(*) as daily_transactions,
        SUM(ft.amount) as daily_amount
    FROM dw.fact_transactions ft
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    GROUP BY ft.customer_key, d.full_date
)
SELECT 
    c.customer_id,
    c.full_name,
    c.segment,
    cd.full_date,
    cd.daily_transactions,
    cd.daily_amount,
    ROUND(cd.daily_amount / NULLIF(cd.daily_transactions, 0), 0) as avg_txn_that_day
FROM customer_daily cd
JOIN dw.dim_customer c ON cd.customer_key = c.customer_key
WHERE cd.daily_transactions > 10 
   OR cd.daily_amount > 100000000
ORDER BY cd.daily_transactions DESC, cd.daily_amount DESC
LIMIT 50;

-- 6.9 Geographic Anomaly Detection (Transactions from Multiple Locations)
SELECT 
    c.customer_id,
    c.full_name,
    c.segment,
    COUNT(DISTINCT b.province) as provinces_visited,
    COUNT(DISTINCT b.city) as cities_visited,
    COUNT(DISTINCT b.branch_id) as branches_visited,
    COUNT(*) as total_transactions,
    SUM(ft.amount) as total_amount,
    STRING_AGG(DISTINCT b.province, ', ') as provinces_list
FROM dw.fact_transactions ft
JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
GROUP BY c.customer_id, c.full_name, c.segment
HAVING COUNT(DISTINCT b.province) > 3
ORDER BY provinces_visited DESC, total_amount DESC
LIMIT 30;

-- 6.10 Real-time Fraud Alerts (Recent Suspicious Activity)
SELECT 
    ft.transaction_id,
    c.full_name as customer_name,
    c.segment,
    ft.amount,
    ft.transaction_type,
    ft.status,
    ft.is_fraud,
    ft.fraud_type,
    ch.channel_name,
    b.branch_name,
    d.full_date,
    EXTRACT(HOUR FROM ft.transaction_date) as transaction_hour,
    CASE 
        WHEN ft.is_fraud = 1 THEN '🚨 FRAUD DETECTED'
        WHEN ft.status = 'FAILED' AND ft.amount > 10000000 THEN '⚠️ HIGH-VALUE FAILED'
        WHEN ft.amount > 50000000 THEN '⚡ HIGH-VALUE TRANSACTION'
        ELSE '✓ Normal'
    END as alert_level
FROM dw.fact_transactions ft
JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
JOIN dw.dim_channel ch ON ft.channel_key = ch.channel_key
JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
JOIN dw.dim_date d ON ft.date_key = d.date_key
WHERE ft.is_fraud = 1 
   OR (ft.status = 'FAILED' AND ft.amount > 10000000)
   OR ft.amount > 50000000
ORDER BY d.full_date DESC, ft.amount DESC
LIMIT 100;