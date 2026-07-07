from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_fraud_detection',
    default_args=default_args,
    description='Fraud and Anomaly Detection Analytics',
    schedule_interval='30 4 * * *',  # Daily at 04:30 WIB
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['banking', 'analytics', 'fraud', 'risk'],
)

def fraud_overview():
    """Get fraud detection overview"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        COUNT(*) as total_transactions,
        SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) as fraud_transactions,
        ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as fraud_rate_pct,
        SUM(ft.amount) as total_amount,
        SUM(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE 0 END) as fraud_amount,
        ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE 0 END) * 100.0 / 
              NULLIF(SUM(ft.amount), 0), 4) as fraud_amount_pct,
        AVG(CASE WHEN ft.is_fraud = 1 THEN ft.amount END) as avg_fraud_amount,
        AVG(CASE WHEN ft.is_fraud = 0 THEN ft.amount END) as avg_normal_amount
    FROM dw.fact_transactions ft;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n🚨 Fraud Detection Overview:")
    print(df.to_string())
    return df

def fraud_by_type():
    """Analyze fraud by type"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        ft.fraud_type,
        COUNT(*) as fraud_count,
        SUM(ft.amount) as total_fraud_amount,
        AVG(ft.amount) as avg_fraud_amount,
        COUNT(DISTINCT ft.customer_key) as affected_customers,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as fraud_pct
    FROM dw.fact_transactions ft
    WHERE ft.is_fraud = 1
    GROUP BY ft.fraud_type
    ORDER BY fraud_count DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n🔍 Fraud by Type:")
    print(df.to_string())
    return df

def detect_high_value_anomalies():
    """Detect high-value transaction anomalies"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    WITH stats AS (
        SELECT 
            AVG(amount) as avg_amount,
            STDDEV(amount) as stddev_amount
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
        d.full_date,
        ROUND((ft.amount - s.avg_amount) / NULLIF(s.stddev_amount, 0), 2) as z_score
    FROM dw.fact_transactions ft
    JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
    JOIN dw.dim_channel ch ON ft.channel_key = ch.channel_key
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    CROSS JOIN stats s
    WHERE ft.amount > (SELECT AVG(amount) + 3 * STDDEV(amount) FROM dw.fact_transactions)
       OR ft.status = 'FAILED'
    ORDER BY ft.amount DESC
    LIMIT 50;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n⚠️ High-Value Anomalies (Top 50):")
    print(df.to_string())
    return df

def suspicious_customer_behavior():
    """Identify customers with suspicious behavior patterns"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        c.customer_id,
        c.full_name,
        c.segment,
        c.city,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
        SUM(CASE WHEN ft.status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
        COUNT(DISTINCT ch.channel_id) as channels_used,
        ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate,
        ROUND(SUM(CASE WHEN ft.status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate
    FROM dw.fact_transactions ft
    JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
    JOIN dw.dim_channel ch ON ft.channel_key = ch.channel_key
    GROUP BY c.customer_id, c.full_name, c.segment, c.city
    HAVING 
        SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) > 0 OR
        SUM(CASE WHEN ft.status = 'FAILED' THEN 1 ELSE 0 END) > 5 OR
        AVG(ft.amount) > (SELECT AVG(amount) * 5 FROM dw.fact_transactions)
    ORDER BY fraud_count DESC, failed_count DESC
    LIMIT 30;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n🎯 Suspicious Customer Behavior (Top 30):")
    print(df.to_string())
    return df

def fraud_trend_over_time():
    """Analyze fraud trend over time"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        d.year,
        d.month,
        d.month_name,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) as fraud_transactions,
        ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as fraud_rate_pct,
        SUM(ft.amount) as total_amount,
        SUM(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE 0 END) as fraud_amount
    FROM dw.fact_transactions ft
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Fraud Trend Over Time:")
    print(df.to_string())
    return df

def channel_fraud_analysis():
    """Analyze fraud by channel"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        ch.channel_name,
        ch.channel_category,
        CASE WHEN ch.is_digital = 1 THEN 'Digital' ELSE 'Traditional' END as channel_type,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) as fraud_transactions,
        ROUND(SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as fraud_rate_pct,
        SUM(ft.amount) as total_amount,
        SUM(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE 0 END) as fraud_amount
    FROM dw.fact_transactions ft
    JOIN dw.dim_channel ch ON ft.channel_key = ch.channel_key
    GROUP BY ch.channel_name, ch.channel_category, ch.is_digital
    ORDER BY fraud_rate_pct DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n🔐 Fraud Analysis by Channel:")
    print(df.to_string())
    return df

# Define tasks
task_fraud_overview = PythonOperator(
    task_id='fraud_overview',
    python_callable=fraud_overview,
    dag=dag,
)

task_fraud_by_type = PythonOperator(
    task_id='fraud_by_type',
    python_callable=fraud_by_type,
    dag=dag,
)

task_high_value_anomalies = PythonOperator(
    task_id='detect_high_value_anomalies',
    python_callable=detect_high_value_anomalies,
    dag=dag,
)

task_suspicious_behavior = PythonOperator(
    task_id='suspicious_customer_behavior',
    python_callable=suspicious_customer_behavior,
    dag=dag,
)

task_fraud_trend = PythonOperator(
    task_id='fraud_trend_over_time',
    python_callable=fraud_trend_over_time,
    dag=dag,
)

task_channel_fraud = PythonOperator(
    task_id='channel_fraud_analysis',
    python_callable=channel_fraud_analysis,
    dag=dag,
)

# Define task dependencies
[task_fraud_overview, task_fraud_by_type, task_high_value_anomalies, 
 task_suspicious_behavior, task_fraud_trend, task_channel_fraud]