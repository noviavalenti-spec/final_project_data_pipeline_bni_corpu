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
    'dag_customer_360',
    default_args=default_args,
    description='Customer 360 - Customer Profiling and Segmentation',
    schedule_interval='30 2 * * *',  # Daily at 02:30 WIB
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['banking', 'analytics', 'customer'],
)

def top_active_customers():
    """Find top 10 most active customers"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        c.customer_id,
        c.full_name,
        c.segment,
        c.city,
        c.province,
        COUNT(*) as transaction_count,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        COUNT(DISTINCT ft.account_key) as total_accounts
    FROM dw.fact_transactions ft
    JOIN dw.dim_customer c ON ft.customer_key = c.customer_key
    GROUP BY c.customer_id, c.full_name, c.segment, c.city, c.province
    ORDER BY transaction_count DESC
    LIMIT 10;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n👥 Top 10 Most Active Customers:")
    print(df.to_string())
    return df

def customer_segment_distribution():
    """Analyze customer distribution by segment"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        c.segment,
        COUNT(DISTINCT c.customer_id) as total_customers,
        COUNT(DISTINCT ft.transaction_id) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        ROUND(AVG(c.salary), 0) as avg_salary,
        ROUND(AVG(c.credit_score), 0) as avg_credit_score
    FROM dw.dim_customer c
    LEFT JOIN dw.fact_transactions ft ON c.customer_key = ft.customer_key
    GROUP BY c.segment
    ORDER BY total_customers DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Customer Segment Distribution:")
    print(df.to_string())
    return df

def high_value_customers():
    """Identify high-value customers"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        c.customer_id,
        c.full_name,
        c.segment,
        c.salary,
        c.credit_score,
        COUNT(*) as transaction_count,
        SUM(ft.amount) as total_transaction_value,
        AVG(ft.amount) as avg_transaction,
        CASE 
            WHEN SUM(ft.amount) > 100000000 THEN 'Platinum'
            WHEN SUM(ft.amount) > 50000000 THEN 'Gold'
            WHEN SUM(ft.amount) > 10000000 THEN 'Silver'
            ELSE 'Bronze'
        END as customer_value_tier
    FROM dw.dim_customer c
    JOIN dw.fact_transactions ft ON c.customer_key = ft.customer_key
    GROUP BY c.customer_id, c.full_name, c.segment, c.salary, c.credit_score
    HAVING SUM(ft.amount) > 10000000
    ORDER BY total_transaction_value DESC
    LIMIT 20;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n💎 Top 20 High-Value Customers:")
    print(df.to_string())
    return df

def customer_by_geography():
    """Analyze customers by geographic location"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        c.province,
        c.city,
        COUNT(DISTINCT c.customer_id) as total_customers,
        COUNT(DISTINCT ft.transaction_id) as total_transactions,
        SUM(ft.amount) as total_amount,
        ROUND(AVG(ft.amount), 0) as avg_transaction
    FROM dw.dim_customer c
    LEFT JOIN dw.fact_transactions ft ON c.customer_key = ft.customer_key
    GROUP BY c.province, c.city
    ORDER BY total_customers DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n🌍 Customer Distribution by Geography:")
    print(df.to_string())
    return df

# Define tasks
task_top_customers = PythonOperator(
    task_id='top_active_customers',
    python_callable=top_active_customers,
    dag=dag,
)

task_segment_dist = PythonOperator(
    task_id='customer_segment_distribution',
    python_callable=customer_segment_distribution,
    dag=dag,
)

task_high_value = PythonOperator(
    task_id='high_value_customers',
    python_callable=high_value_customers,
    dag=dag,
)

task_geography = PythonOperator(
    task_id='customer_by_geography',
    python_callable=customer_by_geography,
    dag=dag,
)

# Define task dependencies
[task_top_customers, task_segment_dist, task_high_value, task_geography]