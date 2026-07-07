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
    'dag_transaction_analytics',
    default_args=default_args,
    description='Transaction Analytics - Volume, Value, and Trends',
    schedule_interval='0 2 * * *',  # Daily at 02:00 WIB
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['banking', 'analytics', 'transactions'],
)

def transaction_daily_trends():
    """Analyze daily transaction trends"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        d.full_date,
        d.year,
        d.month_name,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        MIN(ft.amount) as min_transaction,
        MAX(ft.amount) as max_transaction
    FROM dw.fact_transactions ft
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    GROUP BY d.full_date, d.year, d.month_name
    ORDER BY d.full_date DESC
    LIMIT 30;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Last 30 Days Transaction Trends:")
    print(df.to_string())
    return df

def transaction_monthly_summary():
    """Monthly transaction summary"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        d.year,
        d.month,
        d.month_name,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        COUNT(DISTINCT ft.customer_key) as active_customers
    FROM dw.fact_transactions ft
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Monthly Transaction Summary:")
    print(df.to_string())
    return df

def transaction_by_type():
    """Analyze transactions by type (Credit/Debit)"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        ft.transaction_type,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        COUNT(DISTINCT ft.customer_key) as unique_customers
    FROM dw.fact_transactions ft
    GROUP BY ft.transaction_type;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Transaction by Type:")
    print(df.to_string())
    return df

def transaction_growth_rate():
    """Calculate month-over-month growth rate"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    WITH monthly_data AS (
        SELECT 
            d.year,
            d.month,
            COUNT(*) as total_transactions,
            SUM(ft.amount) as total_amount
        FROM dw.fact_transactions ft
        JOIN dw.dim_date d ON ft.date_key = d.date_key
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
    )
    SELECT 
        year,
        month,
        total_transactions,
        total_amount,
        LAG(total_transactions) OVER (ORDER BY year, month) as prev_transactions,
        LAG(total_amount) OVER (ORDER BY year, month) as prev_amount,
        ROUND(
            (total_transactions - LAG(total_transactions) OVER (ORDER BY year, month)) * 100.0 / 
            NULLIF(LAG(total_transactions) OVER (ORDER BY year, month), 0), 2
        ) as transaction_growth_pct,
        ROUND(
            (total_amount - LAG(total_amount) OVER (ORDER BY year, month)) * 100.0 / 
            NULLIF(LAG(total_amount) OVER (ORDER BY year, month), 0), 2
        ) as amount_growth_pct
    FROM monthly_data;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Month-over-Month Growth Rate:")
    print(df.to_string())
    return df

# Define tasks
task_daily_trends = PythonOperator(
    task_id='transaction_daily_trends',
    python_callable=transaction_daily_trends,
    dag=dag,
)

task_monthly_summary = PythonOperator(
    task_id='transaction_monthly_summary',
    python_callable=transaction_monthly_summary,
    dag=dag,
)

task_by_type = PythonOperator(
    task_id='transaction_by_type',
    python_callable=transaction_by_type,
    dag=dag,
)

task_growth_rate = PythonOperator(
    task_id='transaction_growth_rate',
    python_callable=transaction_growth_rate,
    dag=dag,
)

# Define task dependencies
[task_daily_trends, task_monthly_summary, task_by_type, task_growth_rate]