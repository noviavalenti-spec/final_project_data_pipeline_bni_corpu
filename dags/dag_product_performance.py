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
    'dag_product_performance',
    default_args=default_args,
    description='Product Performance Analytics (Tabungan/Giro/Deposito)',
    schedule_interval='0 4 * * *',  # Daily at 04:00 WIB
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['banking', 'analytics', 'product'],
)

def product_performance_summary():
    """Analyze product performance summary"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        a.product_name,
        a.account_type,
        COUNT(DISTINCT a.account_id) as total_accounts,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        COUNT(DISTINCT ft.customer_key) as unique_customers,
        ROUND(SUM(ft.amount) / COUNT(DISTINCT a.account_id), 0) as amount_per_account
    FROM dw.fact_transactions ft
    JOIN dw.dim_account a ON ft.account_key = a.account_key
    GROUP BY a.product_name, a.account_type
    ORDER BY total_amount DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n💳 Product Performance Summary:")
    print(df.to_string())
    return df

def product_by_segment():
    """Analyze product usage by customer segment"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        a.product_name,
        c.segment,
        COUNT(DISTINCT a.account_id) as total_accounts,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        ROUND(AVG(c.salary), 0) as avg_customer_salary
    FROM dw.fact_transactions ft
    JOIN dw.dim_account a ON ft.account_key = a.account_key
    JOIN dw.dim_customer c ON a.customer_key = c.customer_key
    GROUP BY a.product_name, c.segment
    ORDER BY a.product_name, total_amount DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Product Usage by Customer Segment:")
    print(df.to_string())
    return df

def top_products_by_revenue():
    """Identify top products by revenue"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        a.product_name,
        COUNT(DISTINCT a.account_id) as total_accounts,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_transaction_value,
        AVG(ft.amount) as avg_transaction,
        COUNT(DISTINCT ft.customer_key) as active_customers,
        RANK() OVER (ORDER BY SUM(ft.amount) DESC) as revenue_rank,
        RANK() OVER (ORDER BY COUNT(*) DESC) as volume_rank
    FROM dw.fact_transactions ft
    JOIN dw.dim_account a ON ft.account_key = a.account_key
    GROUP BY a.product_name
    ORDER BY total_transaction_value DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n🏆 Top Products by Revenue:")
    print(df.to_string())
    return df

def product_growth_trend():
    """Analyze product performance trend over time"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        d.year,
        d.month,
        d.month_name,
        a.product_name,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        COUNT(DISTINCT a.account_id) as active_accounts
    FROM dw.fact_transactions ft
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    JOIN dw.dim_account a ON ft.account_key = a.account_key
    GROUP BY d.year, d.month, d.month_name, a.product_name
    ORDER BY d.year, d.month, a.product_name;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📈 Product Growth Trend (Last 12 months):")
    print(df.tail(36).to_string())
    return df

# Define tasks
task_product_summary = PythonOperator(
    task_id='product_performance_summary',
    python_callable=product_performance_summary,
    dag=dag,
)

task_product_segment = PythonOperator(
    task_id='product_by_segment',
    python_callable=product_by_segment,
    dag=dag,
)

task_top_products = PythonOperator(
    task_id='top_products_by_revenue',
    python_callable=top_products_by_revenue,
    dag=dag,
)

task_product_trend = PythonOperator(
    task_id='product_growth_trend',
    python_callable=product_growth_trend,
    dag=dag,
)

# Define task dependencies
[task_product_summary, task_product_segment, task_top_products, task_product_trend]