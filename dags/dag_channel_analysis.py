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
    'dag_channel_analysis',
    default_args=default_args,
    description='Channel Usage and Digital Migration Analysis',
    schedule_interval='30 3 * * *',  # Daily at 03:30 WIB
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['banking', 'analytics', 'channel'],
)

def channel_usage_summary():
    """Analyze channel usage summary"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        c.channel_code,
        c.channel_name,
        c.channel_category,
        CASE WHEN c.is_digital = 1 THEN 'Digital' ELSE 'Traditional' END as channel_type,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        COUNT(DISTINCT ft.customer_key) as unique_customers,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as transaction_pct
    FROM dw.fact_transactions ft
    JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
    GROUP BY c.channel_code, c.channel_name, c.channel_category, c.is_digital
    ORDER BY total_transactions DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📱 Channel Usage Summary:")
    print(df.to_string())
    return df

def digital_vs_traditional():
    """Compare digital vs traditional channel usage"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        CASE WHEN c.is_digital = 1 THEN 'Digital' ELSE 'Traditional' END as channel_type,
        COUNT(DISTINCT c.channel_id) as total_channels,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        COUNT(DISTINCT ft.customer_key) as unique_customers,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as transaction_pct,
        ROUND(SUM(ft.amount) * 100.0 / SUM(SUM(ft.amount)) OVER (), 2) as amount_pct
    FROM dw.fact_transactions ft
    JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
    GROUP BY c.is_digital
    ORDER BY total_transactions DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Digital vs Traditional Channels:")
    print(df.to_string())
    return df

def channel_trend_over_time():
    """Analyze channel usage trend over time"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        d.year,
        d.month,
        d.month_name,
        c.channel_name,
        CASE WHEN c.is_digital = 1 THEN 'Digital' ELSE 'Traditional' END as channel_type,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount
    FROM dw.fact_transactions ft
    JOIN dw.dim_date d ON ft.date_key = d.date_key
    JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
    GROUP BY d.year, d.month, d.month_name, c.channel_name, c.is_digital
    ORDER BY d.year, d.month, c.channel_name;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📈 Channel Trend Over Time (Last 12 months):")
    print(df.tail(60).to_string())
    return df

def digital_migration_analysis():
    """Analyze digital migration patterns"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    WITH monthly_channel AS (
        SELECT 
            d.year,
            d.month,
            c.is_digital,
            COUNT(*) as total_transactions
        FROM dw.fact_transactions ft
        JOIN dw.dim_date d ON ft.date_key = d.date_key
        JOIN dw.dim_channel c ON ft.channel_key = c.channel_key
        GROUP BY d.year, d.month, c.is_digital
    )
    SELECT 
        year,
        month,
        SUM(CASE WHEN is_digital = 1 THEN total_transactions ELSE 0 END) as digital_transactions,
        SUM(CASE WHEN is_digital = 0 THEN total_transactions ELSE 0 END) as traditional_transactions,
        ROUND(
            SUM(CASE WHEN is_digital = 1 THEN total_transactions ELSE 0 END) * 100.0 / 
            SUM(total_transactions), 2
        ) as digital_adoption_rate
    FROM monthly_channel
    GROUP BY year, month
    ORDER BY year, month;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n🚀 Digital Migration Analysis:")
    print(df.to_string())
    return df

# Define tasks
task_channel_summary = PythonOperator(
    task_id='channel_usage_summary',
    python_callable=channel_usage_summary,
    dag=dag,
)

task_digital_traditional = PythonOperator(
    task_id='digital_vs_traditional',
    python_callable=digital_vs_traditional,
    dag=dag,
)

task_channel_trend = PythonOperator(
    task_id='channel_trend_over_time',
    python_callable=channel_trend_over_time,
    dag=dag,
)

task_migration = PythonOperator(
    task_id='digital_migration_analysis',
    python_callable=digital_migration_analysis,
    dag=dag,
)

# Define task dependencies
[task_channel_summary, task_digital_traditional, task_channel_trend, task_migration]