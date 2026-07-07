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
    'dag_branch_performance',
    default_args=default_args,
    description='Branch Performance Analytics',
    schedule_interval='0 3 * * *',  # Daily at 03:00 WIB
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['banking', 'analytics', 'branch'],
)

def branch_performance_ranking():
    """Rank branches by performance"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
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
        RANK() OVER (ORDER BY COUNT(*) DESC) as rank_by_volume,
        RANK() OVER (ORDER BY SUM(ft.amount) DESC) as rank_by_value
    FROM dw.fact_transactions ft
    JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
    GROUP BY b.branch_id, b.branch_code, b.branch_name, b.city, b.province, b.region, b.branch_type
    ORDER BY total_transactions DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n🏢 Branch Performance Ranking:")
    print(df.to_string())
    return df

def regional_performance():
    """Analyze performance by region"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        b.region,
        b.province,
        COUNT(DISTINCT b.branch_id) as total_branches,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        COUNT(DISTINCT ft.customer_key) as unique_customers,
        ROUND(SUM(ft.amount) / COUNT(*), 0) as revenue_per_transaction
    FROM dw.fact_transactions ft
    JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
    GROUP BY b.region, b.province
    ORDER BY total_amount DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Regional Performance:")
    print(df.to_string())
    return df

def branch_type_analysis():
    """Analyze performance by branch type"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        b.branch_type,
        COUNT(DISTINCT b.branch_id) as total_branches,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        ROUND(SUM(ft.amount) / COUNT(DISTINCT b.branch_id), 0) as amount_per_branch
    FROM dw.fact_transactions ft
    JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
    GROUP BY b.branch_type
    ORDER BY total_amount DESC;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n📊 Branch Type Analysis:")
    print(df.to_string())
    return df

def top_performing_branches():
    """Identify top 10 performing branches"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    sql = """
    SELECT 
        b.branch_code,
        b.branch_name,
        b.city,
        b.province,
        COUNT(*) as total_transactions,
        SUM(ft.amount) as total_amount,
        AVG(ft.amount) as avg_transaction,
        COUNT(DISTINCT ft.customer_key) as active_customers,
        ROUND(SUM(ft.amount) / COUNT(*), 0) as revenue_per_txn
    FROM dw.fact_transactions ft
    JOIN dw.dim_branch b ON ft.branch_key = b.branch_key
    GROUP BY b.branch_code, b.branch_name, b.city, b.province
    ORDER BY total_amount DESC
    LIMIT 10;
    """
    
    df = postgres_hook.get_pandas_df(sql)
    print("\n🏆 Top 10 Performing Branches:")
    print(df.to_string())
    return df

# Define tasks
task_branch_ranking = PythonOperator(
    task_id='branch_performance_ranking',
    python_callable=branch_performance_ranking,
    dag=dag,
)

task_regional = PythonOperator(
    task_id='regional_performance',
    python_callable=regional_performance,
    dag=dag,
)

task_branch_type = PythonOperator(
    task_id='branch_type_analysis',
    python_callable=branch_type_analysis,
    dag=dag,
)

task_top_branches = PythonOperator(
    task_id='top_performing_branches',
    python_callable=top_performing_branches,
    dag=dag,
)

# Define task dependencies
[task_branch_ranking, task_regional, task_branch_type, task_top_branches]