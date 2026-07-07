from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_etl_banking_analytics',
    default_args=default_args,
    description='Main ETL Pipeline for Banking Analytics - Extract, Staging, Load to DW',
    schedule_interval='0 1 * * *',  # Daily at 01:00 WIB
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['banking', 'etl', 'main-pipeline'],
)

def create_staging_tables():
    """Create staging tables in PostgreSQL"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    create_tables_sql = """
    -- Create staging schema
    CREATE SCHEMA IF NOT EXISTS staging;
    
    -- Drop tables if exist
    DROP TABLE IF EXISTS staging.transactions;
    DROP TABLE IF EXISTS staging.customers;
    DROP TABLE IF EXISTS staging.accounts;
    DROP TABLE IF EXISTS staging.branches;
    DROP TABLE IF EXISTS staging.channels;
    DROP TABLE IF EXISTS staging.fraud_labels;
    DROP TABLE IF EXISTS staging.dim_date;
    
    -- Create staging tables
    CREATE TABLE staging.transactions (
        transaction_id INTEGER,
        account_id INTEGER,
        customer_id INTEGER,
        branch_id INTEGER,
        channel_id INTEGER,
        amount DECIMAL(15,2),
        type VARCHAR(20),
        status VARCHAR(20),
        transaction_date TIMESTAMP
    );
    
    CREATE TABLE staging.customers (
        customer_id INTEGER,
        full_name VARCHAR(100),
        gender VARCHAR(20),
        segment VARCHAR(20),
        city VARCHAR(50),
        province VARCHAR(50),
        credit_score INTEGER,
        salary DECIMAL(15,2)
    );
    
    CREATE TABLE staging.accounts (
        account_id INTEGER,
        account_no VARCHAR(20),
        account_type VARCHAR(20),
        product_name VARCHAR(50),
        currency VARCHAR(10),
        status VARCHAR(20),
        customer_id INTEGER
    );
    
    CREATE TABLE staging.branches (
        branch_id INTEGER,
        branch_code VARCHAR(10),
        branch_name VARCHAR(100),
        city VARCHAR(50),
        province VARCHAR(50),
        region VARCHAR(50),
        branch_type VARCHAR(50)
    );
    
    CREATE TABLE staging.channels (
        channel_id INTEGER,
        channel_code VARCHAR(10),
        channel_name VARCHAR(50),
        channel_category VARCHAR(50),
        is_digital INTEGER
    );
    
    CREATE TABLE staging.fraud_labels (
        transaction_id INTEGER,
        is_fraud INTEGER,
        fraud_type VARCHAR(100),
        fraud_score DECIMAL(3,2),
        flagged_at TIMESTAMP
    );
    
    CREATE TABLE staging.dim_date (
        date_id INTEGER,
        full_date DATE,
        year INTEGER,
        quarter INTEGER,
        month INTEGER,
        month_name VARCHAR(20),
        day_name VARCHAR(20),
        is_holiday INTEGER
    );
    """
    
    postgres_hook.run(create_tables_sql)
    print("✅ Staging tables created successfully")

def load_csv_to_staging(**context):
    """Load CSV files to staging tables"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    dataset_path = '/opt/airflow/include/dataset'
    
    # Load transactions
    print("Loading transactions...")
    transactions_df = pd.read_csv(f'{dataset_path}/transactions.csv')
    for _, row in transactions_df.iterrows():
        postgres_hook.run("""
            INSERT INTO staging.transactions 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, parameters=(
            row['transaction_id'], row['account_id'], row['customer_id'],
            row['branch_id'], row['channel_id'], row['amount'],
            row['type'], row['status'], row['transaction_date']
        ))
    
    # Load customers
    print("Loading customers...")
    customers_df = pd.read_csv(f'{dataset_path}/customers.csv')
    for _, row in customers_df.iterrows():
        postgres_hook.run("""
            INSERT INTO staging.customers 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, parameters=(
            row['customer_id'], row['full_name'], row['gender'],
            row['segment'], row['city'], row['province'],
            row['credit_score'], row['salary']
        ))
    
    # Load accounts
    print("Loading accounts...")
    accounts_df = pd.read_csv(f'{dataset_path}/accounts.csv')
    for _, row in accounts_df.iterrows():
        postgres_hook.run("""
            INSERT INTO staging.accounts 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, parameters=(
            row['account_id'], row['account_no'], row['account_type'],
            row['product_name'], row['currency'], row['status'],
            row['customer_id']
        ))
    
    # Load branches
    print("Loading branches...")
    branches_df = pd.read_csv(f'{dataset_path}/branches.csv')
    for _, row in branches_df.iterrows():
        postgres_hook.run("""
            INSERT INTO staging.branches 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, parameters=(
            row['branch_id'], row['branch_code'], row['branch_name'],
            row['city'], row['province'], row['region'],
            row['branch_type']
        ))
    
    # Load channels
    print("Loading channels...")
    channels_df = pd.read_csv(f'{dataset_path}/channels.csv')
    for _, row in channels_df.iterrows():
        postgres_hook.run("""
            INSERT INTO staging.channels 
            VALUES (%s, %s, %s, %s, %s)
        """, parameters=(
            row['channel_id'], row['channel_code'], row['channel_name'],
            row['channel_category'], row['is_digital']
        ))
    
    # Load fraud labels
    print("Loading fraud labels...")
    fraud_df = pd.read_csv(f'{dataset_path}/fraud_labels.csv')
    for _, row in fraud_df.iterrows():
        postgres_hook.run("""
            INSERT INTO staging.fraud_labels 
            VALUES (%s, %s, %s, %s, %s)
        """, parameters=(
            row['transaction_id'], row['is_fraud'], row['fraud_type'],
            row['fraud_score'], row['flagged_at']
        ))
    
    # Load dim_date
    print("Loading dim_date...")
    dim_date_df = pd.read_csv(f'{dataset_path}/dim_date.csv')
    for _, row in dim_date_df.iterrows():
        postgres_hook.run("""
            INSERT INTO staging.dim_date 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, parameters=(
            row['date_id'], row['full_date'], row['year'],
            row['quarter'], row['month'], row['month_name'],
            row['day_name'], row['is_holiday']
        ))
    
    print(f"✅ Loaded {len(transactions_df)} transactions, {len(customers_df)} customers, "
          f"{len(accounts_df)} accounts, {len(branches_df)} branches, "
          f"{len(channels_df)} channels, {len(fraud_df)} fraud labels")

def create_dw_schema_and_tables():
    """Create DW schema and dimension/fact tables"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    create_dw_sql = """
    -- Create DW schema if not exists
    CREATE SCHEMA IF NOT EXISTS dw;
    
    -- Drop tables if exist
    DROP TABLE IF EXISTS dw.fact_transactions;
    DROP TABLE IF EXISTS dw.dim_date;
    DROP TABLE IF EXISTS dw.dim_customer;
    DROP TABLE IF EXISTS dw.dim_account;
    DROP TABLE IF EXISTS dw.dim_branch;
    DROP TABLE IF EXISTS dw.dim_channel;
    
    -- Create dimension tables
    CREATE TABLE dw.dim_date (
        date_key INTEGER PRIMARY KEY,
        full_date DATE,
        year INTEGER,
        quarter INTEGER,
        month INTEGER,
        month_name VARCHAR(20),
        day_name VARCHAR(20),
        is_holiday INTEGER
    );
    
    CREATE TABLE dw.dim_customer (
        customer_key INTEGER PRIMARY KEY,
        customer_id INTEGER,
        full_name VARCHAR(100),
        gender VARCHAR(20),
        segment VARCHAR(20),
        city VARCHAR(50),
        province VARCHAR(50),
        credit_score INTEGER,
        salary DECIMAL(15,2),
        effective_date DATE DEFAULT CURRENT_DATE,
        is_current INTEGER DEFAULT 1
    );
    
    CREATE TABLE dw.dim_account (
        account_key INTEGER PRIMARY KEY,
        account_id INTEGER,
        account_no VARCHAR(20),
        account_type VARCHAR(20),
        product_name VARCHAR(50),
        currency VARCHAR(10),
        status VARCHAR(20),
        customer_key INTEGER,
        effective_date DATE DEFAULT CURRENT_DATE,
        is_current INTEGER DEFAULT 1
    );
    
    CREATE TABLE dw.dim_branch (
        branch_key INTEGER PRIMARY KEY,
        branch_id INTEGER,
        branch_code VARCHAR(10),
        branch_name VARCHAR(100),
        city VARCHAR(50),
        province VARCHAR(50),
        region VARCHAR(50),
        branch_type VARCHAR(50)
    );
    
    CREATE TABLE dw.dim_channel (
        channel_key INTEGER PRIMARY KEY,
        channel_id INTEGER,
        channel_code VARCHAR(10),
        channel_name VARCHAR(50),
        channel_category VARCHAR(50),
        is_digital INTEGER
    );
    
    -- Create fact table
    CREATE TABLE dw.fact_transactions (
        transaction_key SERIAL PRIMARY KEY,
        transaction_id INTEGER,
        date_key INTEGER,
        customer_key INTEGER,
        account_key INTEGER,
        branch_key INTEGER,
        channel_key INTEGER,
        amount DECIMAL(15,2),
        transaction_type VARCHAR(20),
        status VARCHAR(20),
        is_fraud INTEGER DEFAULT 0,
        fraud_type VARCHAR(100),
        load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create indexes for performance
    CREATE INDEX idx_fact_transactions_date ON dw.fact_transactions(date_key);
    CREATE INDEX idx_fact_transactions_customer ON dw.fact_transactions(customer_key);
    CREATE INDEX idx_fact_transactions_account ON dw.fact_transactions(account_key);
    CREATE INDEX idx_fact_transactions_branch ON dw.fact_transactions(branch_key);
    CREATE INDEX idx_fact_transactions_channel ON dw.fact_transactions(channel_key);
    """
    
    postgres_hook.run(create_dw_sql)
    print("✅ DW schema and tables created successfully")

def transform_and_load_dw():
    """Transform staging data and load to DW"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    print("Loading dimensions and facts to DW...")
    
    # Load dim_date
    postgres_hook.run("""
        INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, month_name, day_name, is_holiday)
        SELECT 
            date_id as date_key,
            full_date,
            year,
            quarter,
            month,
            month_name,
            day_name,
            is_holiday
        FROM staging.dim_date
        ON CONFLICT (date_key) DO NOTHING;
    """)
    
    # Load dim_customer
    postgres_hook.run("""
        INSERT INTO dw.dim_customer (
            customer_key, customer_id, full_name, gender, segment, 
            city, province, credit_score, salary
        )
        SELECT 
            customer_id as customer_key,
            customer_id,
            full_name,
            gender,
            segment,
            city,
            province,
            credit_score,
            salary
        FROM staging.customers
        ON CONFLICT (customer_key) DO NOTHING;
    """)
    
    # Load dim_account
    postgres_hook.run("""
        INSERT INTO dw.dim_account (
            account_key, account_id, account_no, account_type, 
            product_name, currency, status, customer_key
        )
        SELECT 
            a.account_id as account_key,
            a.account_id,
            a.account_no,
            a.account_type,
            a.product_name,
            a.currency,
            a.status,
            a.customer_id as customer_key
        FROM staging.accounts a
        ON CONFLICT (account_key) DO NOTHING;
    """)
    
    # Load dim_branch
    postgres_hook.run("""
        INSERT INTO dw.dim_branch (
            branch_key, branch_id, branch_code, branch_name, 
            city, province, region, branch_type
        )
        SELECT 
            branch_id as branch_key,
            branch_id,
            branch_code,
            branch_name,
            city,
            province,
            region,
            branch_type
        FROM staging.branches
        ON CONFLICT (branch_key) DO NOTHING;
    """)
    
    # Load dim_channel
    postgres_hook.run("""
        INSERT INTO dw.dim_channel (
            channel_key, channel_id, channel_code, channel_name,
            channel_category, is_digital
        )
        SELECT 
            channel_id as channel_key,
            channel_id,
            channel_code,
            channel_name,
            channel_category,
            is_digital
        FROM staging.channels
        ON CONFLICT (channel_key) DO NOTHING;
    """)
    
    # Load fact_transactions
    postgres_hook.run("""
        INSERT INTO dw.fact_transactions (
            transaction_id, date_key, customer_key, account_key, 
            branch_key, channel_key, amount, transaction_type, 
            status, is_fraud, fraud_type
        )
        SELECT 
            t.transaction_id,
            EXTRACT(YEAR FROM t.transaction_date) * 10000 + 
            EXTRACT(MONTH FROM t.transaction_date) * 100 + 
            EXTRACT(DAY FROM t.transaction_date) as date_key,
            t.customer_id as customer_key,
            t.account_id as account_key,
            t.branch_id as branch_key,
            t.channel_id as channel_key,
            t.amount,
            t.type as transaction_type,
            t.status,
            COALESCE(f.is_fraud, 0) as is_fraud,
            f.fraud_type
        FROM staging.transactions t
        LEFT JOIN staging.fraud_labels f ON t.transaction_id = f.transaction_id
        ON CONFLICT (transaction_id) DO NOTHING;
    """)
    
    print("✅ Data loaded to DW successfully")

def validate_data():
    """Validate loaded data"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
    result = postgres_hook.get_first("""
        SELECT 
            (SELECT COUNT(*) FROM dw.dim_customer) as customers,
            (SELECT COUNT(*) FROM dw.dim_account) as accounts,
            (SELECT COUNT(*) FROM dw.dim_branch) as branches,
            (SELECT COUNT(*) FROM dw.dim_channel) as channels,
            (SELECT COUNT(*) FROM dw.fact_transactions) as transactions,
            (SELECT COUNT(*) FROM dw.fact_transactions WHERE is_fraud = 1) as fraud_transactions
    """)
    
    print(f"\n📊 Data Validation Results:")
    print(f"   • Customers: {result[0]}")
    print(f"   • Accounts: {result[1]}")
    print(f"   • Branches: {result[2]}")
    print(f"   • Channels: {result[3]}")
    print(f"   • Transactions: {result[4]}")
    print(f"   • Fraud Transactions: {result[5]}")
    
    assert result[4] > 0, "No transactions loaded!"
    print("✅ Data validation passed!")

# Define tasks
task_create_staging = PythonOperator(
    task_id='create_staging_tables',
    python_callable=create_staging_tables,
    dag=dag,
)

task_load_staging = PythonOperator(
    task_id='load_csv_to_staging',
    python_callable=load_csv_to_staging,
    provide_context=True,
    dag=dag,
)

task_create_dw = PythonOperator(
    task_id='create_dw_tables',
    python_callable=create_dw_schema_and_tables,
    dag=dag,
)

task_transform_load = PythonOperator(
    task_id='transform_and_load_dw',
    python_callable=transform_and_load_dw,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

# Define task dependencies
task_create_staging >> task_load_staging >> task_create_dw >> task_transform_load >> task_validate