#!/usr/bin/env python3
"""
ETL Script: Load CSV data to staging tables
"""

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

def load_csv_to_staging(dataset_path='/opt/airflow/include/dataset'):
    """Load CSV files to staging tables"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_banking_dw')
    
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
          f"{len(channels_df)} channels, {len(fraud_df)} fraud labels, "
          f"{len(dim_date_df)} dates")

if __name__ == '__main__':
    load_csv_to_staging()