
## 2. Project Files

Now let me create the essential project files:

### File: `include/script/generate_banking_dataset.py`

```python
#!/usr/bin/env python3
"""
Script to generate synthetic banking dataset for ETL pipeline.
Generates 7 CSV files with realistic banking data.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Create dataset directory
os.makedirs('include/dataset', exist_ok=True)

print(" Generating Banking Dataset...")
print("=" * 50)

# 1. Generate dim_date.csv (2023-2025)
print("\n📅 Generating dim_date.csv...")
date_range = pd.date_range(start='2023-01-01', end='2025-12-31', freq='D')
dim_date = pd.DataFrame({
    'date_id': range(1, len(date_range) + 1),
    'full_date': date_range,
    'year': date_range.year,
    'quarter': date_range.quarter,
    'month': date_range.month,
    'month_name': date_range.strftime('%B'),
    'day_name': date_range.strftime('%A'),
    'is_holiday': np.where((date_range.dayofweek >= 5) | 
                          (date_range.month == 12) | 
                          (date_range.month == 1), 1, 0)
})
dim_date.to_csv('include/dataset/dim_date.csv', index=False)
print(f"   ✓ Generated {len(dim_date)} date records (2023-2025)")

# 2. Generate dim_customer.csv
print("\n👥 Generating dim_customer.csv...")
n_customers = 2000
first_names = ['Ahmad', 'Budi', 'Citra', 'Dewi', 'Eko', 'Fitri', 'Gunawan', 'Hani', 
               'Indra', 'Joko', 'Kartika', 'Lina', 'Made', 'Nina', 'Oki', 'Putu',
               'Rudi', 'Siti', 'Tono', 'Utami', 'Wawan', 'Yuni', 'Zainal', 'Anisa', 'Bambang']
last_names = ['Santoso', 'Wijaya', 'Kusuma', 'Putri', 'Nugroho', 'Lestari', 'Pratama', 
              'Sari', 'Hidayat', 'Wulandari', 'Rahman', 'Permata', 'Setiawan', 'Agustina']

customers = []
for i in range(1, n_customers + 1):
    first = random.choice(first_names)
    last = random.choice(last_names)
    segment = random.choices(['Retail', 'Priority', 'VIP'], weights=[0.7, 0.2, 0.1])[0]
    
    if segment == 'Retail':
        credit_score = random.randint(300, 700)
        salary = random.randint(3000000, 15000000)
    elif segment == 'Priority':
        credit_score = random.randint(650, 800)
        salary = random.randint(15000000, 50000000)
    else:  # VIP
        credit_score = random.randint(750, 850)
        salary = random.randint(50000000, 200000000)
    
    customers.append({
        'customer_id': i,
        'full_name': f"{first} {last}",
        'gender': random.choice(['Male', 'Female']),
        'segment': segment,
        'city': random.choice(['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Semarang', 
                              'Makassar', 'Palembang', 'Tangerang', 'Depok', 'Bekasi']),
        'province': random.choice(['DKI Jakarta', 'Jawa Barat', 'Jawa Timur', 'Jawa Tengah', 
                                  'Sumatera Utara', 'Sulawesi Selatan', 'Sumatera Selatan']),
        'credit_score': credit_score,
        'salary': salary
    })

dim_customer = pd.DataFrame(customers)
dim_customer.to_csv('include/dataset/customers.csv', index=False)
print(f"   ✓ Generated {len(dim_customer)} customer records")

# 3. Generate dim_branch.csv
print("\n🏢 Generating dim_branch.csv...")
branches_data = [
    ('B001', 'Jakarta Pusat', 'DKI Jakarta', 'Jakarta', 'Urban'),
    ('B002', 'Jakarta Selatan', 'DKI Jakarta', 'Jakarta', 'Urban'),
    ('B003', 'Surabaya', 'Jawa Timur', 'Surabaya', 'Urban'),
    ('B004', 'Bandung', 'Jawa Barat', 'Bandung', 'Urban'),
    ('B005', 'Medan', 'Sumatera Utara', 'Medan', 'Urban'),
    ('B006', 'Semarang', 'Jawa Tengah', 'Semarang', 'Urban'),
    ('B007', 'Makassar', 'Sulawesi Selatan', 'Makassar', 'Urban'),
    ('B008', 'Palembang', 'Sumatera Selatan', 'Palembang', 'Urban'),
    ('B009', 'Tangerang', 'Banten', 'Jakarta', 'Suburban'),
    ('B010', 'Depok', 'Jawa Barat', 'Jakarta', 'Suburban'),
    ('B011', 'Bekasi', 'Jawa Barat', 'Jakarta', 'Suburban'),
    ('B012', 'Bogor', 'Jawa Barat', 'Bandung', 'Suburban'),
    ('B013', 'Malang', 'Jawa Timur', 'Surabaya', 'Suburban'),
    ('B014', 'Yogyakarta', 'DI Yogyakarta', 'Semarang', 'Urban'),
    ('B015', 'Denpasar', 'Bali', 'Denpasar', 'Urban'),
    ('B016', 'Balikpapan', 'Kalimantan Timur', 'Balikpapan', 'Urban'),
    ('B017', 'Banjarmasin', 'Kalimantan Selatan', 'Banjarmasin', 'Urban'),
    ('B018', 'Manado', 'Sulawesi Utara', 'Manado', 'Urban'),
    ('B019', 'Padang', 'Sumatera Barat', 'Padang', 'Urban'),
    ('B020', 'Pekanbaru', 'Riau', 'Pekanbaru', 'Urban'),
    ('B021', 'Batam', 'Kepulauan Riau', 'Batam', 'Urban'),
    ('B022', 'Samarinda', 'Kalimantan Timur', 'Samarinda', 'Urban'),
    ('B023', 'Jambi', 'Jambi', 'Jambi', 'Rural'),
    ('B024', 'Bengkulu', 'Bengkulu', 'Bengkulu', 'Rural'),
    ('B025', 'Ambon', 'Maluku', 'Ambon', 'Rural'),
]

dim_branch = pd.DataFrame(branches_data, 
                          columns=['branch_code', 'branch_name', 'province', 'city', 'region'])
dim_branch['branch_id'] = range(1, len(dim_branch) + 1)
dim_branch['branch_type'] = dim_branch['region'].map({
    'Urban': 'Main Branch',
    'Suburban': 'Sub Branch',
    'Rural': 'Mini Branch'
})
dim_branch = dim_branch[['branch_id', 'branch_code', 'branch_name', 'city', 'province', 
                         'region', 'branch_type']]
dim_branch.to_csv('include/dataset/branches.csv', index=False)
print(f"   ✓ Generated {len(dim_branch)} branch records")

# 4. Generate dim_channel.csv
print("\n📱 Generating dim_channel.csv...")
dim_channel = pd.DataFrame({
    'channel_id': [1, 2, 3, 4, 5, 6],
    'channel_code': ['ATM', 'MOB', 'TEL', 'INT', 'BRN', 'POS'],
    'channel_name': ['ATM', 'Mobile Banking', 'Teller', 'Internet Banking', 'Branch Office', 'POS'],
    'channel_category': ['Digital', 'Digital', 'Traditional', 'Digital', 'Traditional', 'Digital'],
    'is_digital': [1, 1, 0, 1, 0, 1]
})
dim_channel.to_csv('include/dataset/channels.csv', index=False)
print(f"   ✓ Generated {len(dim_channel)} channel records")

# 5. Generate dim_account.csv
print("\n💳 Generating dim_account.csv...")
n_accounts = 3000
accounts = []
for i in range(1, n_accounts + 1):
    customer_id = random.randint(1, n_customers)
    customer_segment = dim_customer[dim_customer['customer_id'] == customer_id]['segment'].values[0]
    
    if customer_segment == 'VIP':
        product = random.choices(['Tabungan', 'Giro', 'Deposito'], weights=[0.3, 0.3, 0.4])[0]
    elif customer_segment == 'Priority':
        product = random.choices(['Tabungan', 'Giro', 'Deposito'], weights=[0.5, 0.3, 0.2])[0]
    else:
        product = random.choices(['Tabungan', 'Giro', 'Deposito'], weights=[0.8, 0.15, 0.05])[0]
    
    accounts.append({
        'account_id': i,
        'account_no': f"{'0' * (10 - len(str(i)))}{i}",
        'account_type': random.choice(['Saving', 'Checking']),
        'product_name': product,
        'currency': 'IDR',
        'status': random.choices(['Active', 'Dormant', 'Closed'], weights=[0.85, 0.1, 0.05])[0],
        'customer_id': customer_id
    })

dim_account = pd.DataFrame(accounts)
dim_account.to_csv('include/dataset/accounts.csv', index=False)
print(f"   ✓ Generated {len(dim_account)} account records")

# 6. Generate fraud_labels.csv
print("\n🚨 Generating fraud_labels.csv...")
n_transactions = 50000
n_fraud = 432  # ~0.86% fraud rate

fraud_transactions = random.sample(range(1, n_transactions + 1), n_fraud)
fraud_types = ['Unauthorized Transaction', 'Card Skimming', 'Phishing', 
               'Account Takeover', 'Identity Theft', 'Money Laundering']

fraud_labels = []
for txn_id in fraud_transactions:
    fraud_labels.append({
        'transaction_id': txn_id,
        'is_fraud': 1,
        'fraud_type': random.choice(fraud_types),
        'fraud_score': round(random.uniform(0.7, 1.0), 2),
        'flagged_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

fraud_df = pd.DataFrame(fraud_labels)
fraud_df.to_csv('include/dataset/fraud_labels.csv', index=False)
print(f"   ✓ Generated {len(fraud_df)} fraud flag records")

# 7. Generate fact_transactions.csv
print("\n💰 Generating fact_transactions.csv...")
transactions = []

for i in range(1, n_transactions + 1):
    date_id = random.randint(1, len(dim_date))
    account_id = random.randint(1, n_accounts)
    customer_id = dim_account[dim_account['account_id'] == account_id]['customer_id'].values[0]
    branch_id = random.randint(1, len(dim_branch))
    channel_id = random.randint(1, len(dim_channel))
    
    # Transaction amount based on customer segment
    customer_segment = dim_customer[dim_customer['customer_id'] == customer_id]['segment'].values[0]
    
    if customer_segment == 'VIP':
        amount = round(random.lognormvariate(15, 2), 2)  # Higher amounts
    elif customer_segment == 'Priority':
        amount = round(random.lognormvariate(13, 2), 2)
    else:
        amount = round(random.lognormvariate(11, 2), 2)
    
    # Cap at reasonable maximum
    amount = min(amount, 500000000)
    
    txn_type = random.choices(['Credit', 'Debit'], weights=[0.4, 0.6])[0]
    
    # Fraud transactions more likely to fail or have unusual patterns
    if i in fraud_transactions:
        status = random.choices(['SUCCESS', 'FAILED', 'PENDING'], weights=[0.3, 0.5, 0.2])[0]
    else:
        status = random.choices(['SUCCESS', 'FAILED', 'PENDING'], weights=[0.92, 0.05, 0.03])[0]
    
    transaction_date = dim_date[dim_date['date_id'] == date_id]['full_date'].values[0]
    transaction_date = transaction_date + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    
    transactions.append({
        'transaction_id': i,
        'account_id': account_id,
        'customer_id': customer_id,
        'branch_id': branch_id,
        'channel_id': channel_id,
        'amount': amount,
        'type': txn_type,
        'status': status,
        'transaction_date': transaction_date.strftime('%Y-%m-%d %H:%M:%S')
    })

fact_transactions = pd.DataFrame(transactions)
fact_transactions.to_csv('include/dataset/transactions.csv', index=False)
print(f"   ✓ Generated {len(fact_transactions)} transaction records")

print("\n" + "=" * 50)
print("✅ Dataset generation completed successfully!")
print("\n📊 Summary:")
print(f"   • dim_date: {len(dim_date)} records (2023-2025)")
print(f"   • dim_customer: {len(dim_customer)} records")
print(f"   • dim_branch: {len(dim_branch)} records")
print(f"   • dim_channel: {len(dim_channel)} records")
print(f"   • dim_account: {len(dim_account)} records")
print(f"   • fact_transactions: {len(fact_transactions)} records")
print(f"   • fraud_labels: {len(fraud_df)} records")
print("\n📁 Files saved to: include/dataset/")