# NOVIA VALENTINA
---

# Final Project Data Pipeline — Banking Transaction Analytics

Repo ini berisi materi dan hands-on project **End-to-End ETL Pipeline dengan Apache Airflow v3 + PostgreSQL** menggunakan Docker Compose untuk kasus **Banking Transaction Analytics**.

---

## 📋 Deskripsi Case Study

Sebuah bank nasional ingin membangun Data Warehouse untuk menganalisis transaksi keuangan nasabah secara historis. Data bersumber dari public CSV datasets (Kaggle): Bank Transaction Dataset, Bank Customer Churn Dataset, dan Credit Card Fraud Detection Dataset.

Pipeline dibangun menggunakan pendekatan ETL klasik dengan Apache Airflow 3 sebagai orchestrator:
- **Extract** dari CSV 
- **Transform** ke staging schema 
- **Load** ke Star Schema DW

Semua data disimpan di PostgreSQL dalam 2 schema: 
- `staging` (transformasi sementara)
- `dw` (Star Schema final)

**Tujuan akhir**: menghasilkan dashboard analitik untuk tim Business Intelligence, Risk Management, dan C-Level yang menampilkan performa transaksi, profil nasabah, performa cabang, dan deteksi anomali.

---

## 🛠️ Tech Stack & Scope

| Komponen | Teknologi |
|---|---|
| **Orchestration** | Apache Airflow 3 (LocalExecutor) |
| **Sources** | CSV Files (Public Kaggle Datasets) |
| **Storage** | PostgreSQL (2 schemas: staging + dw) |
| **Transform** | PythonOperator + pandas + PostgresHook |
| **Data Model** | Star Schema (PostgreSQL schema dw) |
| **Visualization** | Looker · Power BI · Grafana |
| **Schedule** | 0 1 * * * (daily at 01:00 WIB) |

---

## 📊 Analytics Use Cases

### 01 Transaction Analytics
- Berapa total volume dan nilai transaksi per hari, minggu, dan bulan?
- Apa tren pertumbuhannya?
- **Tables**: dim_date, dim_channel, fact_transactions

### 02 Customer 360
- Siapa nasabah paling aktif berdasarkan frekuensi dan nilai transaksi?
- Bagaimana distribusi per segmen (Retail/Priority/VIP)?
- **Tables**: dim_customer, dim_account, fact_transactions

### 03 Branch Performance
- Cabang mana yang memiliki performa tertinggi berdasarkan jumlah transaksi dan total nilai transaksi per region?
- **Tables**: dim_branch, dim_date, fact_transactions

### 04 Channel Analysis
- Channel apa yang paling banyak digunakan nasabah (ATM, Mobile, Teller, Internet Banking)?
- Bagaimana tren migrasi ke digital?
- **Tables**: dim_channel, dim_date, fact_transactions

### 05 Product Performance
- Produk rekening mana (Tabungan/Giro/Deposito) yang menghasilkan volume transaksi dan saldo rata-rata tertinggi?
- **Tables**: dim_account, dim_date, fact_transactions

### 06 Risk & Fraud Detection
- Adakah transaksi anomali (nilai sangat besar, frekuensi tidak wajar, atau status FAILED berulang) yang perlu diwaspadai?
- **Tables**: dim_customer, dim_channel, fact_transactions

---

## 📁 Struktur Repo
├── dags/                          # Airflow DAG files
│   ├── dag_etl_banking_analytics.py
│   ├── dag_transaction_analytics.py
│   ├── dag_customer_360.py
│   ├── dag_branch_performance.py
│   ├── dag_channel_analysis.py
│   ├── dag_product_performance.py
│   └── dag_fraud_detection.py
├── include/
│   ├── dataset/                   # Source CSV files (generate dulu)
│   │   ├── transactions.csv
│   │   ├── accounts.csv
│   │   ├── customers.csv
│   │   ├── fraud_labels.csv
│   │   ├── branches.csv
│   │   ├── channels.csv
│   │   └── dim_date.csv
│   ├── script/                    # Python ETL scripts
│   │   ├── generate_banking_dataset.py
│   │   ├── etl_staging.py
│   │   └── etl_data_warehouse.py
│   └── sql/                       # SQL transform files
│       ├── staging/
│       │   ├── create_staging_tables.sql
│       │   └── load_staging_data.sql
│       └── dw/
│           ├── create_dim_tables.sql
│           ├── create_fact_tables.sql
│           └── transform_to_dw.sql
├── docker-compose.yaml
├── .env.example                   # Template environment variables
└── requirements.txt

include/sql/queries/
├── 01_transaction_analytics.sql
├── 02_customer_360.sql
├── 03_branch_performance.sql
├── 04_channel_analysis.sql
├── 05_product_performance.sql
├── 06_risk_fraud_detection.sql
├── analytics_queries.sql (master file dengan semua queries)
└── README.md (dokumentasi)


---

## 📦 Dataset yang Di-generate

Script `generate_banking_dataset.py` akan membuat 7 file CSV:

| File | Records | Destination Table | Kolom |
|---|---|---|---|
| **transactions.csv** | 50.000 | fact_transactions | transaction_id, account_id, customer_id, branch_id, channel_id, amount, type, status, transaction_date |
| **accounts.csv** | ~3.000 | dim_account | account_id, account_no, account_type, product_name, currency, status, customer_id |
| **customers.csv** | 2.000 | dim_customer | customer_id, full_name, gender, segment, city, province, credit_score, salary |
| **fraud_labels.csv** | ~432 | fraud flags | transaction_id, is_fraud, fraud_type, fraud_score, flagged_at |
| **branches.csv** | 25 | dim_branch | branch_id, branch_code, branch_name, city, province, region, branch_type |
| **channels.csv** | 6 | dim_channel | channel_id, channel_code, channel_name, channel_category, is_digital |
| **dim_date.csv** | 1.096 hari | dim_date | date_id, full_date, year, quarter, month, month_name, day_name, is_holiday (2023-2025) |

---

## 🚀 Cara Menjalankan di GitHub Codespace (Rekomendasi)

### 1. Fork Repo Ini

1. Buka halaman repo: [https://github.com/saipulrx/final_project_data_pipeline_bni_corpu](https://github.com/saipulrx/final_project_data_pipeline_bni_corpu)
2. Klik tombol **`Fork`** di pojok kanan atas
3. Pilih akun GitHub pribadi sebagai destination
4. Klik **`Create fork`**

### 2. Buka Codespace

Di halaman **repo fork milik Anda**, klik tombol **`Code`** → tab **`Codespaces`** → **`Create codespace on main`**.

Tunggu hingga environment selesai di-setup (sekitar 1-2 menit).

### 3. Setup Environment Variables

```bash
cp .env.example .env

Edit file .env sesuai kebutuhan:

```bash
# Minimal configuration:
AIRFLOW_UID=50000
ETL_POSTGRES_HOST=postgres
ETL_POSTGRES_PORT=5432
ETL_POSTGRES_DB=banking_dw
ETL_POSTGRES_USER=airflow
ETL_POSTGRES_PASSWORD=airflow

4. Jalankan Docker Compose


```bash

# Fresh start (hapus volume lama jika ada)
docker compose down -v

# Start semua services
docker compose up -d

Tunggu semua container healthy (sekitar 1-2 menit):

```bash
docker compose ps

Semua service harus berstatus healthy atau running.
5. Generate Dataset

pip install pandas
python include/script/generate_banking_dataset.py

Dataset akan tersimpan di include/dataset/.
6. Buka Airflow UI
Di panel Ports Codespace (tab bawah), cari port 8082 → klik ikon globe untuk buka di browser.

    Username: airflow
    Password: airflow

7. Setup Airflow Connection
Buka Admin → Connections → + lalu isi:

Field	                                  Value
Connection ID	                          postgres_banking_dw
Connection Type	                        Postgres
Host	                                  postgres (untuk Codespace) atau localhost (untuk lokal)
Database	                              banking_dw
Login	                                  airflow
Password	                              airflow
Port	                                  5432
Extra	                                  {"schema": "dw"}


Klik Save.
8. Jalankan DAG

    Buka halaman DAGs
    Pilih DAG yang ingin dijalankan (misal: dag_etl_banking_analytics)
    Klik tombol ▶ Trigger DAG
    Monitor eksekusi di tab Graph atau Logs
