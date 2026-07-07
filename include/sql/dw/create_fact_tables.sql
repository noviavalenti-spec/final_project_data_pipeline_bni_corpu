-- Drop fact table if exists
DROP TABLE IF EXISTS dw.fact_transactions;

-- Create fact table
CREATE TABLE dw.fact_transactions (
    transaction_key SERIAL PRIMARY KEY,
    transaction_id INTEGER NOT NULL UNIQUE,
    date_key INTEGER NOT NULL REFERENCES dw.dim_date(date_key),
    customer_key INTEGER NOT NULL REFERENCES dw.dim_customer(customer_key),
    account_key INTEGER NOT NULL REFERENCES dw.dim_account(account_key),
    branch_key INTEGER NOT NULL REFERENCES dw.dim_branch(branch_key),
    channel_key INTEGER NOT NULL REFERENCES dw.dim_channel(channel_key),
    amount DECIMAL(15,2) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
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
CREATE INDEX idx_fact_transactions_fraud ON dw.fact_transactions(is_fraud);
CREATE INDEX idx_fact_transactions_status ON dw.fact_transactions(status);
CREATE INDEX idx_fact_transactions_amount ON dw.fact_transactions(amount);
CREATE INDEX idx_fact_transactions_type ON dw.fact_transactions(transaction_type);

-- Create composite indexes for common queries
CREATE INDEX idx_fact_transactions_date_customer ON dw.fact_transactions(date_key, customer_key);
CREATE INDEX idx_fact_transactions_date_branch ON dw.fact_transactions(date_key, branch_key);
CREATE INDEX idx_fact_transactions_date_channel ON dw.fact_transactions(date_key, channel_key);
CREATE INDEX idx_fact_transactions_date_product ON dw.fact_transactions(date_key, account_key);