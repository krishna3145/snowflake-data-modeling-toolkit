-- Gold layer: Fraud detection analytics mart
-- Kimball dimensional model — fact_transactions + dim_accounts

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    cluster_by=['transaction_date', 'account_id']
) }}

WITH transactions AS (
    SELECT * FROM {{ ref('int_transactions_cleaned') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

fraud_signals AS (
    SELECT
        transaction_id,
        account_id,
        amount,
        merchant_category,
        transaction_date,
        CASE
            WHEN amount > 10000 THEN 'HIGH_VALUE'
            WHEN merchant_category = 'UNKNOWN' THEN 'SUSPICIOUS_MERCHANT'
            ELSE 'NORMAL'
        END AS risk_flag,
        COUNT(*) OVER (
            PARTITION BY account_id
            ORDER BY transaction_date
            ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
        ) AS txn_velocity_24h
    FROM transactions
)

SELECT * FROM fraud_signals
