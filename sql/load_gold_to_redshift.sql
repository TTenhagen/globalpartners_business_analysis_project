-- Run after each Gold build (wired into the LoadRedshift state via sp_load_gold_tables)
-- Daily CLV snapshot: idempotent append of the current snapshot_date
DELETE FROM customer_clv_daily WHERE snapshot_date = CURRENT_DATE;
COPY customer_clv_daily
FROM 's3://globalpartners-raw/gold/customer_clv_daily/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/gp-redshift-copy-role'
FORMAT AS PARQUET;

-- Full-refresh tables: truncate + reload
TRUNCATE customer_rfm_segments;
COPY customer_rfm_segments
FROM 's3://globalpartners-raw/gold/customer_rfm_segments/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/gp-redshift-copy-role'
FORMAT AS PARQUET;

TRUNCATE customer_churn_indicators;
COPY customer_churn_indicators
FROM 's3://globalpartners-raw/gold/customer_churn_indicators/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/gp-redshift-copy-role'
FORMAT AS PARQUET;

TRUNCATE sales_trends_daily;
COPY sales_trends_daily
FROM 's3://globalpartners-raw/gold/sales_trends_daily/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/gp-redshift-copy-role'
FORMAT AS PARQUET;

TRUNCATE loyalty_program_comparison;
COPY loyalty_program_comparison
FROM 's3://globalpartners-raw/gold/loyalty_program_comparison/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/gp-redshift-copy-role'
FORMAT AS PARQUET;

TRUNCATE location_performance;
COPY location_performance
FROM 's3://globalpartners-raw/gold/location_performance/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/gp-redshift-copy-role'
FORMAT AS PARQUET;

TRUNCATE discount_effectiveness;
COPY discount_effectiveness
FROM 's3://globalpartners-raw/gold/discount_effectiveness/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/gp-redshift-copy-role'
FORMAT AS PARQUET;