INSERT INTO temp.customers (etl_key, customer_id, customer_name, etl_start_date, etl_end_date, etl_current_ind, etl_timestamp)
SELECT
    MD5(toString(c.customer_id) || toString(now())) AS etl_key,
    c.customer_id,
    c.customer_name,
    today() AS etl_start_date,
    '9999-12-31' AS etl_end_date,
    'Y' AS etl_current_ind,
    now() AS etl_timestamp
FROM stage.customers c;