INSERT INTO temp.customers (etl_key, customer_id, customer_name, etl_start_date, etl_end_date, etl_current_ind, etl_timestamp)
SELECT
    MD5(toString(tgt.customer_id) || toString(now())) AS etl_key,
    coalesce(c.customer_id, tgt.customer_id) AS customer_id,
    coalesce(c.customer_name, tgt.customer_name) AS customer_name,
    today() AS etl_start_date,
    '9999-12-31' AS etl_end_date,
    'Y' AS etl_current_ind,
    now() AS etl_timestamp
FROM data_model.customers tgt
LEFT JOIN stage.customers c ON tgt.customer_id = c.customer_id
WHERE tgt.etl_current_ind = 'Y' 
AND (c.customer_name IS NOT NULL);