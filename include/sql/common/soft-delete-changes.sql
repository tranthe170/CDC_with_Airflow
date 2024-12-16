-- Only insert records from `stage_customer` where deleted_at is null
INSERT INTO temp_customer (etl_key, id, name, address, payment_method, etl_start_date, etl_end_date, etl_current_ind, etl_timestamp)
SELECT
    MD5(toString(c.id) || toString(now())) AS etl_key,
    c.id,
    c.name,
    ca.address,
    cp.payment_method,
    today() AS etl_start_date,
    '9999-12-31' AS etl_end_date,
    'Y' AS etl_current_ind,
    now() AS etl_timestamp
FROM stage_customer c
LEFT JOIN stage_customer_address ca ON c.id = ca.cust_id
LEFT JOIN stage_customer_payment cp ON c.id = cp.cust_id
WHERE c.deleted_at IS NULL;

-- End-date records in `data_model_customer` for soft-deleted entries
INSERT INTO data_model_customer
SELECT
    tgt.etl_key,
    tgt.id,
    tgt.name,
    tgt.address,
    tgt.payment_method,
    tgt.etl_start_date,
    yesterday() AS etl_end_date,
    'N' AS etl_current_ind,
    now() AS etl_timestamp
FROM data_model_customer tgt
JOIN stage_customer src ON tgt.id = src.id
WHERE tgt.etl_current_ind = 'Y'
AND src.deleted_at IS NOT NULL;
