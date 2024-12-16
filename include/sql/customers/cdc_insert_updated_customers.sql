-- Step 1: Use a subquery to find the latest historical version (HWM logic)
INSERT INTO data_model.customers
SELECT 
    src.etl_key,
    src.customer_id,
    src.customer_name,
    src.etl_start_date,
    src.etl_end_date,
    src.etl_current_ind,
    now() AS etl_timestamp
FROM temp.customers src
JOIN (
    SELECT 
        customer_id,
        customer_name,
        etl_timestamp
    FROM (
        SELECT 
            customer_id,
            customer_name,
            etl_timestamp,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY etl_timestamp DESC) AS row_num
        FROM data_model.customers
        WHERE etl_current_ind = 'N'
    )
    WHERE row_num = 1  -- Keep only the latest historical record for each id
) tgt
ON src.customer_id = tgt.customer_id
WHERE md5(tgt.customer_name) != md5(src.customer_name);