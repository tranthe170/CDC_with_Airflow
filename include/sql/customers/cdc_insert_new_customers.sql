-- Add new rows in the table
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
LEFT JOIN data_model.customers tgt
    ON src.customer_id = tgt.customer_id
WHERE tgt.customer_id IS NULL;
