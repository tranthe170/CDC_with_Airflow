-- Add new rows in the table
INSERT INTO data_model.products
SELECT 
    src.etl_key,
    src.product_id,
    src.product_name,
    src.cost,
    src.original_sale_price,
    src.discount,
    src.current_price,
    src.taxes,
    src.etl_start_date,
    src.etl_end_date,
    src.etl_current_ind,
    now() AS etl_timestamp
FROM temp.products src
LEFT JOIN data_model.products tgt
    ON src.product_id = tgt.product_id
WHERE tgt.product_id IS NULL;
