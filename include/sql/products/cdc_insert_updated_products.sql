-- Step 1: Use a subquery to find the latest historical version (HWM logic)
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
JOIN (
    SELECT 
        product_id,
        product_name,
        cost,
        original_sale_price,
        discount,
        current_price,
        taxes,
        etl_timestamp
    FROM (
        SELECT 
            product_id,
            product_name,
            cost,
            original_sale_price,
            discount,
            current_price,
            taxes,
            etl_timestamp,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY etl_timestamp DESC) AS row_num
        FROM data_model.products
        WHERE etl_current_ind = 'N'
    )
    WHERE row_num = 1  -- Keep only the latest historical record for each product_id
) tgt
ON src.product_id = tgt.product_id
WHERE md5(tgt.product_name || tgt.cost || tgt.original_sale_price || tgt.discount || tgt.current_price || tgt.taxes)
      != md5(src.product_name || src.cost || src.original_sale_price || src.discount || src.current_price || src.taxes);
