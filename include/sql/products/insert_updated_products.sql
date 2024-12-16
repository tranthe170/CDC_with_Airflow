INSERT INTO temp.products (etl_key, product_id, product_name, cost, original_sale_price, discount, current_price, taxes, etl_start_date, etl_end_date, etl_current_ind, etl_timestamp)
SELECT
    MD5(toString(tgt.product_id) || toString(now())) AS etl_key,
    coalesce(p.product_id, tgt.product_id) AS product_id,
    coalesce(p.product_name, tgt.product_name) AS product_name,
    coalesce(p.cost, tgt.cost) AS cost,
    coalesce(p.original_sale_price, tgt.original_sale_price) AS original_sale_price,
    coalesce(p.discount, tgt.discount) AS discount,
    coalesce(p.current_price, tgt.current_price) AS current_price,
    coalesce(p.taxes, tgt.taxes) AS taxes,
    today() AS etl_start_date,
    '9999-12-31' AS etl_end_date,
    'Y' AS etl_current_ind,
    now() AS etl_timestamp
FROM data_model.products tgt
LEFT JOIN stage.products p ON tgt.product_id = p.product_id
WHERE tgt.etl_current_ind = 'Y' 
AND (
    p.product_name IS NOT NULL OR
    p.cost IS NOT NULL OR
    p.original_sale_price IS NOT NULL OR
    p.discount IS NOT NULL OR
    p.current_price IS NOT NULL OR
    p.taxes IS NOT NULL
);
