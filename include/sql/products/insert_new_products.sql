INSERT INTO temp.products (etl_key, product_id, product_name, cost, original_sale_price, discount, current_price, taxes, etl_start_date, etl_end_date, etl_current_ind, etl_timestamp)
SELECT
    MD5(p.product_id || toString(now())) AS etl_key,
    p.product_id,
    p.product_name,
    p.cost,
    p.original_sale_price,
    p.discount,
    p.current_price,
    p.taxes,
    today() AS etl_start_date,
    '9999-12-31' AS etl_end_date,
    'Y' AS etl_current_ind,
    now() AS etl_timestamp
FROM stage.products p
LEFT JOIN data_model.products tgt
    ON p.product_id = tgt.product_id
WHERE tgt.product_id IS NULL;
