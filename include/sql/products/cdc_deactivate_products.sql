ALTER TABLE data_model.products UPDATE
    etl_end_date = today() - 1,
    etl_current_ind = 'N',
    etl_timestamp = now()
WHERE product_id IN (
    SELECT src.product_id
    FROM temp.products src
    JOIN data_model.products tgt
        ON src.product_id = tgt.product_id
    WHERE tgt.etl_current_ind = 'Y'
      AND (
          md5(tgt.product_name) != md5(src.product_name) OR
          md5(tgt.cost) != md5(src.cost) OR
          md5(tgt.original_sale_price) != md5(src.original_sale_price) OR
          md5(tgt.discount) != md5(src.discount) OR
          md5(tgt.current_price) != md5(src.current_price) OR
          md5(tgt.taxes) != md5(src.taxes)
      )
)
AND etl_current_ind = 'Y';
