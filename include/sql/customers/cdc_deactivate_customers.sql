ALTER TABLE data_model.customers UPDATE
    etl_end_date = today() - 1,
    etl_current_ind = 'N',
    etl_timestamp = now()
WHERE customer_id IN (
    SELECT src.customer_id
    FROM temp.customers src
    JOIN data_model.customers tgt
        ON src.customer_id = tgt.customer_id
    WHERE tgt.etl_current_ind = 'Y'
      AND md5(tgt.customer_name) != md5(src.customer_name)
)
AND etl_current_ind = 'Y';