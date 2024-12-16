-- End-date records that are marked as deleted in the `deleted_entries` table
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
JOIN (SELECT table_id FROM stage_deleted_entries WHERE table_name = 'customer') de ON tgt.id = de.table_id
WHERE tgt.etl_current_ind = 'Y';
