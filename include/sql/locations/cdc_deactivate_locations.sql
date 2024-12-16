ALTER TABLE data_model.locations UPDATE
    etl_end_date = today() - 1,
    etl_current_ind = 'N',
    etl_timestamp = now()
WHERE location_id IN (
    SELECT src.location_id
    FROM temp.locations src
    JOIN data_model.locations tgt
        ON src.location_id = tgt.location_id
    WHERE tgt.etl_current_ind = 'Y'
      AND (
          md5(tgt.customer_name) != md5(src.customer_name) OR
          md5(tgt.county) != md5(src.county) OR
          md5(tgt.state_code) != md5(src.state_code) OR
          md5(tgt.stage) != md5(src.stage) OR
          md5(tgt.type) != md5(src.type) OR
          md5(tgt.latitude) != md5(src.latitude) OR
          md5(tgt.longitude) != md5(src.longitude)
      )
)
AND etl_current_ind = 'Y';
