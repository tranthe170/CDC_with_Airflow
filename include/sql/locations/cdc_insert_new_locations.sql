-- Add new rows in the table
INSERT INTO data_model.locations
SELECT 
    src.etl_key,
    src.location_id,
    src.name,
    src.county,
    src.state_code,
    src.stage,
    src.type,
    src.latitude,
    src.longitude,
    src.etl_start_date,
    src.etl_end_date,
    src.etl_current_ind,
    now() AS etl_timestamp
FROM temp.locations src
LEFT JOIN data_model.locations tgt
    ON src.location_id = tgt.location_id
WHERE tgt.location_id IS NULL;
