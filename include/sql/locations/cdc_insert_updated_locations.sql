-- Step 1: Use a subquery to find the latest historical version (HWM logic)
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
JOIN (
    SELECT 
        location_id,
        name,
        county,
        state_code,
        stage,
        type,
        latitude,
        longitude,
        etl_timestamp
    FROM (
        SELECT 
            location_id,
            name,
            county,
            state_code,
            stage,
            type,
            latitude,
            longitude,
            etl_timestamp,
            ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY etl_timestamp DESC) AS row_num
        FROM data_model.locations
        WHERE etl_current_ind = 'N'
    )
    WHERE row_num = 1  -- Keep only the latest historical record for each location_id
) tgt
ON src.location_id = tgt.location_id
WHERE md5(tgt.name || tgt.county || tgt.state_code || tgt.stage || tgt.type || tgt.latitude || tgt.longitude)
      != md5(src.name || src.county || src.state_code || src.stage || src.type || src.latitude || src.longitude);
