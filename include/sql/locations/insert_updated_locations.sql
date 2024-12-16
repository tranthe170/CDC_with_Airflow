INSERT INTO temp.locations (etl_key, location_id, name, county, state_code, stage, type, latitude, longitude, etl_start_date, etl_end_date, etl_current_ind, etl_timestamp)
SELECT
    MD5(toString(tgt.location_id) || toString(now())) AS etl_key,
    coalesce(l.location_id, tgt.location_id) AS location_id,
    coalesce(l.name, tgt.name) AS name,
    coalesce(l.county, tgt.county) AS county,
    coalesce(l.state_code, tgt.state_code) AS state_code,
    coalesce(l.stage, tgt.stage) AS stage,
    coalesce(l.type, tgt.type) AS type,
    coalesce(l.latitude, tgt.latitude) AS latitude,
    coalesce(l.longitude, tgt.longitude) AS name,
    today() AS etl_start_date,
    '9999-12-31' AS etl_end_date,
    'Y' AS etl_current_ind,
    now() AS etl_timestamp
FROM data_model.locations tgt
LEFT JOIN stage.locations l ON tgt.location_id = l.location_id
WHERE tgt.etl_current_ind = 'Y' 
AND (l.name IS NOT NULL OR l.county IS NOT NULL OR l.state_code IS NOT NULL OR l.stage IS NOT NULL OR l.type IS NOT NULL OR l.latitude IS NOT NULL OR l.longitude IS NOT NULL);