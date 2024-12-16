INSERT INTO temp.locations (etl_key, location_id, name, county, state_code, state, type, latitude, longitude, etl_start_date, etl_end_date, etl_current_ind, etl_timestamp)
SELECT
    MD5(toString(l.location_id) || toString(now())) AS etl_key,
    l.location_id,
    l.name,
    l.county,
    l.state_code,
    l.state,
    l.type,
    l.latitude,
    l.longitude,
    today() AS etl_start_date,
    '9999-12-31' AS etl_end_date,
    'Y' AS etl_current_ind,
    now() AS etl_timestamp
FROM stage.locations l;
