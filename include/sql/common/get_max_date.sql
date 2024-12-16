-- Retrieve max updated_at date from a given stage table in Clickhouse
SELECT max(updated_at) FROM stage_{stage_table};
