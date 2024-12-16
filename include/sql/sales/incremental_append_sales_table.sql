INSERT INTO data_model.sales
SELECT * FROM stage.sales
WHERE order_id NOT IN (SELECT order_id FROM data_model.sales);
