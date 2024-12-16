import logging
import os
from datetime import datetime

from airflow.decorators import task
from apache.airflow.providers.clickhouse.operators.ClickhouseOperator import (
    ClickhouseOperator,
)

from airflow import DAG
from include.operators.app_operators import (
    CollectMetdataOperator,
    ExtractOperator,
    GetDAGConfOperator,
)

source_tables = ["customers", "products", "locations", "sales"]

with DAG(
    "custom_op_etl_db_to_db",
    start_date=datetime(2023, 8, 2),
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=os.path.join(os.getcwd(), "include", "sql"),
) as dag:

    # DAG Configuration Task
    dag_conf = GetDAGConfOperator(task_id="get_dag_conf")

    @task.branch
    def check_full_load(**context):
        # Pull the XCom value from the 'get_dag_conf' task
        dag_conf = context["ti"].xcom_pull(task_ids="get_dag_conf")

        if dag_conf is None:
            raise ValueError("XCom value for 'get_dag_conf' is None.")

        load_type = dag_conf.get("load_type")
        logging.info(f"Retrieved load_type: {load_type}")

        # Determine which branch to take
        if load_type == "full":
            # Return the task IDs for all full_transform tasks
            return ["full_transform_customers", "full_transform_products", "full_transform_locations", "truncate_data_model_sales"]
        elif load_type == "delta":
            # Return the task IDs for incremental load tasks (if implemented)
            return ["insert_new_customers", "insert_new_products", "insert_new_locations", "sales_incremental_append"]
        else:
            raise ValueError(f"Invalid load_type: {load_type}")

    check_load_type = check_full_load()

    for source_table in source_tables:
        extract = ExtractOperator(
            task_id=f"{source_table}_extract",
            source_table=source_table,
            load_type="{{ ti.xcom_pull(task_ids='get_dag_conf', key='load_type') }}",
            where_cond="{{ ti.xcom_pull(task_ids='get_dag_conf', key='where_cond') }}",
        )

        truncate_stage = ClickhouseOperator(
            task_id=f"truncate_{source_table}_stage",
            click_conn_id="clickhouse",
            sql=f"TRUNCATE TABLE stage.{source_table};",
        )

        insert_stage = ClickhouseOperator(
            task_id=f"insert_{source_table}_stage",
            click_conn_id="clickhouse",
            sql=f"""
            INSERT INTO stage.{source_table}
            SELECT *
            FROM s3('http://minio:9000/pg-data/{source_table}.csv', 'minio', 'minio123', 'CSV');
            """,
        )

        collect_metadata = CollectMetdataOperator(
            task_id=f"{source_table}_get_max_date",
            source_table=source_table,
        )

        truncate_temp = ClickhouseOperator(
            task_id=f"truncate_temp_{source_table}",
            click_conn_id="clickhouse",
            sql=f"TRUNCATE TABLE temp.{source_table};",
        )

        # Workflow logic for non-sales tables (apply SCD logic)
        if source_table != "sales":
            full_transform = ClickhouseOperator(
                task_id=f"full_transform_{source_table}",
                click_conn_id="clickhouse",
                sql=f"{source_table}/full_{source_table}_transform.sql",
            )

            backup_table = ClickhouseOperator(
                task_id=f"backup_{source_table}_table",
                click_conn_id="clickhouse",
                sql=f"{source_table}/backup_{source_table}_table.sql",
            )

            truncate_data_model_table = ClickhouseOperator(
                task_id=f"truncate_data_model_{source_table}",
                click_conn_id="clickhouse",
                sql=f"TRUNCATE TABLE data_model.{source_table};",
            )

            full_load_table = ClickhouseOperator(
                task_id=f"load_{source_table}_table",
                click_conn_id="clickhouse",
                sql=f"{source_table}/full_load_{source_table}_table.sql",
            )

            insert_new_records = ClickhouseOperator(
                task_id=f"insert_new_{source_table}",
                click_conn_id="clickhouse",
                sql=f"{source_table}/insert_new_{source_table}.sql",
            )

            insert_updated_records = ClickhouseOperator(
                task_id=f"insert_updated_{source_table}",
                click_conn_id="clickhouse",
                sql=f"{source_table}/insert_updated_{source_table}.sql",
            )

            cdc_deactivate = ClickhouseOperator(
                task_id=f"cdc_deactivate_{source_table}",
                click_conn_id="clickhouse",
                sql=f"{source_table}/cdc_deactivate_{source_table}.sql",
            )

            cdc_add_updated_rows = ClickhouseOperator(
                task_id=f"cdc_add_updated_{source_table}",
                click_conn_id="clickhouse",
                sql=f"{source_table}/cdc_insert_updated_{source_table}.sql",
            )

            cdc_add_new_rows = ClickhouseOperator(
                task_id=f"cdc_add_new_{source_table}",
                click_conn_id="clickhouse",
                sql=f"{source_table}/cdc_insert_new_{source_table}.sql",
            )

            # Staging Task Dependencies for SCD-enabled tables
            dag_conf >> extract >> truncate_stage >> insert_stage >> collect_metadata >> truncate_temp >> check_load_type

            # Full Load Flow (if load_type is "full")
            check_load_type >> full_transform >> backup_table >> truncate_data_model_table >> full_load_table

            # Incremental Load Flow (if load_type is "delta")
            check_load_type >> insert_new_records >> insert_updated_records >> cdc_deactivate >> cdc_add_updated_rows >> cdc_add_new_rows

        # Workflow logic for the sales table (simplified)
        else:
            truncate_data_model_sales = ClickhouseOperator(
                task_id=f"truncate_data_model_sales",
                click_conn_id="clickhouse",
                sql="TRUNCATE TABLE data_model.sales;",
            )

            sales_full_load = ClickhouseOperator(
                task_id=f"full_load_sales",
                click_conn_id="clickhouse",
                sql="sales/full_load_sales_table.sql",
            )

            sales_incremental_append = ClickhouseOperator(
                task_id=f"incremental_append_sales",
                click_conn_id="clickhouse",
                sql="sales/incremental_append_sales_table.sql",
            )

            dag_conf >> extract >> truncate_stage >> insert_stage >> collect_metadata >> truncate_temp >> check_load_type

            # Full Load Flow for sales
            check_load_type >> truncate_data_model_sales >> sales_full_load

            # Incremental Load Flow for sales
            check_load_type >> sales_incremental_append

