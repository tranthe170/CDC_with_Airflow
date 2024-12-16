import logging
import os

from airflow.models import BaseOperator, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from clickhouse_driver import Client
from minio import Minio


class ExtractOperator(BaseOperator):

    template_fields = ["load_type", "where_cond"]

    def __init__(
        self, source_table: str, where_cond: str, load_type: str, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_table = source_table
        self.where_cond = where_cond
        self.load_type = load_type

    def execute(self, context: Context) -> None:
        # Connect to PostgreSQL
        pg_hook = PostgresHook(
            schema="sales_db",
            postgres_conn_id="postgres",
        )

        # Read SQL query from file
        sql_file_path = os.path.join(
            os.getcwd(), "include", "sql", f"{self.source_table}", f"{self.source_table}.sql"
        )
        with open(sql_file_path, "r") as sql_file:
            sql = sql_file.read()

        # Define output file path
        out_file = os.path.join(
            os.getcwd(), "include", "data", f"{self.source_table}.csv"
        )

        # Generate SQL query based on load type
        if self.load_type == "full":
            sql = f"COPY {self.source_table} TO STDOUT WITH CSV DELIMITER ','"
        else:
            max_updated = Variable.get(f"{self.source_table}_max_updated")
            if not max_updated:
                raise ValueError("MAX updated_at not found for Delta load! Aborting...")
            sql = sql.format(where_cond=self.where_cond).format(max_date=max_updated)
            sql = f"COPY ({sql}) TO STDOUT WITH CSV DELIMITER ','"

        # Execute PostgreSQL COPY command and save data to file
        pg_hook.copy_expert(sql, out_file)

        # Upload the file to Minio
        minio_client = Minio(
            endpoint="minio:9000",
            access_key="minio",
            secret_key="minio123",
            secure=False,
        )

        minio_client.fput_object(
            bucket_name="pg-data",
            object_name=f"{self.source_table}.csv",
            file_path=out_file,
        )
        logging.info(f"File {self.source_table}.csv uploaded to Minio.")


class CollectMetdataOperator(BaseOperator):
    def __init__(self, source_table: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_table = source_table

    def execute(self, context: Context) -> None:
        # Instantiate ClickHouse client
        client = Client(
            host="clickhouse-server",  # Replace with your ClickHouse host
            port=9000,  # Native protocol port
            user="default",  # Replace with your ClickHouse username if needed
            password="",  # Replace with your ClickHouse password if needed
        )

        # SQL query to fetch max(updated_at)
        sql = f"SELECT max(updated_at) FROM stage.{self.source_table};"

        # Execute the query
        result = client.execute(sql)
        max_date = result[0][0] if result else None

        # Log and store the metadata in Airflow variables
        if max_date:
            Variable.set(
                f"{self.source_table}_max_updated",
                max_date.strftime("%Y-%m-%d %H:%M:%S"),
            )
            self.log.info(f"Max updated_at for {self.source_table}: {max_date}")
        else:
            self.log.warning(f"No data found in stage_{self.source_table}")


class GetDAGConfOperator(BaseOperator):

    def execute(self, context: Context) -> dict:
        # Fetch the configuration
        dag_conf = Variable.get("customer", deserialize_json=True, default_var={})
        load_type = dag_conf.get("load_type", None)

        # Log configuration and run type
        logging.info(f"Fetched DAG Configuration: {dag_conf}")
        logging.info(f"DAG Run Type: {context['dag_run'].run_type}")

        # Validate load type
        if context["dag_run"].run_type == "scheduled" and load_type == "full":
            raise ValueError("Full run can't be scheduled! Aborting...")

        if load_type == "full":
            where_cond = None
        elif load_type == "delta":
            where_cond = " where updated_at > '{max_date}'"
        else:
            raise ValueError("Invalid load type specified in configuration.")

        # Log the constructed condition
        logging.info(f"Constructed where_cond: {where_cond}")

        # Return the constructed configuration for XCom
        return {"where_cond": where_cond, "load_type": load_type}
