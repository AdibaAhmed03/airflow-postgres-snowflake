from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_and_load():
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    records = pg_hook.get_records("SELECT * FROM public.my_table")

    conn = sf_hook.get_conn()
    cursor = conn.cursor()

    for row in records:
        cursor.execute(
            "INSERT INTO my_table VALUES (%s, %s, %s)", row
        )

    cursor.close()
    conn.close()

with DAG(
    dag_id="postgres_to_snowflake",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id="transfer_postgres_to_snowflake",
        python_callable=extract_and_load,
    )
