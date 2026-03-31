from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowException
import os
import snowflake.connector
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

SNOWFLAKE_CONN_ID = "snowflake_default"
LOCAL_FILE_PATTERN = "/opt/data/raw/listing_events/*.parquet"

DATABASE = "PROJECT_DB"
SCHEMA = "BRONZE"
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"
STAGE_NAME = "INTERNAL_LOAD_STAGE"
FILE_FORMAT_NAME = "PARQUET_FORMAT"
TABLE_NAME = "RAW_LISTINGS"


def _get_snowflake_connection():
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
    ]

    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        raise AirflowException(
            f"Missing required Snowflake environment variables: {', '.join(missing)}"
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role=ROLE,
    )


def put_files_to_stage():
    conn = _get_snowflake_connection()
    cur = conn.cursor()

    try:
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"USE SCHEMA {SCHEMA}")
        cur.execute(f"USE WAREHOUSE {WAREHOUSE}")

        put_sql = (
            f"PUT file://{LOCAL_FILE_PATTERN} "
            f"@{STAGE_NAME} "
            f"AUTO_COMPRESS=FALSE "
            f"OVERWRITE=TRUE"
        )

        result = cur.execute(put_sql).fetchall()

        if not result:
            raise AirflowException("PUT command returned no results.")

        uploaded = 0
        for row in result:
            row_text = " | ".join("" if v is None else str(v) for v in row).lower()
            if "uploaded" in row_text:
                uploaded += 1

        if uploaded == 0:
            raise AirflowException(
                f"PUT completed but no files were uploaded. "
                f"Check whether files exist at {LOCAL_FILE_PATTERN} inside the container."
            )

        print(f"Uploaded {uploaded} file(s) to @{STAGE_NAME}")

    finally:
        cur.close()
        conn.close()




with DAG(
    dag_id="full_pipeline",
    start_date=datetime(2026, 3, 19),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end',
                        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
                        )
    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="python /opt/airflow/kafka/producer.py --num-events 50000"
    )

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/airflow/spark/stream_consumer.py \
    --bootstrap-servers kafka:9092 \
    --duration 30"
    )
    test_connection = SQLExecuteQueryOperator(
        task_id="test_connection",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        SELECT
            CURRENT_ACCOUNT() AS account_name,
            CURRENT_USER() AS user_name,
            CURRENT_ROLE() AS role_name,
            CURRENT_WAREHOUSE() AS warehouse_name,
            CURRENT_DATABASE() AS database_name,
            CURRENT_SCHEMA() AS schema_name;
        """,
    )

    create_stage_objects = SQLExecuteQueryOperator(
        task_id="create_stage_objects",
        conn_id=SNOWFLAKE_CONN_ID,
        split_statements=True,
        sql=f"""
        USE ROLE {ROLE};
        USE WAREHOUSE {WAREHOUSE};
        USE DATABASE {DATABASE};
        USE SCHEMA {SCHEMA};

        CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT_NAME}
            TYPE = 'PARQUET';

        CREATE STAGE IF NOT EXISTS {STAGE_NAME}
            FILE_FORMAT = {FILE_FORMAT_NAME}
            COMMENT = 'Internal stage for loading Parquet files';

        CREATE OR REPLACE TABLE {TABLE_NAME} (
            ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            source_file STRING,
            raw_data VARIANT
        );
        """,
    )

    upload_to_stage = PythonOperator(
        task_id="upload_to_stage",
        python_callable=put_files_to_stage,
    )

    verify_stage_has_files = SQLExecuteQueryOperator(
        task_id="verify_stage_has_files",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        USE ROLE {ROLE};
        USE WAREHOUSE {WAREHOUSE};
        USE DATABASE {DATABASE};
        USE SCHEMA {SCHEMA};

        LIST @{STAGE_NAME};
        """,
    )

    copy_into_raw_listings = SQLExecuteQueryOperator(
        task_id="copy_into_raw_listings",
        conn_id=SNOWFLAKE_CONN_ID,
        split_statements=True,
        sql=f"""
        USE ROLE {ROLE};
        USE WAREHOUSE {WAREHOUSE};
        USE DATABASE {DATABASE};
        USE SCHEMA {SCHEMA};

        COPY INTO {TABLE_NAME} (ingestion_ts, source_file, raw_data)
        FROM (
            SELECT
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                METADATA$FILENAME,
                TO_VARIANT(OBJECT_CONSTRUCT(*))
            FROM @{STAGE_NAME}
            (FILE_FORMAT => '{FILE_FORMAT_NAME}')
        )
        ON_ERROR = 'CONTINUE'
        PURGE = TRUE;
        """,
    )

    verify_raw_listings_loaded = SQLExecuteQueryOperator(
        task_id="verify_raw_listings_loaded",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        USE ROLE {ROLE};
        USE WAREHOUSE {WAREHOUSE};
        USE DATABASE {DATABASE};
        USE SCHEMA {SCHEMA};

        SELECT COUNT(*) AS row_count
        FROM {TABLE_NAME};
        """,
    )

    seed_dbt = BashOperator(
        task_id="seed_dbt",
        bash_command="dbt seed --project-dir /opt/airflow/dbt_listings --profiles-dir /root/.dbt"
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="dbt run --project-dir /opt/airflow/dbt_listings --profiles-dir /root/.dbt"
    )
    start >> [run_producer,run_consumer] >> test_connection >> create_stage_objects >> upload_to_stage >> verify_stage_has_files >> copy_into_raw_listings >> verify_raw_listings_loaded >> seed_dbt >> run_dbt >> end