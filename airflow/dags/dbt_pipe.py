from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id="dbt_pipeline",
    start_date=datetime(2026, 3, 19),
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end',
                        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
                        )


    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="dbt run --project-dir /opt/airflow/dbt_listings --profiles-dir /root/.dbt"
    )
    start >> run_dbt >> end