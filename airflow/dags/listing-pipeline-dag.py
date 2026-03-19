from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

with DAG(
    dag_id="run_pipeline",
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
        bash_command="python /opt/airflow/kafka/producer.py --num-events 15"
    )

    start >> run_producer >> end