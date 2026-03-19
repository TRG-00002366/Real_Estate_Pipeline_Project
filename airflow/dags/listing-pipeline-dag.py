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

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/airflow/spark/stream_consumer.py \
    --bootstrap-servers kafka:9092 \
    --duration 30"
    )

    run_batch_rdd = BashOperator(
        task_id="run_batch_rdd",
        bash_command="spark-submit \
    /opt/airflow/spark/batch_rdd_etl.py"
    )

    start >> run_producer >> run_consumer >> run_batch_rdd >> end