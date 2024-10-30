from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.slack.notifications.slack import SlackNotifier

from custom.json import JsonExtractOperator
from custom.sensor import HTTPJsonSensor
from datetime import datetime
import os

default_args = {
    'owner': 'kdk0411',
    'start_date': datetime(2024, 10, 29),
    'retries': 3,
}

with DAG(
    dag_id=os.path.splitext(os.path.basename(__file__))[0],
    default_args=default_args,
    schedule_interval='0 0 30 * *',
    on_success_callback=SlackNotifier(
        text="{{ dag.dag_id }} DAG succeeded!", 
        channel="#monitoring", 
        slack_conn_id="slack"),
    on_failure_callback=SlackNotifier(
        text="{{ dag.dag_id }} DAG fail!",
        channel="#monitoring",
        slack_conn_id="slack",
    )
) as dag:

    urls = {
        'population_trend': Variable.get('population_trend_url'),
        'average_first_marriage_age': Variable.get('average_first_marriage_age_url'),
        'gender_income': Variable.get('gender_income_url')
    }

    # TaskGroup 정의
    with TaskGroup("json_extraction_group") as json_extraction_group:
        for key, url in urls.items():
            check_url = HTTPJsonSensor(
                task_id=f'Check_url_{key}',
                url=url,
                dag=dag,
            )

            json_extract = JsonExtractOperator(
                task_id=f'json_extract{key}',
                key=key,
                url=url,
                dag=dag,
            )

            check_url >> json_extract

    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_dag',
        trigger_dag_id='transform_dag',
    )
    
    json_extraction_group >> trigger_transform_dag
