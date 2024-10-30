from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.slack.notifications.slack import SlackNotifier
from custom.load import LoadCsvOperator
import os

folder_names = ['population_trend', 'average_first_marriage_age', 'gender_income']

default_args = {
    'owner': 'kdk0411',
    'retries': 3,
}

with DAG(
    dag_id=os.path.splitext(os.path.basename(__file__))[0],
    default_args=default_args,
    schedule_interval=None,
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
    
    with TaskGroup("json_extraction_group") as load_to_dw_group:
        for folder_name in folder_names:
            
            load_to_dw = LoadCsvOperator(
                task_id=f'{folder_name}load_to_dw',
                db_user='airflow',
                db_password='airflow',
                db_host='postgres',
                db_port='5432',
                bucket_name='trend',
                key=folder_name,
            )
            
            load_to_dw
            
    load_to_dw_group
