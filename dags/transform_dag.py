from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack import SlackNotifier

import os

folder_names = ['population_trend', 'average_first_marriage_age', 'gender_income']

default_args = {
    'owner': 'kdk0411',
    'retries': 3,
}

with DAG(
    dag_id=os.path.splitext(os.path.basename(__file__))[0],
    default_args=default_args,
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
    
    for folder_name in folder_names:
    
        format_data = DockerOperator(
            task_id=f'process_{folder_name}',
            max_active_tis_per_dag=1,
            image='airflow/spark-app',
            container_name=f'{folder_name}',
            environment={
                'FOLDER_NAME': folder_name,
                'DAG_RUN_DATE': '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y-%m") }}'
            },
            api_version='auto',
            auto_remove=True,
            docker_url='tcp://docker-proxy:2375',
            network_mode='container:spark-master',
            task_concurrency=1,
            tty=True,
            mount_tmp_dir=False
        )
    
    trigger_load_dag = TriggerDagRunOperator(
        task_id='trigger_load_dag',
        trigger_dag_id='load_dag',
    )
    
    format_data >> trigger_load_dag