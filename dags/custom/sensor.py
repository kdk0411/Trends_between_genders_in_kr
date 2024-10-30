from airflow.models import BaseOperator, TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException
from airflow.utils.session import create_session

import requests
import json
import os

def create_date(ds):
    formatted_date = ds[:7]  # YYYY-MM 형식으로 변환
    return formatted_date

class HTTPJsonSensor(BaseOperator):
    def __init__(self, url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url

    def execute(self, context):

        full_url = f"{self.url}"
        self.log.info(f"Sending request to URL: {full_url}")

        response = requests.get(full_url)

        if response.status_code != 200:
            self.log.error(f"Error: Received status code {response.status_code} for URL: {full_url}")
            raise AirflowException(f"Failed to fetch data from {full_url}")

        try:
            response_data = response.json()
            self.log.info(f"Received JSON response: {response_data}")
            return response_data
        except json.JSONDecodeError:
            self.log.error("Response is not in JSON format.")
            raise AirflowException("Failed to decode JSON response.")
        
class CustomJsonFileSensor(BaseOperator):
    def __init__(self, bucket_name, key,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.key = key
    
    def execute(self, context):
        ds = context['ds']
        dag_run_date = create_date(ds)
        file_name = self.key + '.json'
        file_path = os.path.join('s3://', self.bucket_name, dag_run_date, self.key, file_name)

        if not os.path.exists(file_path):
            self.log.info(f"{file_path} does not exist. Triggering previous DAG...")
            raise AirflowException(f"File {file_path} does not exist. Previous DAG triggered.")

        self.log.info(f"File {file_path} exists.")