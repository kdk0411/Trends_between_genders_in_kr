from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from botocore.exceptions import ClientError
from airflow.hooks.base_hook import BaseHook

import boto3
from botocore.client import Config
import requests
import json

def create_date(ds):
    formatted_date = ds[:7]  # YYYY-MM 형식으로 변환
    return formatted_date

class APISensor(BaseOperator):
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

        connection = BaseHook.get_connection('minio')
        endpoint_url = connection.extra_dejson.get('endpoint_url')
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=connection.login,
            aws_secret_access_key=connection.password,
            config=Config(signature_version='s3v4')
        )
    
    def execute(self, context):
        ds = context['ds']
        dag_run_date = create_date(ds)
        file_name = self.key + '.json'
        file_path = f'{dag_run_date}/{self.key}/{file_name}'

        self.client.head_object(Bucket=self.bucket_name, Key=file_path)

        try:
            self.client.head_object(Bucket=self.bucket_name, Key=file_path)
            self.log.info(f'File {file_path} exists in bucket {self.bucket_name}.')
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.log.error(f'File {file_path} does not exist in bucket {self.bucket_name}. Failing the DAG.')
                raise Exception(f'File {file_path} does not exist.')
            else:
                self.log.error(f'Error occurred: {e}')
                raise