from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

import requests
import json

class JsonExtractOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self, bucket_name, url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.url = url
        
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
        self.log.info(f"Executing Json_Extract_Operator Guideline with param_1 : {self.bucket_name} and param_2 : {self.url}")
        
        self.check_bucket()
        
        try:
            response = requests.get(self.url)
            response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
            data = response.json()
        except requests.RequestException as e:
            self.log.error(f"Error fetching data from URL: {e}")
            raise
        except json.JSONDecodeError as e:
            self.log.error(f"Error decoding JSON: {e}")
            raise

        json_data = json.dumps(data).encode('utf-8')

        try:
            self.client.put_object(
                Bucket='trend',
                Key=f'{self.bucket_name}.json',
                Body=json_data,
                ContentType='application/json'
            )
            self.log.info(f"JSON 데이터가 '{self.bucket_name} 위치에 저장되었습니다.")
        except ClientError as e:
            self.log.error(f"Error uploading to MinIO: {e}")
            raise

    def check_bucket(self):
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.client.create_bucket(Bucket=self.bucket_name)
        