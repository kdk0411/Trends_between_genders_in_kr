from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
import requests
import json

class HTTPJsonSensor(BaseOperator):
    def __init__(self, conn_id, url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.url = url

    def execute(self, context):
        connection = BaseHook.get_connection(self.conn_id)
        endpoint = connection.host
        extra = connection.extra_dejson

        full_url = f"{endpoint}{extra['endpoint']}{self.url}"
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