from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook

import boto3
from botocore.client import Config
from sqlalchemy import create_engine, exc, Table, Column, Integer, Float, Date, MetaData, String
from sqlalchemy.schema import PrimaryKeyConstraint

import pandas as pd
import os

def create_date(ds):
    formatted_date = ds[:7]
    return formatted_date

class LoadCsvOperator(BaseOperator):
    @apply_defaults
    def __init__(self, db_user, db_password, db_host, db_port, bucket_name, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
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

        self.metadata = MetaData(schema='public')
        self.define_table_structure()

    def execute(self, context):
        ds = context['ds']
        dag_run_date = create_date(ds)
        self.prefix = os.path.join(dag_run_date, self.key, 'csv')

        objects = self.client.list_objects(Bucket=self.bucket_name, Prefix=self.prefix).get('Contents', [])
        csv_file_name = [obj['Key'] for obj in objects if obj['Key'].endswith('.csv')][0]

        data = self.client.get_object(Bucket=self.bucket_name, Key=csv_file_name)
        df = pd.read_csv(data['Body'])

        self.log.info("DataFrame columns before inserting: %s", df.columns)
        self.log.info("DataFrame head before inserting: %s", df.head())

        db_uri = f'postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/airflow'
        self.log.info("Database URI: %s", db_uri)
        engine = create_engine(db_uri)
        
        self.metadata.create_all(engine)
        df.to_sql(self.key, engine, if_exists='append', index=False)

    def define_table_structure(self):
        if self.key == 'population_trend':
            self.table = Table(
                self.key,
                self.metadata,
                Column('date', Integer, nullable=False),
                Column('value', Integer),
                Column('gender', String, nullable=False),
                Column('country', String),
                Column('category', String),
                Column('trend_id', String, primary_key=True)
            )
        elif self.key == 'average_first_marriage_age':
            self.table = Table(
                self.key,
                self.metadata,
                Column('date', Integer, nullable=False),
                Column('gender', String, nullable=False),
                Column('age', Float),
                Column('country', String),
                Column('age_id', String, primary_key=True)
            )
        elif self.key == 'gender_income':
            self.table = Table(
                self.key,
                self.metadata,
                Column('date', Integer, nullable=False),
                Column('category', String),
                Column('unit', String),
                Column('employment_type', String),
                Column('gender', String, nullable=False),
                Column('value', Float),
                Column('income_id', String, primary_key=True)
            )