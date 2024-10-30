from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.minio.hooks.minio import MinioHook
import pandas as pd

class LoadOperator(BaseOperator):
    @apply_defaults
    def __init__(self, minio_bucket, minio_key, postgres_table, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.minio_bucket = minio_bucket
        self.minio_key = minio_key
        self.postgres_table = postgres_table

    def execute(self, context):
        # MinIO Hook을 사용하여 파일 다운로드
        minio_hook = MinioHook(minio_conn_id='minio')
        file_path = minio_hook.download_file(self.minio_bucket, self.minio_key)

        # CSV 파일을 Pandas DataFrame으로 읽기
        df = pd.read_csv(file_path)

        # PostgreSQL Hook을 사용하여 데이터 적재
        postgres_hook = PostgresHook(postgres_conn_id='postgres')
        df.to_sql(self.postgres_table, postgres_hook.get_sqlalchemy_engine(), if_exists='append', index=False)

        self.log.info(f"Loaded {len(df)} rows into {self.postgres_table} from {self.minio_bucket}/{self.minio_key}.")
