from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from datetime import datetime

# PostgreSQL 연결 URI
DB_USER = 'urface'
DB_PASSWORD = 'urface'
DB_HOST = 'postgres'
DB_PORT = '5432'
DB_NAME = 'trend'
DB_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def log_db_info(**kwargs):
    # SQLAlchemy 엔진 생성
    try:
        engine = create_engine(DB_URI)
        print(f"Successfully created engine for database: {DB_NAME}")
        print(f"Database URI: {DB_URI}")
    except Exception as e:
        print(f"Error creating engine: {e}")

# DAG 정의
with DAG(
    dag_id='test_db_logging_dag',
    schedule_interval='@once',
    start_date=datetime(2024, 10, 30),
    catchup=False,
) as dag:
    
    log_db_task = PythonOperator(
        task_id='log_db_info_task',
        python_callable=log_db_info,
        provide_context=True,
    )

# 태스크 순서 정의
log_db_task