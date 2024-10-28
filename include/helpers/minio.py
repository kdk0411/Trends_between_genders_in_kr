from minio import Minio
from airflow.hooks.base import BaseHook

def get_minio_client():
    minio = BaseHook.get_connection('minio').extra_dejson
    client = Minio(
        endpoint=minio['endpoint_url'].split('//')[1],
        access_key=minio['aws_access_key_id'],
        secret_key=minio['aws_secret_access_key'],
        secure=False
    )
    return client


import boto3
from botocore.client import Config

# MiniO 클라이언트 생성
def get_minio_client():
    client = boto3.client(
        's3',
        endpoint_url='http://minio-server:9000',  # MiniO 서버의 URL
        aws_access_key_id='your_access_key',
        aws_secret_access_key='your_secret_key',
        config=Config(signature_version='s3v4')
    )
    return client

# 사용 예
minio_client = get_minio_client()

# 버킷 생성
minio_client.create_bucket(Bucket='bucket_name')

# 로컬 파일을 MiniO 버킷에 업로드
minio_client.upload_file('local_file.txt', 'minio_folder/remote_file.txt', ExtraArgs={'Bucket': 'my-bucket'})

# 파일 객체를 MiniO 버킷에 업로드
with open('local_file.txt', 'rb') as data:
    minio_client.upload_fileobj(data, 'my-bucket', 'minio_folder/remote_file.txt')

# MiniO에서 특정 객체를 가져오기
response = minio_client.get_object(Bucket='my-bucket', Key='minio_folder/remote_file.txt')
file_content = response['Body'].read()

# MiniO 버킷 내의 객체 목록을 가져오기
response = minio_client.list_objects_v2(Bucket='my-bucket', Prefix='minio_folder/')
for obj in response.get('Contents', []):
    print(obj['Key'])

# MiniO 버킷에서 특정 객체를 삭제
minio_client.delete_object(Bucket='my-bucket', Key='minio_folder/remote_file.txt')

# MiniO 버킷 내에서 객체를 복사
minio_client.copy_object(Bucket='my-bucket', CopySource='my-bucket/minio_folder/remote_file.txt', Key='minio_folder/copied_file.txt')

# 객체의 메타데이터를 가져온다. 객체의 존재 여부를 확인하는 데 유용함
response = minio_client.head_object(Bucket='my-bucket', Key='minio_folder/remote_file.txt')
print(response['ContentLength'])

# 특정 객체에 대한 임시 URL을 생성하여 액세스를 제공
url = minio_client.generate_presigned_url('get_object', Params={'Bucket': 'my-bucket', 'Key': 'minio_folder/remote_file.txt'}, ExpiresIn=3600)
print(url)