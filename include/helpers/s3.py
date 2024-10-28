from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# S3Hook를 사용하여 S3 클라이언트 생성
s3_client = S3Hook(aws_conn_id='my_aws_connection')

# 버킷 생성
s3_client.create_bucket(Bucket='bucket_name')

# 폴터 생성
s3_client.put_object(Bucket='bucket_name', Key='folder_name')

# 로컬 파일을 S3 버킷에 업로드
s3_client.load_file(filename='local_file.txt', key='s3_folder/remote_file.txt', bucket_name='my-bucket', replace=True)

# 문자열 데이터를 S3 버킷에 업로드
s3_client.load_string(string_data='Hello, S3!', key='s3_folder/hello.txt', bucket_name='my-bucket', replace=True)

# S3에서 파일 읽기
file_content = s3_client.read_key('s3_folder/remote_file.txt', bucket_name='my-bucket')

# S3 버킷 내에 특정 키가 존재하는지 확인
exists = s3_client.check_key(key='s3_folder/remote_file.txt', bucket_name='my-bucket')

# S3 버킷에서 하나 또는 여러 개의 객체를 삭제
s3_client.delete_objects(keys=['s3_folder/remote_file.txt'], bucket_name='my-bucket')

# S3 버킷 내의 특정 접두사에 해당하는 키 목록을 가져오기
keys = s3_client.list_keys(bucket_name='my-bucket', prefix='s3_folder/')

# S3 버킷 객체를 가져온다. 이 메서드는 S3 API에 대한 더 저수준의 접근을 허용
bucket = s3_client.get_bucket(bucket_name='my-bucket')
