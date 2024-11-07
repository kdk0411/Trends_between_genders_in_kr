# 청년층 혼인 인식 및 출산 동향 시각화

## 프로젝트 소개

- 최근 청년층 사이에서 성별간 이슈가 끊이지 않는 이유를 파악하고자 하였습니다.
- 성별간 혼인수, 이혼수, 빈부격차 데이터를 추출하여 최근 젠더이슈의 근간을 들여다보고자 하였습니다.


## 1. 개발 환경
- Airflow 2.9.1
- MinIO 7.1.14
- Elk stack 7.5.2
- Spark 3.3.0
  
![image](https://github.com/user-attachments/assets/e50a88ad-0013-4d88-bbe7-432178c0b9e0)

## 2. ETL 파이프라인
- 통계청 API에서 데이터를 추출하여 MinIO에 저장
- DockerOperator & Spark로 빌드한 Image로 Json 데이터를 CSV로 변환
- LoadCsvOperator로 CSV 데이터를 PostgreSQL에 적재

![image](https://github.com/user-attachments/assets/34e4fa62-4ed9-407e-8e94-13058e096fb1)

## 3. ELK Stack
- ELK Stack을 사용하여 로그 관리

![image](https://github.com/user-attachments/assets/a9f0e82b-24b6-42b8-bac5-5179b9769923)

## 4. 시각화
### 4-1. 
