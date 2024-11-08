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
### 4-1. 성별간 트렌드
![population_trend_Chart](https://github.com/user-attachments/assets/b29c2e79-380d-4ed8-9cf4-c591a7da11a5)

### 4-2. 성별간 초혼 연령
![average_first_marriage_age_Chart](https://github.com/user-attachments/assets/d7074146-025a-44e7-bb40-838cf7c9b57b)

### 4-3. 성별간 소득 차이
![gender_income_Chart](https://github.com/user-attachments/assets/a7fece2a-f9de-4da7-81d0-3f6de64733b8)

## 5. 프로젝트 후기
- 통계청 API를 사용하는 좋은 경험을 하였다. 통계청 API가 어떤 구성으로 이루어져 있는지 알 수 있는 시간이였다.
- 처음으로 Operator를 커스터마이징 하여 사용해본 프로젝트이다.
- 의문이 드는 점은 실제 ETL Task와 Sensor의 경계선이 어느정도인지 모르겠다.
- 하나의 CustomOperator에 Sensor와 Task를 한 번에 사용하는 것이 좋은지 분리하여 각각의 Task로 사용하는 것이 좋은지 아직까지 잘 모르겠다.
- 또한 Airflow의 Task Log를 수집하여 모니터링할 수 있는 좋은 경험이였다.
- ELK Stack을 조금 더 세밀하게 사용하지 못해본 것이 아쉬웠다.
- ELK Stack 이외에도 airflow의 자체 log수집 방식도 있었으나 이를 제대로 활요해보지 못한것이 아쉽다.
