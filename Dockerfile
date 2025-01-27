FROM apache/airflow:2.10.4
ADD requirements.txt .
RUN pip install apache-airflow==2.10.4 -r requirements.txt