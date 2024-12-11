FROM apache/airflow:2.10.3

USER root

RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
