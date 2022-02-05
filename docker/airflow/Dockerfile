FROM apache/airflow:2.2.1-python3.8

COPY requirements.txt requirements.txt

USER root
RUN apt-get update -y && apt-get install default-jdk curl iputils-ping nano -y

USER airflow
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt