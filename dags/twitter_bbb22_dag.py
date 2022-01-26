from datetime import datetime, timedelta
from os.path import join

import airflow
from airflow.models import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from plugins.operators.twitter_operator import TwitterOperator
from plugins.operators.webhdfs_operator import WebHDFSOperator

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": "mamcarujo@gmail.com",
    "retries": 0,
    # "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="twitter_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    is_twitter_available = HttpSensor(
        task_id="is_twitter_available",
        method="GET",
        http_conn_id="twitter_site_default",
        endpoint="",
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )
    twitter_operator = TwitterOperator(
        task_id="twitter_extract_bbb22",
        conn_id="twitter_default",
        query="big brother brasil",
        file_path="outputs/twitter_bbb22_{{ds}}.json",
        start_time="2022-01-21T00:00:00.00Z",
        end_time="2022-01-21T23:59:59.00Z",
    )
    is_webhdfs_available = WebHdfsSensor(
        task_id="is_webhdfs_available", webhdfs_conn_id="webhdfs_default", filepath=""
    )

    webhdfs_operator = WebHDFSOperator(
        task_id="webhdfs_upload",
        webhdfs_conn_id="webhdfs_default",
        source="outputs/twitter_bbb22_{{ds}}.json",
        destination="twitter_bbb22_{{ds}}.json",
    )
    is_webhdfs_file_uploaded = WebHdfsSensor(
        task_id="is_webhdfs_file_uploaded",
        webhdfs_conn_id="webhdfs_default",
        filepath="twitter_bbb22_{{ds}}.json",
    )

    (
        is_twitter_available
        >> twitter_operator
        >> is_webhdfs_available
        >> webhdfs_operator
        >> is_webhdfs_file_uploaded
    )
