from datetime import datetime, timedelta

import airflow
from airflow.models import DAG
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.sensors.http import HttpSensor

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
}

with DAG(
    dag_id="twitter_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    EXEC_DATE = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

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
        query="Furia GG",
        file_path=f"outputs/twitter_furiagg_{EXEC_DATE}.json",
        start_time=f"{EXEC_DATE}T00:00:00.00Z",
        end_time=f"{EXEC_DATE}T23:59:59.00Z",
    )
    is_webhdfs_available = WebHdfsSensor(
        task_id="is_webhdfs_available", webhdfs_conn_id="webhdfs_default", filepath=""
    )

    webhdfs_operator = WebHDFSOperator(
        task_id="webhdfs_upload",
        webhdfs_conn_id="webhdfs_default",
        source=f"outputs/twitter_furiagg_{EXEC_DATE}.json",
        destination=f"twitter_furiagg_{EXEC_DATE}.json",
    )
    is_webhdfs_file_uploaded = WebHdfsSensor(
        task_id="is_webhdfs_file_uploaded",
        webhdfs_conn_id="webhdfs_default",
        filepath=f"twitter_furiagg_{EXEC_DATE}.json",
    )

    submit_task_spark = SparkSubmitOperator(
        task_id="submit_task_spark",
        conn_id="spark_default",
        verbose=False,
        application=("/opt/airflow/spark/twitter_spark.py"),
        name="twitter_transformation",
        application_args=[
            "--src",
            f"/user/default/twitter_furiagg_{EXEC_DATE}.json",
        ],
    )

    (
        is_twitter_available
        >> twitter_operator
        >> is_webhdfs_available
        >> webhdfs_operator
        >> is_webhdfs_file_uploaded
        >> submit_task_spark
    )
