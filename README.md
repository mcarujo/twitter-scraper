# Twitter Scraper
The goal of this repository is to build a struct to extract data from the Twitter (or any data source) save in an HDFS (Hadoop) to be processed by Spark cluster. For this task, it was used the reference of many different repositories such as [Airflow](https://github.com/apache/airflow), [Big Data Europe](https://github.com/big-data-europe/docker-hadoop-spark-workbench) and [bitnami](https://hub.docker.com/r/bitnami/spark) (Spark).

In this repo you gonna find a similar structure like this picture:

![structure_docker_compose](https://github.com/mcarujo/twitter-scraper/blob/main/images/structure_docker_compose.png)

## Getting Start
Firstly, we can build and run our images using the docker-compose for it.
```
  docker-compose up -d 
```
After this command all images should be up and can be accessed by the following ports:
- http://localhost:80 ~> Airflow webpage;
- http://localhost:8080 ~> Spark master webpage;
- http://localhost:50070/explorer.html#/ ~> HDFS webpage;

### Folders and key files
- ~/logs -> Airflow logs store for debug.
- ~/outputs -> Save the outputs from the airflow dag execution and could be used for debugging.
- ~/data -> Directory to save the HDFS cluster information.
- ~/docker -> There are two folders to store the docker files to build the Airflow and Spark images.
- ~/dags -> Store the dags files to run over Airflow.
- ~/spark -> Store the files and can be submitted to the spark to run over the Pyspark.

### Airflow
Everything starts using the airflow, and for that, there is a template, an example that can be used as a reference to build your data pipeline.
To define what will be searched on Twitter, you can change the field query for that.


```python
twitter_operator = TwitterOperator(
        task_id="twitter_extract_bbb22",
        conn_id="twitter_default",
        query="Furia GG",
        file_path=f"outputs/twitter_furiagg_{EXEC_DATE}.json", 
        start_time=f"{EXEC_DATE}T00:00:00.00Z",
        end_time=f"{EXEC_DATE}T23:59:59.00Z",
    )
```



The following image is the dag's graph that contains six steps.
![airflow_dag_example](https://github.com/mcarujo/twitter-scraper/blob/main/images/airflow_dag_example.PNG)


Airflow requires the connectors to be used by the operators, sensors and hooks. When the application runs for the first time you need to set the initial connectors as you can see in this image.

![airflow_connectors_example](https://github.com/mcarujo/twitter-scraper/blob/main/images/airflow_connectors_example.PNG)


## Versions
- Airflow image: (From Airflow)
  - Airflow 2.2.1
  - Python 3.8.12
  - Spark 3.2.1  
- Spark image: (From Bitnami)
  - Python 3.8.12
  - Spark 3.2.1
- HDFS image: (From Big Data Europe)
  - Hadoop 2.8
