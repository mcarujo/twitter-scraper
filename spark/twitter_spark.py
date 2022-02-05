import argparse
from pyspark.sql import SparkSession


def init_spark():
    spark = (
        SparkSession.builder.master("spark://spark:7077")
        .appName("Airflow Twitter Pyspark")
        .getOrCreate()
    )
    sc = spark.sparkContext
    return spark, sc


def main(src: str):
    spark, sc = init_spark()
    df = spark.read.json(f"hdfs://namenode:8020{src}")
    df.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    args = parser.parse_args()
    main(args.src)
