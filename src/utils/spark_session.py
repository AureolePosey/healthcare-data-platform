from pyspark.sql import SparkSession


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("healthcare-data-platform")
        .master("local[*]")
        .getOrCreate()
    )
    return spark