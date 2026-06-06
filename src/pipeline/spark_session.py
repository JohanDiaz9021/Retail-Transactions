import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str = "supermarket-pipeline") -> SparkSession:
    python_path = os.path.join(os.path.dirname(sys.executable), "python.exe")
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.pyspark.python", python_path)
        .getOrCreate()
    )
