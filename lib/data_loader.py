from pyspark.sql.types import StructField, StructType, IntegerType, TimestampType
from pyspark.sql import SparkSession


def load(env, spark: SparkSession):
    schema = StructType([StructField("video_id", IntegerType(), False),
                         StructField("timestamp", TimestampType(), False)])
    if env == "LOCAL":
        return spark.read.format("csv").schema(schema).load("../test_data/test_data_input.csv")
