from pyspark.sql import SparkSession
from lib import config_loader


def get_spark_session(env):
    if env == "LOCAL":
        return (SparkSession.builder.config(conf=config_loader.get_spark_conf(env))
                .config('spark.driver.extraJavaOptions',
                        '-Dlog4j.configuration=file:log4j.properties')
                .master("local[2]").getOrCreate())

    elif env == "PROD":
        return (SparkSession.builder.config(conf=config_loader.get_spark_conf(env))
                .getOrCreate())
