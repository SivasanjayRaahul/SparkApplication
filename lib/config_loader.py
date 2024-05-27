from pyspark import SparkConf
import configparser


def get_spark_conf(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("../conf/spark.conf")
    print(config.items(env))
    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf
