import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from chispa import assert_df_equality

from lib import data_loader, utils


@pytest.fixture(scope="session")
def env():
    return "LOCAL"


@pytest.fixture(scope="session")
def spark(env):
    return utils.get_spark_session(env)


@pytest.fixture(scope="session")
def expected_input_df(spark):
    schema = StructType([StructField("video_id", IntegerType(), False),
                         StructField("timestamp", TimestampType(), False)])

    return spark.read.format("csv").schema(schema).load("test_data/test_data_input.csv")


def test_data_loader(expected_input_df, env, spark):
    actual_df = data_loader.load(env, spark)
    assert_df_equality(actual_df, expected_input_df)
