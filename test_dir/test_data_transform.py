import datetime
from chispa import assert_df_equality

import pytest
from pyspark.sql.types import StructType, StructField, IntegerType

from lib import data_loader, utils, transform_data


@pytest.fixture(scope="session")
def env():
    return "LOCAL"


@pytest.fixture(scope="session")
def spark(env):
    return utils.get_spark_session(env)


@pytest.fixture(scope="session")
def expected_transform_data(spark):
    schema = StructType([StructField("video_id", IntegerType(), False),
                         StructField("count", IntegerType(), False)])
    return spark.read.schema(schema).csv("test_data/test_transform_data.csv")


def test_data_transform(env, spark, expected_transform_data):
    input_df = data_loader.load(env, spark, date=datetime.date.today())
    actual_transform_data = transform_data.to(input_df)
    assert_df_equality(actual_transform_data, expected_transform_data, ignore_metadata=True)
