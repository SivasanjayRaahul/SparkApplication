import pytest

from lib import utils, config_loader


@pytest.fixture(scope="session")
def spark():
    return utils.get_spark_session()


@pytest.fixture(scope="session")
def env():
    return "LOCAL"


def test_blank_test(spark):
    assert spark.version == "3.0.2"


def test_spark_local_config_loader(env):
    configs = config_loader.get_spark_conf(env)
    assert configs.get("spark.app.name") == "sbdl-local"


def test_spark_prod_config_loader():
    configs = config_loader.get_spark_conf("PROD")
    assert configs.get("spark.app.name") == "sbdl"
    assert configs.get("spark.executor.cores") == "4"
    assert configs.get("spark.executor.memory") == "10GB"
    assert configs.get("spark.executor.memoryoverhead") == "1GB"
    assert configs.get("spark.executor.instances") == "2"
    assert configs.get("spark.sql.shuffle.partitions") == "8"
