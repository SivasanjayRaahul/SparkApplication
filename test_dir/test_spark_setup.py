import pytest

from lib import utils, config_loader


@pytest.fixture(scope="session")
def env():
    return "LOCAL"


@pytest.fixture(scope="session")
def spark(env):
    return utils.get_spark_session(env)


def test_blank_test(spark):
    assert spark.version == "3.0.2"


def test_spark_local_config_loader(env):
    configs = config_loader.get_spark_conf(env)
    assert configs.get("spark.app.name") == "sparkapp-local"


def test_spark_prod_config_loader():
    configs = config_loader.get_spark_conf("PROD")
    assert configs.get("spark.app.name") == "sparkapp"
    # assert configs.get("spark.executor.cores") == "4"
    # assert configs.get("spark.executor.memory") == "10GB"
    # assert configs.get("spark.executor.memoryoverhead") == "1GB"
    # assert configs.get("spark.executor.instances") == "2"
    # assert configs.get("spark.sql.shuffle.partitions") == "8"


def test_spark_local_session(env):
    spark_session = utils.get_spark_session(env)
    assert spark_session.conf.get("spark.app.name") == "sparkapp-local"


def test_spark_prod_session():
    spark_session = utils.get_spark_session("PROD")
    assert spark_session.conf.get("spark.app.name") == "sparkapp"
