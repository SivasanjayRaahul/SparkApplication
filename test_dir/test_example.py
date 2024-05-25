import pytest

from ..lib import utils

@pytest.fixture(scope="session")
def spark():
    return utils.get_spark_session()


def test_blank_test(spark):
    assert spark.version == "3.0.2"
