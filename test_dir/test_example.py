import pytest

from lib.utils import get_spark_session
@pytest.fixture(scope="session")
def spark():
    return get_spark_session()


def test_blank_test(spark):
    assert spark.version == "3.0.2"
