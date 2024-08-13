import pytest
from lib.Utils  import get_spark_session

@pytest.fixture
def spark():
    "create spark session"
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    #yield will return a value when pytest is running & when pytest completes it will execute following code
    #here spark session remains active until pytest completes & then gets stopped as we are using yield instead of return
    spark_session.stop()

@pytest.fixture
def expected_result(spark):
    "returns expected result df as"
    result_schema = "state string, count int"
    return spark.read \
            .format("csv") \
            .option("header", False) \
            .schema(result_schema) \
            .load("data/test_result/state_aggregate.csv")
            