#python -m pytest  #python -m pytest -v
#python -m pytest -m transformation #python -m pytest -m "not transformation"
#python -m pytest --fixtures #python -m pytest --markers

import pytest
from lib import Utils as ut
from lib.DataReader import  *
from lib.DataManipulation import *
from lib import ConfigReader as cr
import conftest as ct
#below code is moved to configtest file
'''
@pytest.fixture
def spark():
    spark_session = ct.get_spark_session("LOCAL")
    yield spark_session
    #yield will return a value when pytest is running & when pytest completes it will execute following code
    #here spark session remains active until pytes completes & then gets stopped
    spark_session.stop()

'''
@pytest.mark.skip()
def test_read_customer_df(spark):
    #spark session createor is moved to fixture as its commonky called in all functions
    #spark=ut.get_spark_session("LOCAL")
    cust_count = read_customers(spark,"LOCAL").count()
    assert cust_count == 12435

@pytest.mark.slow
def test_read_orders_df(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_cnt = filter_closed_orders(orders_df).count()
    assert filtered_cnt == 7556
@pytest.mark.slow
def test_read_app_config(spark):
    config = cr.get_app_config("LOCAL")
    assert config["orders.file.path"] =="data/orders.csv"

@pytest.mark.transformation
def test_count_orders_state(spark,expected_result):
    cust_df = read_customers(spark, "LOCAL")
    actual_result = count_orders_state(cust_df)
    assert actual_result.collect() == expected_result.collect()


@pytest.mark.generic_test_v1
def test_generic_closed_count(spark):
    orders_df = read_orders(spark, "LOCAL")    
    filtered_cnt = filter_order_generic(orders_df,"CLOSED").count()
    assert filtered_cnt == 7556
@pytest.mark.generic_test_v1
def test_generic_pendingpayment_count(spark):
    orders_df = read_orders(spark, "LOCAL")    
    filtered_cnt = filter_order_generic(orders_df,"PENDING_PAYMENT").count()
    assert filtered_cnt == 15030
@pytest.mark.generic_test_v1
def test_generic_complete_count(spark):
    orders_df = read_orders(spark,"LOCAL")
    filtered_cnt = filter_order_generic(orders_df,"COMPLETE").count()
    assert filtered_cnt == 22900

#Above 3 functions can be done by parametrizing the values to be passed, allowing multiple test with just one function

@pytest.mark.parametrize(
        "status,count" , [
        ("CLOSED",7556),
        ("PENDING_PAYMENT",15030),
        ("COMPLETE",22900)
                    ]   )

@pytest.mark.generic_test_v2
def test_generic_filter_check_count(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    filtered_cnt = filter_order_generic(orders_df,status).count()
    assert filtered_cnt == count