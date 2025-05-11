import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

from CommonUtilities.utils import verify_expected_from_files_to_actual_from_db, \
    verify_expected_from_db_to_actual_from_db
from Configuration.ETLconfigs import *
import pytest

#from Utilities.Utils import *
import logging

# Logging confiruration
logging.basicConfig(
    filename = "Logs/ETLLogs.log",
    filemode = "w", #  a  for append the log file and w for overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("connect_to_mysql_db")
class TestDataLoading:

    def test_DataLoad_fact_sales_check(self,connect_to_mysql_db):
        logger.info("Test cases execution for fact_sales data load has started ....")
        try:
            expected_query = """select sales_id,product_id,store_id,quantity,sales_amount,sale_date from sales_with_details order by sales_id,product_id,store_id"""
            actual_query = """select sales_id,product_id,store_id,quantity,total_sales,sale_date from fact_sales order by sales_id,product_id,store_id"""
            verify_expected_from_db_to_actual_from_db(expected_query,connect_to_mysql_db,actual_query,connect_to_mysql_db)
            logger.info("Test cases execution for fact_sales data load has completed ....")
        except Exception as e:
            logger.error(f"Test cases execution for fact_sales data load has failed {e}")
            pytest.fail("Test cases execution for fact_sales data load has failed" )

    def test_DataLoad_Sales_Summary_check(self,connect_to_mysql_db):
        logger.info("Test cases execution for sales_summary data load has started ....")
        try:
            expected_query = """select product_id,month,year,total_sales from monthly_sales_summary_source"""
            actual_query = """select product_id,month,year,total_sales from monthly_sales_summary"""
            verify_expected_from_db_to_actual_from_db(expected_query,connect_to_mysql_db,actual_query,connect_to_mysql_db)
            logger.info("Test cases execution for fact_sales data load has completed ....")
        except Exception as e:
            logger.error(f"Test cases execution for sales_summary data load has failed {e}")
            pytest.fail("Test cases execution for sales_summary data load has failed" )
