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
class TestDataTransformation:

    def test_DataTransformation_Filter_check(self,connect_to_mysql_db):
        logger.info("Test cases execution for filter transformation has started ....")
        try:
            expected_query = """select * from staging_sales where sale_date>='2024-09-10'"""
            actual_query = """select * from filtered_sales_data"""
            verify_expected_from_db_to_actual_from_db(expected_query,connect_to_mysql_db,actual_query,connect_to_mysql_db)
        except Exception as e:
            logger.error(f"Test cases execution for filter transformation failed {e}")
            pytest.fail("Test cases execution for filter transformation failed" )


    def test_DataTransformation_Router_Low_region_check(self,connect_to_mysql_db):
        logger.info("Test cases execution for Router_Low transformation has started ....")
        try:
            expected_query = """select * from filtered_sales_data where region ='Low'"""
            actual_query = """select * from low_sales"""
            verify_expected_from_db_to_actual_from_db(expected_query,connect_to_mysql_db,actual_query,connect_to_mysql_db)
        except Exception as e:
            logger.error(f"Test cases execution for Router_Low transformation failed {e}")
            pytest.fail("Test cases execution for Router_Low transformation failed" )

    def test_DataTransformation_Router_High_region_check(self,connect_to_mysql_db):
        logger.info("Test cases execution for Router_High transformation has started ....")
        try:
            expected_query = """select * from filtered_sales_data where region ='High'"""
            actual_query = """select * from high_sales"""
            verify_expected_from_db_to_actual_from_db(expected_query,connect_to_mysql_db,actual_query,connect_to_mysql_db)
        except Exception as e:
            logger.error(f"Test cases execution for Router_High transformation failed {e}")
            pytest.fail("Test cases execution for Router_High transformation failed" )



