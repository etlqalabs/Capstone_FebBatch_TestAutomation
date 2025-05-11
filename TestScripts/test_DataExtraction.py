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


@pytest.mark.usefixtures("print_message","connect_to_mysql_db","connect_to_oracle_db")
class TestDataExtraction:

    @pytest.mark.dataExtraction
    def test_DataExtraction_sales_data_to_staging(self,connect_to_mysql_db):
        logger.info("Test cases execution for sales_data extraction from source started ....")
        try:
            actual_query = """select * from staging_sales"""
            verify_expected_from_files_to_actual_from_db("TestData/sales_data_Linux_remote.csv","csv",actual_query,connect_to_mysql_db)
            logger.info("Test cases execution for sales_data extraction from source completed ....")
        except Exception as e:
            logger.error(f"Test cases execution for sales_data extraction from source failed {e}")
            pytest.fail("Test cases execution for sales_data extraction from source failed" )



    @pytest.mark.dataExtraction
    def test_DataExtraction_product_data_to_staging(self,connect_to_mysql_db):
        logger.info("Test cases execution for product_data extraction from source started ....")
        try:
            actual_query = """select * from staging_product"""
            verify_expected_from_files_to_actual_from_db("TestData/product_data.csv","csv",actual_query,connect_to_mysql_db)
            logger.info("Test cases execution for product_data extraction from source completed ....")
        except Exception as e:
            logger.error(f"Test cases execution for product_data extraction from source failed {e}")
            pytest.fail("Test cases execution for product_data extraction from source failed" )

    @pytest.mark.dataExtraction
    def test_DataExtraction_inventory_data_to_staging(self,connect_to_mysql_db):
        logger.info("Test cases execution for inventory_data extraction from source started ....")
        try:
            actual_query = """select * from staging_inventory"""
            verify_expected_from_files_to_actual_from_db("TestData/inventory_data.xml","xml",actual_query,connect_to_mysql_db)
            logger.info("Test cases execution for inventory_data extraction from source completed ....")
        except Exception as e:
            logger.error(f"Test cases execution for inventory_data extraction from source failed {e}")
            pytest.fail("Test cases execution for inventory_data extraction from source failed" )

    @pytest.mark.dataExtraction
    def test_DataExtraction_supplier_data_to_staging(self,connect_to_mysql_db):
        logger.info("Test cases execution for supplier_data extraction from source started ....")
        try:
            actual_query = """select * from staging_supplier"""
            verify_expected_from_files_to_actual_from_db("TestData/supplier_data.json","json",actual_query,connect_to_mysql_db)
        except Exception as e:
            logger.error(f"Test cases execution for supplier_data extraction from source failed {e}")
            pytest.fail("Test cases execution for supplier_data extraction from source failed" )

    @pytest.mark.oracleSourceExtraction
    def test_DataExtraction_store_data_to_staging(self,connect_to_oracle_db,connect_to_mysql_db):
        logger.info("Test cases execution for stores_data extraction from source started ....")
        try:
            expected_query = """select * from stores"""
            actual_query = """select * from staging_stores"""
            verify_expected_from_db_to_actual_from_db(expected_query,connect_to_oracle_db,actual_query,connect_to_mysql_db)
        except Exception as e:
            logger.error(f"Test cases execution for stores_data extraction from source failed {e}")
            pytest.fail("Test cases execution for stores_data extraction from source failed" )




