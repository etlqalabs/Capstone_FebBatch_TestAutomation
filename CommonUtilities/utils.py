import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
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



oracle_engine = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
#mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")
mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")

def verify_expected_from_files_to_actual_from_db(file_path,file_type,query_actual,db_engine):
    try:
        if file_type == "csv":
            df_expected = pd.read_csv(file_path)
        elif file_type == "json":
            df_expected = pd.read_json(file_path)
        elif file_type == "xml":
            df_expected = pd.read_xml(file_path,xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed {file_type}")

        df_actual = pd.read_sql(query_actual, db_engine)
        assert df_actual.equals(df_expected), "data extraction didn't happen correctly"
    except Exception as e:
        logger.error(f"data extraction from sales didn't happen correctly{e}")

def verify_expected_from_db_to_actual_from_db(query_expected,db_engine_expected ,query_actual,db_engine_actual):
    try:
        df_expected = pd.read_sql(query_expected, db_engine_expected)
        df_actual = pd.read_sql(query_actual, db_engine_actual)
        assert df_actual.equals(df_expected), "data extraction didn't happen correctly"
    except Exception as e:
        logger.error(f"data extraction didn't happen correctly{e}")
