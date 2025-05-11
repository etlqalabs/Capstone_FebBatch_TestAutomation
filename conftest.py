import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.ETLconfigs import *
import pytest

import logging

# Logging confiruration
logging.basicConfig(
    filename = "Logs/ETLLogs.log",
    filemode = "w", #  a  for append the log file and w for overwrite
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)


@pytest.fixture()
def connect_to_oracle_db():
    logger.info("Oracle conneciton is being established...")
    oracle_engine = create_engine( f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}").connect()
    logger.info("Oracle conneciton has been established...")
    yield oracle_engine
    oracle_engine.close()
    logger.info("Oracle conneciton has been closed..")

@pytest.fixture()
def connect_to_mysql_db():
    logger.info("mysql conneciton is being established...")
    mysql_engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}").connect()
    logger.info("mysql conneciton has been established...")
    yield mysql_engine
    mysql_engine.close()
    logger.info("mysql conneciton has been closed..")

@pytest.fixture()
def print_message():
    logger.info("This is pre test fixture...")
    yield
    logger.info("This is post test fixture...")