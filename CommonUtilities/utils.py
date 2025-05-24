import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Configuration.ETLconfigs import *
import pytest
import os

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
        assert df_actual.equals(df_expected), "data does not match between expected and actual"
    except Exception as e:
        logger.error(f"data extraction from sales didn't happen correctly{e}")

def verify_expected_from_db_to_actual_from_db(query_expected,db_engine_expected ,query_actual,db_engine_actual):
    try:
        df_expected = pd.read_sql(query_expected, db_engine_expected).astype(str)
        logger.info(f"The expected data is: {df_expected}")
        df_actual = pd.read_sql(query_actual, db_engine_actual).astype(str)
        logger.info(f"The expected data is: {df_actual}")
        assert df_actual.equals(df_expected), "data does not match between expected and actual"
    except Exception as e:
        logger.error(f"data does not match between expected and actual{e}")
        pytest.fail("data does not match between expected and actual")

# Data Quality related functions

# utility for checking if file exists
def check_file_exists(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"File {file_path} does not exists{e}")

# utility for checking if file exists
def check_file_size_for_data(file_path):
    try:
        if os.path.getsize(file_path) != 0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"File {file_path} does not have any data{e}")

    def verify_expected_from_files_to_actual_from_db(file_path, file_type, query_actual, db_engine):
        try:
            if file_type == "csv":
                df_expected = pd.read_csv(file_path)
            elif file_type == "json":
                df_expected = pd.read_json(file_path)
            elif file_type == "xml":
                df_expected = pd.read_xml(file_path, xpath=".//item")
            else:
                raise ValueError(f"unsupported file type passed {file_type}")

            df_actual = pd.read_sql(query_actual, db_engine)
            assert df_actual.equals(df_expected), "data does not match between expected and actual"
        except Exception as e:
            logger.error(f"data extraction from sales didn't happen correctly{e}")

def check_duplicates_rows(file_path,file_type):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df= pd.read_xml(file_path,xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed {file_type}")
        logger.info(f"The  data is: {df}")
        if df.duplicated().any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"error while reading the file{e}")

def check_duplicates_for_specific_column(file_path,file_type,column_name):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df= pd.read_xml(file_path,xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed {file_type}")
        logger.info(f"The  data is: {df}")
        if df[column_name].duplicated().any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"error while reading the file{e}")



def check_for_null_values(file_path,file_type):
    try:
        if file_type == "csv":
            df = pd.read_csv(file_path)
        elif file_type == "json":
            df = pd.read_json(file_path)
        elif file_type == "xml":
            df= pd.read_xml(file_path,xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed {file_type}")

        logger.info(f"The  data is: {df}")
        if df.isnull.any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"error while reading the file{e}")



# S3 related code


#### New code ###########
import boto3
import pandas as pd
from io import StringIO

# Initialize a session using Boto3
s3 = boto3.client('s3')

# Read the file from S3 and return dataframe
def read_csv_from_s3(bucket_name, file_key):
    try:
        # Fetch the CSV file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)

        # Read the content of the file and load it into a Pandas DataFrame
        csv_content = response['Body'].read().decode('utf-8')  # Decode content to string
        data = StringIO(csv_content)  # Use StringIO to simulate a file-like object

        # Read the CSV data into a Pandas DataFrame
        df = pd.read_csv(data)

        # Return the DataFrame
        return df
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        return None


def verify_expected_as_S3_to_actual_as_db(bucket_name,file_key,db_engine_actual,query_actual):
    # The desired path and file name in the S3 bucket
    # Call the function to read the CSV file from S3
    df_expected = read_csv_from_s3(bucket_name, file_key)
    logger.info(f"The expected data is the database is: {df_expected}")
    df_actual = pd.read_sql(query_actual, db_engine_actual)
    logger.info(f"The actual data is the database is: {df_actual}")
    assert df_actual.equals(df_expected), f"expected does not match with expected data in{query_actual}"

