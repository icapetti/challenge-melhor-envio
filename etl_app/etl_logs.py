"""This script is responsible for the ETL process and Consumers and Requests reporting."""

import json
import logging
from shutil import rmtree
from pathlib import Path
from os import makedirs
from datetime import datetime

from utils.etl_helper import ETL_Helper

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

PROJECT_BASE_PATH = Path(__file__).resolve().parents[0]
TMP = PROJECT_BASE_PATH / 'data' / 'tmp'
FILE_PATH = PROJECT_BASE_PATH / 'data' / 'teste-data-engineer.zip'
UNZIP_FILE_PATH = PROJECT_BASE_PATH / TMP / 'teste-data-engineer'/ 'logs.txt'
CREDENTIALS_PATH = PROJECT_BASE_PATH / 'configs' / 'credentials.json'
OUTPUT_FILE = PROJECT_BASE_PATH / 'data' / 'output' / f'consumer-and-service-report-{datetime.now()}.xlsx'
CONSUMER_QUERY = PROJECT_BASE_PATH / 'configs' / 'consumer-report-dql.sql'
SERVICE_QUERY = PROJECT_BASE_PATH / 'configs' / 'service-report-dql.sql'
CONSUMER_SHEET_NAME = 'consumer'
SERVICE_SHEET_NAME = 'service'

with open(CREDENTIALS_PATH) as file:
    MYSQL_CONN = json.load(file)['mysql']

if __name__=='__main__':
    logging.info("Starting ETL APP!")
    etl_helper = ETL_Helper()

    # Extract zip files to a temporary folder
    etl_helper.extract_zip(input_file_path=FILE_PATH, extracted_files_path=TMP)

    # Read txt file as jsonlines and transform to table
    df = etl_helper.from_jsonlines_to_df(txt_source=UNZIP_FILE_PATH)

    # Renames columns to stadardize underline as words separator instead of dot or hyphen
    df.columns = etl_helper.standardize_columns_names(df.columns)

    # Converts dates from unix epoch to datetime
    date_columns = ['service_updated_at', 'service_created_at', 'route_updated_at', 'route_created_at', 'started_at']
    df_standardized_dates = etl_helper.from_unix_to_datetime(df=df, date_columns=date_columns)

    # Converts lists columns to json
    json_columns = ['route_methods', 'route_paths', 'route_protocols', 'request_querystring']
    list_columns_converted = etl_helper.from_list_to_json(df=df_standardized_dates, json_columns=json_columns)

    # Loads data to db
    etl_helper.load_to_mysql(db_conn=MYSQL_CONN, df=list_columns_converted, table="logs")

    # Delete temp folder and files
    rmtree(TMP)

    # Create output folder if not exists
    makedirs(Path(__file__).resolve().parents[1] / 'data' / 'output', exist_ok=True)

    logging.info("Start generating REPORT PROCESS...")
    # Get data from db
    customer_report = etl_helper.query_mysql(db_conn=MYSQL_CONN, query_name=CONSUMER_QUERY)
    service_report = etl_helper.query_mysql(db_conn=MYSQL_CONN, query_name=SERVICE_QUERY)

    # Write report to file
    etl_helper.write_df_to_xlsx(data=[customer_report, service_report], output_path=OUTPUT_FILE, sheet_names=[CONSUMER_SHEET_NAME, SERVICE_SHEET_NAME])

    logging.info("End of ETL APP execution.")
