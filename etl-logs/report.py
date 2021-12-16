"""TODO: docstring"""

import json
from pathlib import Path
from os import makedirs

from utils.etl_helper import ETL_Helper

CREDENTIALS_PATH = Path(__file__).resolve().parents[0] / 'configs' / 'credentials.json'
with open(CREDENTIALS_PATH) as file:
    MYSQL_CONN = json.load(file)['mysql']

QUERIES_BASE_PATH = Path(__file__).resolve().parents[0] / 'configs'
CONSUMER_QUERY = 'consumer-report-dql.sql'
SERVICE_QUERY = 'service-report-dql.sql'
CONSUMER_SHEET_NAME = 'consumer'
SERVICE_SHEET_NAME = 'service'
OUTPUT_FILE = Path(__file__).resolve().parents[1] / 'data' / 'output' / 'consumer-and-service-report.xlsx'

if __name__=='__main__':
    etl_helper = ETL_Helper()
    makedirs(Path(__file__).resolve().parents[1] / 'data' / 'output', exist_ok=True)

    # Get data from db
    customer_report = etl_helper.query_mysql(db_conn=MYSQL_CONN, query_name=QUERIES_BASE_PATH / CONSUMER_QUERY)
    service_report = etl_helper.query_mysql(db_conn=MYSQL_CONN, query_name=QUERIES_BASE_PATH / SERVICE_QUERY)

    # Write report to file
    etl_helper.write_df_to_xlsx(data=[customer_report, service_report], output_path=OUTPUT_FILE, sheet_names=[CONSUMER_SHEET_NAME, SERVICE_SHEET_NAME])
