"""TODO: docstring"""

import json
import shutil
from pathlib import Path

from utils.etl_helper import ETL_Helper

PROJECT_BASE_PATH = Path(__file__).resolve().parents[1]
TMP = PROJECT_BASE_PATH / 'data' / 'tmp'
FILE_PATH = PROJECT_BASE_PATH / 'data' / 'teste-data-engineer.zip'
UNZIP_FILE_PATH = PROJECT_BASE_PATH / TMP / 'teste-data-engineer'/ 'logs.txt'
CREDENTIALS_PATH = Path(__file__).resolve().parents[0] / 'configs' / 'credentials.json'

with open(CREDENTIALS_PATH) as file:
    MYSQL_CONN = json.load(file)['mysql']

if __name__=='__main__':
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
    shutil.rmtree(TMP)
