import logging
import re
import json
from pathlib import Path
from zipfile import ZipFile

from pandas import json_normalize, to_datetime, read_sql, ExcelWriter
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class ETL_Helper():
    """This class is responsible for the methods that help in the process of Extracting,
    Transforming and loading data, as well as generating reports based on this data."""

    def __init__(self) -> None:
        logging.basicConfig(
            format='%(asctime)s %(message)s',
            level=logging.INFO)

    def extract_zip(
            self,
            input_file_path: Path,
            extracted_files_path: Path) -> None:
        """It unzips files with a .zip extension."""
        logging.info(f"Unzipping {input_file_path}")
        with ZipFile(input_file_path, 'r') as zipObj:
            zipObj.extractall(extracted_files_path)
        logging.info(f"Files extracted successfully to {extracted_files_path}")

    def from_jsonlines_to_df(self, txt_source: Path) -> object:
        """Reads files with .txt extension and jsonlines data and transforms
        the data into a data frame."""
        logging.info("Getting json from txt...")
        with open(txt_source) as file:
            data = [json.loads(line) for line in file]
        logging.info(f"{len(data)} lines extracted from {txt_source}")

        logging.info("Converting json to Data Frame...")
        df = json_normalize(data)
        logging.info(f"Data Frame generated with {len(df)} rows")

        return df

    def standardize_columns_names(self, df_columns: object) -> object:
        """Standardizes column names, replacing dots and hyphens as separators for underlines."""
        logging.info("Standardizing columns names")
        return [re.sub("[.-]", "_", c).lower() for c in df_columns]

    def from_unix_to_datetime(self, df: object, date_columns: list) -> object:
        """Converts dates that are in unix epoch format to datetime."""
        logging.info(f"Converting dates of columns {date_columns}")
        df[date_columns] = df[date_columns].apply(to_datetime, unit='s')
        return df

    def from_list_to_json(self, df: object, json_columns: list) -> object:
        """Converts columns with list data into json format compatible with MySQL JSON format."""
        logging.info(f"Converting json of columns {json_columns}")
        for c in json_columns:
            df[c] = df[c].apply(json.dumps)

        return df

    def get_engine(self, db_conn: dict) -> Engine:
        """Get engine for database connection."""
        logging.info("Getting engine")
        return create_engine(
            "{jdbc}://{user}:{pass}@{host}:{port}/{db}".format(**db_conn))

    def load_to_mysql(self, db_conn: dict, df: object, table: str) -> None:
        """Loads a dataframe into a MYSQL table."""
        logging.info(f"Loading {len(df)} rows to {table}...")
        engine = self.get_engine(db_conn=db_conn)
        df.to_sql(table, engine, if_exists="append", index=False)
        logging.info("Done!")

    def query_mysql(self, db_conn: dict, query_name: Path) -> object:
        """Executes query in MYSQL database and returns data in DataFrame format."""
        logging.info("Getting data from db")
        engine = self.get_engine(db_conn=db_conn)
        path = query_name

        with open(path) as file:
            query = file.read()

        df = read_sql(query, engine)
        logging.info(f"Done! {len(df)} rows.")
        return df

    def write_df_to_xlsx(
            self,
            data: list,
            sheet_names: list,
            output_path: Path) -> None:
        """Writes the data of a Data Frame in a file with .xlsx extension.
        If more than one Data Frame is entered, the data is saved in a single file separated by sheets."""
        logging.info(f"Writting {sheet_names} to file...")
        with ExcelWriter(output_path) as writer:
            for index, df in enumerate(data):
                df.to_excel(writer, sheet_name=sheet_names[index], index=False)

        logging.info(f"Done! File {output_path} generated successfully!")
