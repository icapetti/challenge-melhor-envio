import logging
import re
import json
from pathlib import Path
from zipfile import ZipFile

from pandas import json_normalize, to_datetime, read_sql, ExcelWriter
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

class ETL_Helper():
    """TODO: doctstring"""
    def __init__(self) -> None:
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    def extract_zip(self, input_file_path:Path, extracted_files_path:Path) -> None:
        """TODO: doctstring"""
        logging.info(f"Unzipping {input_file_path}")
        with ZipFile(input_file_path, 'r') as zipObj:
            zipObj.extractall(extracted_files_path)
        logging.info(f"Files extracted successfully to {extracted_files_path}")

    def from_jsonlines_to_df(self, txt_source: Path) -> object:
        """TODO: doctstring"""
        logging.info("Getting json from txt...")
        with open(txt_source) as file:
            data = [json.loads(line) for line in file]
        logging.info(f"{len(data)} lines extracted from {txt_source}")

        logging.info("Converting json to Data Frame...")
        df = json_normalize(data)
        logging.info(f"Data Frame generated with {len(df)} rows")

        return df

    def standardize_columns_names(self, df_columns: object) -> object:
        """TODO: doctstring"""
        logging.info("Standardizing columns names")
        return [re.sub("[.-]", "_", c).lower() for c in df_columns]

    def from_unix_to_datetime(self, df: object, date_columns: list) -> object:
        """TODO: doctstring"""
        logging.info(f"Converting dates of columns {date_columns}")
        df[date_columns] = df[date_columns].apply(to_datetime, unit='s')
        return df

    def from_list_to_json(self, df: object, json_columns: list) -> object:
        """TODO: doctstring"""
        logging.info(f"Converting json of columns {json_columns}")
        for c in json_columns:
            df[c] = df[c].apply(json.dumps)

        return df

    def get_engine(self, db_conn: dict) -> Engine:
        logging.info("Getting engine")
        return create_engine("{jdbc}://{user}:{pass}@{host}:{port}/{db}".format(**db_conn))

    def load_to_mysql(self, db_conn: dict, df: object, table:str) -> None:
        logging.info(f"Loading {len(df)} rows to {table}...")
        engine = self.get_engine(db_conn=db_conn)
        df.to_sql(table, engine, if_exists="append", index=False)
        logging.info("Done!")

    def query_mysql(self, db_conn: dict, query_name: Path):
        logging.info("Getting data from db")
        engine = self.get_engine(db_conn=db_conn)
        path = query_name

        with open(path) as file:
            query = file.read()

        df = read_sql(query, engine)
        logging.info(f"Done! {len(df)} rows.")
        return df

    def write_df_to_xlsx(self, data: list, sheet_names: list, output_path: Path) -> None:
        logging.info(f"Writting {sheet_names} to file...")
        with ExcelWriter(output_path) as writer:
            for index, df in enumerate(data):
                df.to_excel(writer, sheet_name=sheet_names[index], index=False)

        logging.info(f"Done! File {output_path} generated successfully!")
