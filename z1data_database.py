import os
import sys
import psycopg2
from psycopg2 import sql
import pandas as pd
import glob
from sqlalchemy import create_engine


class Z12Database:
    def __init__(self, db_name, db_user, db_password, db_host, db_port):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.conn = self.get_db_connection()

    def get_db_connection(self):
        """Establish a database connection."""
        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
        )
        return conn

    def check_and_create_table(self):
        """Check if the z1_meta_data table exists, and create it if it doesn't."""
        # SQL command to create the table
        create_table_query = """
        CREATE TABLE z1_meta_data (
            seriesid VARCHAR(100),
            long_names VARCHAR(255),
            table_position VARCHAR(50),
            table_name TEXT,
            annotations TEXT,
            record_date DATE NOT NULL,
            PRIMARY KEY(seriesid,table_position,table_name)
        );
        """

        with self.conn.cursor() as cur:
            cur.execute(create_table_query)
            self.conn.commit()

    def check_record_date(self, record_date):
        """Check if the record_date exists in the z1_meta_data table."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM z1_meta_data WHERE record_date = %s
            """,
                (record_date,),
            )
            return cur.fetchone() is not None

    def load_files_to_db(self, folder_path, record_date):
        """Load files from the specified folder into the database."""
        # Get all text files in the directory except those containing 'transactions' in their name
        files = glob.glob(os.path.join(data_directory, "*.txt"))
        files_to_load = [f for f in files if "transactions" not in os.path.basename(f)]

        # Create a connection to the PostgreSQL database
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )

        for file_path in files_to_load:
            df = pd.read_csv(file_path, sep="\t", header=None)
            df["record_date"] = record_date
            # Check if the DataFrame has five columns
            if df.shape[1] == 6:
                df.columns = [
                    "seriesid",
                    "long_names",
                    "table_position",
                    "table_name",
                    "annotations",
                    "record_date",
                ]
                # Remove duplicate rows
                df.drop_duplicates(inplace=True)
                try:
                    # Load the DataFrame into the PostgreSQL table
                    df.to_sql("z1_meta_data", engine, if_exists="append", index=False)
                except Exception as e:
                    print(f"Error writing to database for file {file_path}: {e}")
            else:
                print(f"File {file_path} does not have exactly six columns. Skipping")

    def process(self, record_date, folder_path):
        self.check_and_create_table()
        if not self.check_record_date(record_date):
            self.load_files_to_db(folder_path, record_date)

    def close_connection(self):
        self.conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python z1data_database.py <record_date> <folder_path>")
        sys.exit(1)

    record_date = sys.argv[1]
    folder_path = sys.argv[2]

    db = Z12Database(
        db_name="z1data",
        db_user="gma",
        db_password="gmapass",
        db_host="localhost",
        db_port="5432",
    )

    try:
        db.process(record_date, folder_path)
    finally:
        db.close_connection()
