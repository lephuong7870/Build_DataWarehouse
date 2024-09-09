import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
import sys
import os
from os.path import join, dirname, abspath
from dotenv import load_dotenv


dotenv_path = join(dirname(abspath(__name__)), ".env")
load_dotenv(dotenv_path)

sys.path.insert(0, dirname(dirname(abspath(__file__))))
dag_file_path = dirname(abspath(__file__))

ctx = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema=os.environ['SNOWFLAKE_SCHEMA'],
    role=os.environ['SNOWFLAKE_ROLE']
)



def load_csv_to_snowflake_staging():
    cursor = ctx.cursor(SnowflakeCursor) 
    cursor.execute(f"USE SCHEMA {os.environ('SNOWFLAKE_DATABASE')}.{os.environ('SNOWFLAKE_SCHEMA')}")

    data_dir = "data/"
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]  

    for file_name in csv_files:
       
        file_path = os.path.join(data_dir, file_name)

        # Upload data to temporary location
        cursor.execute(f"PUT 'file://{file_path}' @csv_local auto_compress=true;")
        print(f"Uploaded {file_name} to Snowflake staging area.")

    # Close the cursor
    cursor.close()
    print("All CSV files processed.")

def load_staged_data_to_tables():
    cursor = ctx.cursor(SnowflakeCursor)  
    cursor.execute(f"USE SCHEMA {os.environ('SNOWFLAKE_DATABASE')}.{os.environ('SNOWFLAKE_SCHEMA')}")


    tables = [
        {"table_name": "DIM_CATEGORIES", "pattern": ".*categories.*"},
        {"table_name": "DIM_EMPLOYEES", "pattern": ".*employees.*"},
        {"table_name": "DIM_PRODUCTS", "pattern": ".*products.*"},
        {"table_name": "DIM_SUPPLIERS", "pattern": ".*suppliers.*"},
        {"table_name": "FACT_ORDERS", "pattern": ".*orders.*"},
        {"table_name": "FACT_ORDER_DETAILS", "pattern": ".*order_details.*"},
    ]
   

    # Loop through each table dictionary
    for table in tables:
        table_name = table["table_name"]
        pattern = table["pattern"]

        # Load data using COPY INTO
        try:
            cursor.execute(f"""
                           copy into {table_name} 
                           from (select distinct * from @csv_local t) 
                           file_format = (type='csv' field_delimiter=',' skip_header=1)
                           on_error = 'CONTINUE' 
                           pattern='{pattern}'
                           force = false;
                           """) 
            
            print(f"Loaded data into table: {table_name}")
        except snowflake.connector.Error as err:
            print(f"Error encountered for table {table_name}: {err}")


    cursor.close()
    print("All CSV files processed.")

