import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from snowflake.connector.cursor import SnowflakeCursor, ProgrammingError
import snowflake.connector
import sys
import os
from os.path import join, dirname, abspath
from dotenv import load_dotenv

dotenv_path = join(dirname(abspath(__name__)), ".env")
load_dotenv(dotenv_path)
sys.path.insert(0, dirname(dirname(abspath(__file__))))
dag_file_path = dirname(abspath(__file__))

from utils.create_table import ddl_statements

# Snowflake connection details
conn = snowflake.connector.connect(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    schema=os.environ['SNOWFLAKE_SCHEMA'],
    role=os.environ['SNOWFLAKE_ROLE']
)


csv_files = [
    ('categories.csv', 'dim_categories') , 
    ('customers.csv' , 'dim_customers' ) ,
    ('employee_territories.csv' , 'dim_employee_territories') ,
    ('employees.csv' , 'dim_employees') ,
    ('order_details.csv', 'dim_order_details') ,
    ('orders.csv' , 'dim_orders') ,
    ('products.csv' , 'dim_products') ,
    ('regions.csv' , 'dim_regions') ,
    ('shippers.csv' , 'dim_shippers') ,
    ('suppliers.csv' , 'dim_suppliers') ,
    ('territories.csv' , 'dim_territories')
]


try:
    print("Connection successful!")
    cursor = conn.cursor(SnowflakeCursor)
    cursor.execute("SELECT CURRENT_VERSION()")
    row = cursor.fetchone()
    print("Snowflake version:", row[0])
    
    
except snowflake.connector.errors.Error as e:
    print("Connection failed!")
    print(e)

def extract_csv():
    base_url = 'https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/'
    for csv_file, _ in csv_files:
        url = f"{base_url}{csv_file}"
        print(url)
        df = pd.read_csv(url)
        df.to_csv(f"data/{csv_file}", index=False)

def create_tables():
    """Create tables in the Snowflake data warehouse if they do not exist."""
    try:
        cursor = conn.cursor(SnowflakeCursor)
        cursor.execute(f"USE SCHEMA {os.environ('SNOWFLAKE_DATABASE')}.{os.environ('SNOWFLAKE_SCHEMA')}")
        for ddl in ddl_statements.values():
            cursor.execute(ddl)
            print(f"Executed DDL: {ddl.strip().splitlines()[0]}...")  
        print("All tables created successfully.")
    except snowflake.connector.errors.Error as e:
        print("Error creating tables:", e)
    conn.close()

