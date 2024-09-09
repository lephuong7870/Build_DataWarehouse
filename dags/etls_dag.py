from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os
from os.path import join, dirname, abspath
from dotenv import load_dotenv

load_dotenv(verbose=True)
dotenv_path = join(dirname(abspath(__name__)), ".env")
load_dotenv(dotenv_path)


sys.path.insert(0, dirname(dirname(abspath(__file__))))
dag_file_path = dirname(abspath(__file__))


from dwh_scripts.create_table_sf import  *
from dwh_scripts.insert_table_sf import  *


defaulf_args = {
    'owner' : 'ETL Snowflake',
    'start_date': datetime(2024,9,7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    }
with DAG(
    'Load_Data_To_Snowflake',
    default_args=defaulf_args,
    schedule_interval='@daily'
) as dag:
    
    extract_csv_task = PythonOperator(
        task_id = 'extract_csv' ,
        python_callable= extract_csv 
    )

    create_tables_task = PythonOperator(
        task_id = 'create_table_snowflake' ,
        python_callable=create_tables
    )

    load_csv_to_snowflake_staging_task = PythonOperator(
        task_id = 'load_csv_to_staging_snowflake' ,
        python_callable=load_csv_to_snowflake_staging
    )

    load_staged_data_to_tables_task = PythonOperator(
        task_id = 'load_data_to_snowflake' ,
        python_callable=load_staged_data_to_tables
    )

    extract_csv_task >> create_tables_task >> load_csv_to_snowflake_staging_task >> load_staged_data_to_tables_task
    