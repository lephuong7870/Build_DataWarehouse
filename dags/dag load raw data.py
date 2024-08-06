from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from  airflow.models import Variable
import pandas as pd
from airflow.operators.email import EmailOperator
from sqlalchemy import create_engine







default_args = {
    'owner' : 'airflow' ,
    'email' : 'thanhphuonng7870@gmail.com',
    'email_on_failure': True,
    'start_date': days_ago(1),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
     
}


def load_csv_to_postgresql(table, path):
  
    con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@postgresql:5432/{Variable.get("POSTGRES_DB")}')
    df = pd.read_csv(path)
    df.to_sql( table , con , if_exists='append', index=False)
    
   
with DAG(
    "LOAD_RAW_DATA", 
    default_args=default_args,
    schedule_interval='@daily'


) as dag:
    
    
    create_table = PostgresOperator(
    task_id = "create_table",
    postgres_conn_id="postgres",
    sql="sql/create_table.sql"


    )


    with TaskGroup("create raw data") as load_data:


        raw_country = PostgresOperator(
        task_id = "raw country",
        postgres_conn_id="postgres",
        sql="./dataset/raw_country.sql"
        )

        raw_invoices = PythonOperator(
        task_id='raw invoices',
        python_callable=load_csv_to_postgresql,
        op_kwargs={
            "table":"raw_invoices",
            "path": "./dataset/online_retail.csv"
            }
        )





    create_table >> load_data 
