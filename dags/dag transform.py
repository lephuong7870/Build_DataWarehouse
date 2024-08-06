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
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from cosmos import DbtTaskGroup
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ExecutionConfig
from cosmos.config import RenderConfig
from cosmos.constants import LoadMode

from ...dbt_model.cosmos_config import DBT_CONFIG
from ...dbt_model.cosmos_config import DBT_PROJECT_CONFIG

from params import DBT_VENV_EXEC_PATH
from params import DS_TRANSFORM_BQ


default_args = {
    'owner' : 'airflow' ,
    'email' : 'thanhphuonng7870@gmail.com',
    'email_on_failure': True,
    'start_date': days_ago(1),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
     
}


   
with DAG(
    "DBT transform", 
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(0),
    schedule=[DS_TRANSFORM_BQ],
    catchup=False,


) as dag:
    
    start = EmptyOperator(task_id="start")

   
    dbt_marts = DbtTaskGroup(
        group_id="dbt_marts",
        profile_config=DBT_CONFIG,
        project_config=DBT_PROJECT_CONFIG,
        render_config=RenderConfig(  # controls how task are rendered visually
            load_method=LoadMode.DBT_LS,
            select=["path:models/marts"],
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_VENV_EXEC_PATH,
        ),
    )


    end = EmptyOperator(
        task_id="end",
    )

    start >> dbt_marts >>  end