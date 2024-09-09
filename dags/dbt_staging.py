from datetime import datetime, timedelta
from airflow import DAG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ExecutionConfig
from cosmos.config import RenderConfig
from cosmos.constants import LoadMode


from dbt_project.dbt_config import DBT_CONFIG, DBT_PROJECT_CONFIG, DBT_EXECUTABLE_PATH
from airflow.operators.empty import EmptyOperator

def dbt_staging():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end" )

    dbt_stg = DbtTaskGroup(
        group_id="dbt_staging",
        profile_config=DBT_CONFIG,
        project_config=DBT_PROJECT_CONFIG,
        render_config=RenderConfig( 
            load_method=LoadMode.DBT_LS, select=["path:models/staging"]
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
    )


    dbt_stg.set_upstream(start)
    dbt_stg.set_downstream(end)


defaulf_args = {
    'owner' : 'ETL Snowflake',
    'start_date': datetime(2024,9,7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
    }
with DAG(
    'DBT_Staging',
    default_args=defaulf_args,
    schedule_interval='@daily'
) as dag:
    
    dbt_staging() 