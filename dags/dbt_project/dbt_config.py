from pathlib import Path
from cosmos.config import ProfileConfig
from cosmos.config import ProjectConfig



# DBT Configuration
DBT_PROJECT_PATH = "/opt/airflow/dags/dbt_project"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"

DBT_CONFIG = ProfileConfig(
    profile_name="dbt_project",
    target_name="dev",
    profiles_yml_filepath=Path(
        "/opt/airflow/dags/dbt_project/profiles.yml"
    ),  
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path="/opt/airflow/dags/dbt_project/",
)

