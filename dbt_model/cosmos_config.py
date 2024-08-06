from pathlib import Path

from cosmos.config import ProfileConfig
from cosmos.config import ProjectConfig

DBT_CONFIG = ProfileConfig(
    profile_name="online_retail",
    target_name="dev",
    profiles_yml_filepath=Path("/opt/airflow/dbt_model/profiles.yml"), 
)

DBT_PROJECT_CONFIG = ProjectConfig( dbt_project_path="/opt/airflow/dbt_model/")

