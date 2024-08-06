from airflow import Dataset


BQ_DATASET = "online_retail"
BQ_SRC_INVOICES = "raw_invoices"
BQ_SRC_COUNTRY = "raw_country"

DBT_VENV_EXEC_PATH = "/opt/airflow/dbt_venv/bin/dbt"


# Datasets
DS_START = Dataset("start")
DS_INVOICES_BQ = Dataset(f"{BQ_DATASET}.{BQ_SRC_INVOICES}")
DS_COUNTRY_BQ = Dataset(f"{BQ_DATASET}.{BQ_SRC_COUNTRY}")
DS_STAGING_BQ = Dataset(f"{BQ_DATASET}.stg_invoices")
DS_TRANSFORM_BQ = Dataset(f"{BQ_DATASET}.fct_invoices")