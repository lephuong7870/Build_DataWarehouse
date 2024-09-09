FROM apache/airflow:2.7.1-python3.9

COPY requirements.txt /opt/airflow/

USER root
RUN apt-get update  && apt-get install -y gcc python3-dev

USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt


RUN export PIP_USER=false && python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    deactivate

    