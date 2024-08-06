FROM apache/airflow:2.3.1-python3.8

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.3.1


## Start Python packages
USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Set up dbt_venv
ENV PIP_USER=false
ENV VIRTUAL_ENV=/opt/airflow/dbt_venv
RUN python3 -m venv $VIRTUAL_ENV && ${VIRTUAL_ENV}/bin/pip install dbt-postgres
ENV PIP_USER=true


WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID