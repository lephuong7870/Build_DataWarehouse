#!/bin/bash


# Deactivate authentication for webserver
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> opt/airflow/webserver_config.py


airflow db init
airflow db upgrade

airflow connections add 'postgres' \
    --conn-type 'postgres' \
    --conn-login $POSTGRES_USER \
    --conn-password $POSTGRES_PASSWORD \
    --conn-host $POSTGRES_CONTAINER \
    --conn-port $POSTGRES_PORT \
    --conn-schema $POSTGRES_DB

airflow users create \
    --username admin \
    --firstname phuong \
    --lastname le \
    --role Admin \
    --email thanhphuong7870@gmail.com \
    --password admin

airflow db check


airflow scheduler &
exec airflow webserver