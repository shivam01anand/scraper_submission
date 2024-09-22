#!/bin/bash

set -e

sudo apt-get update
sudo apt-get install -y python3 python3-pip

pip3 install apache-airflow pandas scipy selenium webdriver_manager SQLAlchemy

export AIRFLOW_HOME=~/airflow

airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

mkdir -p ~/airflow/dags
cp main.py ~/airflow/dags/

airflow webserver -D

airflow scheduler -D

echo "Airflow is now running. You can access the web interface at http://localhost:8080"
echo "Use username 'admin' and password 'admin' to log in."

sleep 10

airflow dags trigger generate_sentiment_dag -c '{"ticker": "hdfc"}'

echo "generate_sentiment_dag has been triggered. Check the Airflow web interface for execution status."

while true; do sleep 1; done
