#!/bin/bash
set -e

echo "Updating package list..."
sudo apt-get update

echo "Installing Python3 and pip..."
sudo apt-get install -y python3 python3-pip

echo "Installing required Python packages..."
pip3 install apache-airflow pandas scipy selenium webdriver_manager SQLAlchemy

echo "Setting Airflow home directory..."
export AIRFLOW_HOME=~/airflow

echo "Initializing Airflow database..."
airflow db init

echo "Creating Airflow admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Creating Airflow DAGs directory and copying DAG file..."
mkdir -p ~/airflow/dags
cp main.py ~/airflow/dags/

echo "Starting Airflow webserver..."
airflow webserver -D

echo "Starting Airflow scheduler..."
airflow scheduler -D

echo "Airflow is now running. You can access the web interface at http://localhost:8080"
echo "Use username 'admin' and password 'admin' to log in."

echo "Waiting for Airflow services to fully start..."
sleep 10

echo "Triggering the DAG (the 2nd dag is triggered on successful completion of 1st)"
airflow dags trigger generate_sentiment_dag -c '{"ticker": "hdfc"}'

echo "The DAG has been triggered. Check the Airflow web interface for execution status."

echo "Script execution completed. Entering infinite loop to keep the script running..."
while true; do sleep 1; done
