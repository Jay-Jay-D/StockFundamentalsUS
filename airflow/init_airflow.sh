#!/bin/bash

# Airflow quick set-up and deploy.

# If there is no local 'docker-compose.yaml' download the latest stable version.
if [ ! -f "./docker-compose.yaml" ]; then
    echo "Missing docker-compose.yaml - downloading..."
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
    echo "DONE!" 
fi

# Create '.env' file to get rid of the warning ("AIRFLOW_UID is not set")
if [ ! -f ".env" ]; then
    echo "Missing airflow .env file, creating ..."
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    echo "DONE!"
fi

# Airflow will run in the background, it takes some time to complete.
# When it's ready, the GUI will be available at http://localhost:8080 user:airflow pass:airflow
# To stop Airflow run 'docker compose stop'.
nohup docker compose up >/dev/null 2>&1 & echo "Running AirFlow"
