#!/bin/bash
echo "Deploying Airflow DAGs..."

scp -r dags/* airflow@$AIRFLOW_HOST:/opt/airflow/dags/

echo "Deployment successful!"
