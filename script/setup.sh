#!/usr/bin/env bash


echo "Copying files script_python to src airflow dags"
cp -r ./scripts_python_procesado/*.py ./mnt/airflow/dags/src/
cp -r ./scripts_python_procesado/dags/*.py ./mnt/airflow/dags/
cp -r ./data/ ./mnt/airflow/dags/src/data
echo "Copying completed"