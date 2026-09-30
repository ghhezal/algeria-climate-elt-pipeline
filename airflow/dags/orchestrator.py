import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

sys.path.append('/opt/airflow/scripts')
from insert_records import main as ingest_weather_records

HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", ".")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "algeria-weather-pipeline_algeria_network")

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'description': "Orchestrates ingestion, dbt seed, run, and tests for Algerian Climate Zones weather",
    'start_date': datetime(2026, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'catchup': False,
}

dag = DAG(
    dag_id="algeria_climate_zones_pipeline",
    default_args=default_args,
    schedule=timedelta(minutes=15),
    tags=['algeria', 'weather', 'climate', 'dbt'],
)

with dag:
    ingest_task = PythonOperator(
        task_id='ingest_algerian_weather_data',
        python_callable=ingest_weather_records,
    )

    dbt_run_task = DockerOperator(
        task_id='dbt_run_transformations',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='build',
        working_dir='/usr/app',
        mounts=[
            Mount(
                source=f'{HOST_PROJECT_DIR}/dbt/my_project',
                target='/usr/app',
                type='bind'
            ),
            Mount(
                source=f'{HOST_PROJECT_DIR}/dbt/profiles.yml',
                target='/root/.dbt/profiles.yml',
                type='bind'
            ),
        ],
        network_mode=DOCKER_NETWORK,
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',
    )

    ingest_task >> dbt_run_task
