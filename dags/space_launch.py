import json
import pandas as pd
import pendulum
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException 
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_if_launch(task_instance, **kwargs):
    response = task_instance.xcom_pull(task_ids="get_launch", key="return_value")
    response_json = json.loads(response) 
    if response_json["count"] == 0:
        raise AirflowSkipException("Skipping: No launches today")


def _obtain_data_launch(launch):
    dict_launch = {
        "id": launch["id"],
        "name": launch["name"],
        "status": launch["status"]["abbrev"],
        "country_code": launch["pad"]["country_code"],
        "service_provider_name": launch["launch_service_provider"]["name"],
        "service_provider_type": launch["launch_service_provider"]["type"],
    }
    return dict_launch


def process_data(task_instance, ds):

    response = task_instance.xcom_pull(task_ids="get_launch", key="return_value")
    response_json = json.loads(response)
    launches_list = [_obtain_data_launch(launch) for launch in response_json["results"]]
    df_results = pd.DataFrame(launches_list)
    df_results.to_parquet(path=f"/tmp/{ds}.parquet")

def _read_parquet_and_write_to_postgres(ds):

    # Read data from parquet
    df_launches = pd.read_parquet(f"/tmp/{ds}.parquet")
    df_launches.to_csv(f"/tmp/{ds}.csv", header=False, index=False)

    hook = PostgresHook(postgres_conn_id="postgres")
    hook.copy_expert(f"COPY rocket_launches FROM STDIN WITH CSV DELIMITER AS ','", f"/tmp/{ds}.csv")


with DAG(
    dag_id="space_launch_daily",
    start_date=pendulum.today("UTC").add(days=-40),
    description="This DAG will save the launches of today",
    schedule="@daily",
) as dag:

    wait_for_api = HttpSensor( 
        task_id='check_api', 
        http_conn_id='SpeceDevsThrottle', 
        endpoint='', 
        method='GET', 
        response_check=lambda response: response.status_code == 200, 
        mode='poke', 
        timeout=300, 
        poke_interval=60, 
    ) 

    get_launch = SimpleHttpOperator(
        task_id='get_launch',
        http_conn_id='SpaceDevs',
        endpoint='',
        method="GET",
        data={"net__gte": "{{ ds }} 00:00:00", "net__lt": "{{ next_ds }} 00:00:00"},
        response_check=lambda response: response.status_code == 200, 
        log_response=True,
    )

    check_launch = PythonOperator(
        task_id="check_launch_today",
        python_callable=get_if_launch,
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    create_bq_empty_ds = BigQueryCreateEmptyDatasetOperator(
        task_id='new_dataset_creator',
        gcp_conn_id='GoogleBigQuery',
        dataset_id='agustinir_dataset',
        project_id='aflow-training-rabo-2023-10-02',
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src="/tmp/{{ ds }}.parquet",
        dst="agustinir/launches/{{ ds }}.parquet",
        bucket="aflow-training-rabo-2023-10-02",
        gcp_conn_id='GoogleBigQuery',
    )

    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        gcp_conn_id='GoogleBigQuery',
        bucket="aflow-training-rabo-2023-10-02",
        source_objects="agustinir/launches/{{ ds }}.parquet",
        source_format="parquet",
        destination_project_dataset_table=f"agustinir_dataset.launches",
        write_disposition="WRITE_APPEND",
    )

    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS rocket_launches (
        id VARCHAR,
        name VARCHAR,
        status VARCHAR,
        country_code VARCHAR,
        service_provider_name VARCHAR,
        service_provider_type VARCHAR
        );
        """
    )

    write_parquet_to_postgres = PythonOperator(
        task_id="write_parquet_to_postgres",
        python_callable=_read_parquet_and_write_to_postgres
    )


    wait_for_api >> get_launch >> check_launch >> process_data >> create_bq_empty_ds >> upload_file >> gcs_to_bigquery
    process_data >> create_postgres_table >> write_parquet_to_postgres
