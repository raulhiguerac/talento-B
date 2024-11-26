import os
import json 

from datetime import timedelta, datetime

# The DAG object
from airflow import DAG
from pathlib import Path

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator

from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator

def parse_csv_to_list(filepath):
    import csv

    with open(filepath, newline="") as file:
        reader = csv.reader(file)
        next(reader)
        yield from reader  


# initializing the default arguments
default_args = {
		'owner': 'RH',
		'start_date': datetime(2024, 11, 25),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
with DAG('xm-extract',
		default_args=default_args,
		description='Retrieve information about CIIU',
		schedule_interval='@daily',
		catchup=False,
		tags=['talento-b']
) as dag:

    start_task = DummyOperator(task_id='start_task', dag=dag)

    extract = SimpleHttpOperator(
        task_id="extract_XM_data",
        http_conn_id="SCRAP_URL",
        endpoint= "/xm-data",
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            'date': '{{ ds }}',
            'bucket': 'xm-data',
        }),
        method="POST",
        dag=dag,
    )

    upload = S3ToSqlOperator(
        task_id="upload_postgres",
        s3_bucket="xm-data",
        s3_key='{{ ds }}.csv',
        table="ciuu_data",
        aws_conn_id="minio",
        parser=parse_csv_to_list,
        sql_conn_id="PROD_DB",
        dag=dag,
    )

    end_task = DummyOperator(task_id='end_task', dag=dag)

    start_task >> extract >> upload >> end_task