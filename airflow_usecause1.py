import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_operator import (
    GCSCreateBucketOperator)
from airflow.contrib.operators.bigquery_operator import (
    BigQueryCreateEmptyDatasetOperator,BigQueryCreateEmptyTableOperator)
from airflow.contrib.operators.gcs_to_gcs import GCSToGCSOperator
from datetime import timedelta

BUCKET_NAME = "airflow_us"
DATASET_NAME = "airflow_stage_dev"
DATASET_NAME1 = "airflow_history_dev"
TABLE_NAME = "customer"
    
default_args={
          'start_date' : airflow.utils.dates.days_ago(0),
        'depends_on_past': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=10)
}
dag = DAG(
    'airflow_manitor',
    default_args = default_args,
    description = 'hardwork_pays'
    )

Createbucket = GCSCreateBucketOperator(
    task_id = 'CreateNewBucket',
    bucket_name = BUCKET_NAME,
    storage_class = 'MULTI_REGIONAL',
    dag=dag)
    
CreateDataset_stage = BigQueryCreateEmptyDatasetOperator(
    task_id = 'Create_dataset_stage',
    dataset_id = DATASET_NAME,
    dag = dag)

CreateDataset_history = BigQueryCreateEmptyDatasetOperator(
    task_id = 'Create_dataset_history',
    dataset_id = DATASET_NAME1,
    dag = dag)

create_table1 = BigQueryCreateEmptyTableOperator(
    task_id = 'create_table1',
    dataset_id = DATASET_NAME,
    table_id = 'customer',
    dag = dag,
    schema_fields = [
        {"name": "customer_code", "type": "string", "mode": "REQUIRED"},
        {"name": "customer", "type": "string", "mode": "REQUIRED"},
		{"name": "platform", "type": "string", "mode": "REQUIRED"},
		{"name": "channel", "type": "string", "mode": "REQUIRED"},
		{"name": "market", "type": "string", "mode": "REQUIRED"},
		{"name": "sub_zone", "type": "string", "mode": "REQUIRED"},
		{"name": "region", "type": "string", "mode": "REQUIRED"}
    ]
)

create_table2 = BigQueryCreateEmptyTableOperator(
    task_id = 'create_table2',
    dataset_id = DATASET_NAME1,
    table_id = 'customer',
    dag = dag,
    schema_fields = [
        {"name": "customer_code", "type": "integer", "mode": "REQUIRED"},
        {"name": "customer", "type": "string", "mode": "REQUIRED"},
		{"name": "platform", "type": "string", "mode": "REQUIRED"},
		{"name": "channel", "type": "string", "mode": "REQUIRED"},
		{"name": "market", "type": "string", "mode": "REQUIRED"},
		{"name": "sub_zone", "type": "string", "mode": "REQUIRED"},
		{"name": "region", "type": "string", "mode": "REQUIRED"}
    ]
)

bucket_to_bucket = GCSToGCSOperator(
    task_id='copy_file',
    source_bucket='airflow_us',
    source_object='dim_customer.csv',
    destination_bucket=BUCKET_NAME,
    destination_object='dim_customer.csv',
    dag = dag)

loading_into_stage = BashOperator(
    task_id = "loading_file",
    bash_command = "bq load --source_format=CSV --skip_leading_rows=1 airflow_stage_dev.customer gs://airflow_us/dim_customer.csv",
    dag = dag)

loading_into_history = BashOperator(
    task_id = "transforming_file",
    bash_command = 'bq query --use_legacy_sql=false "INSERT INTO airflow_history_dev.customer SELECT CAST(customer_code AS INTEGER), customer, platform, channel, market, sub_zone, region FROM airflow_stage_dev.customer"',
    dag = dag)

Createbucket>>bucket_to_bucket>>CreateDataset_stage>>create_table1>>loading_into_stage>>CreateDataset_history>>create_table2>>loading_into_history