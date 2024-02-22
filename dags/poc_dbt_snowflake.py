from datetime import datetime
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from azure.storage.blob import BlobServiceClient
import snowflake.connector
import tempfile

def transfer_blob_to_snowflake():
    # Azure Blob Storage configuration
    account_url = 'https://synapsefabrictest.blob.core.windows.net'
    credential = 'sv=2020-02-10&st=2024-02-19T16%3A18%3A50Z&se=2028-02-27T16%3A18%3A00Z&sr=c&sp=racwdlmeop&sig=4yWcl2o%2B0%2BeMAIPjVbeO43gFZeWuv1FKyzz%2FhXFIPJE%3D'
    blob_container_name = 'demosnowflake'
    blob_file_name = 'telecom_zipcode_population.csv'
    
    # Snowflake configuration
    snowflake_account = 'fv45033.eu-central-1'
    snowflake_user = 'GPARLADE'
    snowflake_password = 'DanielCarter10'
    snowflake_database = 'POC_DBT_AIRFLOW'
    snowflake_schema = 'PUBLIC'
    snowflake_stage = '@MY_INT_STAGE'
    
    # Download blob
    blob_service_client = BlobServiceClient(account_url, credential=credential)
    blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=blob_file_name)
    
    # Save blob content to a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        blob_content = blob_client.download_blob().readall()
        temp_file.write(blob_content)
        temp_file_path = temp_file.name
    
    # Upload temporary file to Snowflake stage
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        schema=snowflake_schema
    )
    with conn.cursor() as cursor:
        stage_file_path = f"{snowflake_stage}/{blob_file_name}"
        cursor.execute(f"PUT 'file://{temp_file_path}' '{stage_file_path}' AUTO_COMPRESS=TRUE OVERWRITE = TRUE")
    

def copy_to_snowflake_table():

    # Snowflake configuration
    snowflake_account = 'fv45033.eu-central-1'
    snowflake_user = 'GPARLADE'
    snowflake_password = 'DanielCarter10'
    snowflake_database = 'POC_DBT_AIRFLOW'
    snowflake_schema = 'PUBLIC'
    snowflake_stage = '@MY_INT_STAGE'
    snowflake_table1= 'BRONZE.TELECOM_ZIPCODE_POPULATION'
    file_format= 'my_csv_format'
    file1= 'telecom_zipcode_population.csv.gz'

    # Upload file at internal stage to the table 

    conn2 = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        schema=snowflake_schema
    )
    with conn2.cursor() as cursor:
        cursor.execute(f"COPY INTO {snowflake_table1} FROM {snowflake_stage} FILE_FORMAT= (TYPE = CSV) ON_ERROR = CONTINUE PURGE = TRUE")

profile_config = ProfileConfig(profile_name="default",
                               target_name="dev",
                               profile_mapping=SnowflakeUserPasswordProfileMapping(conn_id="snowflake_conn", 
                                                    profile_args={
                                                        "database": "POC_DBT_AIRFLOW",
                                                        "schema": "BRONZE"
                                                        },
                                                    ))

with DAG(
    dag_id="poc_dbt_snowflake",
    start_date=datetime(2024, 2, 15),
    schedule_interval="@daily",
):

    transfer_task = PythonOperator(
        task_id='transfer_task',
        python_callable=transfer_blob_to_snowflake
    )

    copy_to_table_task = PythonOperator(
        task_id='copy_to_table',
        python_callable=copy_to_snowflake_table
    )

    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt/cosmosproject"),
        operator_args={"install_deps": True},
        execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
        profile_config=profile_config
    )

    e2 = EmptyOperator(task_id="post_dbt")

    transfer_task >> copy_to_table_task >> dbt_tg >> e2