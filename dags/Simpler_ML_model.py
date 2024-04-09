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
from snowflake.snowpark import session

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
        cursor.execute(f"DELETE FROM {snowflake_table1}")

    with conn2.cursor() as cursor:
        cursor.execute(f"COPY INTO {snowflake_table1} FROM {snowflake_stage} FILE_FORMAT= (TYPE = CSV) ON_ERROR = CONTINUE PURGE = TRUE")

def snowpark_ml():    
    from snowflake.ml.modeling.preprocessing import StandardScaler
    from snowflake.ml.modeling.pipeline import Pipeline
    from snowflake.ml.modeling.xgboost import XGBClassifier
    from snowflake.ml.modeling.metrics import accuracy_score, precision_score, recall_score, f1_score
    from snowflake.snowpark import Session 
    from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
    import pandas as pd

    conn3 = {
        'user' :'GPARLADE',
        'password':'DanielCarter10',
        'account':'fv45033.eu-central-1',
        'database':'POC_DBT_AIRFLOW',
        'schema':'GOLD'
    }

    session = Session.builder.configs(conn3).create()

    data = session.sql("select * from POC_DBT_AIRFLOW.GOLD.UNLIMITED_DATA_CUSTOMERS")   

    train_data, test_data = data.random_split(weights=[0.8, 0.2], seed=0)

    feature_cols = train_data.columns
    
    feature_cols.remove('CUSTOMER_STATUS')

    feature_cols.remove('CUSTOMER_ID')
    
    target_col = ['CUSTOMER_STATUS']

    output_colss = ['PREDICTION']

    model = XGBClassifier(input_cols=feature_cols, label_cols=target_col, output_cols = output_colss)

    model.fit(train_data)

    predict_on_training_data = model.predict(train_data)
    predict_on_test_data = model.predict(test_data)
    eval_accuracy = accuracy_score(df=predict_on_test_data, y_true_col_names='CUSTOMER_STATUS', y_pred_col_names='PREDICTION')
    eval_precision = precision_score(df=predict_on_test_data, y_true_col_names='CUSTOMER_STATUS', y_pred_col_names='PREDICTION')
    eval_recall = recall_score(df=predict_on_test_data, y_true_col_names='CUSTOMER_STATUS', y_pred_col_names='PREDICTION')
    eval_f1 = f1_score(df=predict_on_test_data, y_true_col_names='CUSTOMER_STATUS', y_pred_col_names='PREDICTION')

    metrics={'Measures' : ['Accuracy', 'Precision', 'Recall', 'F1'],
              'Results' : [eval_accuracy, eval_precision, eval_recall, eval_f1]}

    metrics_df = pd.DataFrame(metrics)

    predict_on_test_data = predict_on_test_data.to_pandas()

    test_data = test_data.to_pandas()
    
    print(type(predict_on_test_data))

    print(type(test_data))

    predict_on_test_data = session.write_pandas(predict_on_test_data, "ML_PREDICTION", auto_create_table=False, overwrite=True)

    metrics_df = session.write_pandas(metrics_df, "ML_SCORES", auto_create_table=False, overwrite=True)

profile_config = ProfileConfig(profile_name="default",
                               target_name="dev",
                               profile_mapping=SnowflakeUserPasswordProfileMapping(conn_id="snowflake_conn", 
                                                    profile_args={
                                                        "database": "POC_DBT_AIRFLOW",
                                                        "schema": "BRONZE"
                                                        },
                                                    ))

with DAG(
    dag_id="simpler_ml_model",
    start_date=datetime(2024, 3, 27),
    schedule_interval="@monthly",
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

    ml = PythonOperator(
        task_id='machinelearning_model',
        python_callable=snowpark_ml
    )

    e2 = EmptyOperator(task_id="post_dbt")

    transfer_task >> copy_to_table_task >> dbt_tg >> ml >> e2