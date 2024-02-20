from datetime import datetime
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

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

    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt/cosmosproject"),
        operator_args={"install_deps": True},
        execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
        profile_config=profile_config
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> dbt_tg >> e2