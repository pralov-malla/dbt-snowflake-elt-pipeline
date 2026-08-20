"""Orchestrate the Olist dbt project in Snowflake with Astronomer Cosmos."""

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pendulum import datetime


DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt/snowflake_elt"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"


profile_config = ProfileConfig(
    profile_name="snowflake_elt",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_dbt",
        profile_args={
            "database": "DBT_DB",
            "schema": "DBT_SCHEMA",
            "warehouse": "DBT_WH",
            "role": "DBT_ROLE",
        },
    ),
)


dbt_snowflake_elt = DbtDag(
    dag_id="dbt_snowflake_elt",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    project_config=ProjectConfig(
        DBT_PROJECT_PATH,
        install_dbt_deps=True,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    ),
    default_args={
        "retries": 1,
    },
    tags=["dbt", "snowflake", "cosmos"],
)
