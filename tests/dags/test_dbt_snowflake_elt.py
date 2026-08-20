"""Basic validation for the dbt Snowflake orchestration DAG."""

import os

# Cosmos normally caches its parsed dbt graph in Airflow's metadata database.
# The isolated pytest container has no initialized metadata database, so caching
# is disabled for tests only. Production Airflow keeps the default cache enabled.
os.environ["AIRFLOW__COSMOS__ENABLE_CACHE"] = "False"

from airflow.models import DagBag


def load_dag_bag() -> DagBag:
    return DagBag(include_examples=False)


def test_dag_imports_without_errors():
    dag_bag = load_dag_bag()

    assert not dag_bag.import_errors


def test_dbt_snowflake_dag_configuration():
    dag = load_dag_bag().dags.get("dbt_snowflake_elt")

    assert dag is not None
    assert dag.catchup is False
    assert dag.default_args["retries"] == 1
    assert {"dbt", "snowflake", "cosmos"}.issubset(dag.tags)
