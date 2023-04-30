from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

from cosmos.providers.dbt.core.operators import (
    DbtRunOperator,
    DbtSeedOperator,
)

with DAG(
    dag_id="import-seeds",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    project_seeds = [
        {
            "project": "jaffle_shop",
            "seeds": ["raw_customers", "raw_payments", "raw_orders"],
        }
    ]

    install_deps = DbtRunOperator(
        task_id="jaffle_shop_install_deps",
        command="deps",
        project_dir=f"/usr/local/airflow/dbt/jaffle_shop",
        schema="public",
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        conn_id="postgres",
        dbt_flags=["--no-write-json", "--no-version-check", "--no-analytics"],
        dbt_args={"install_deps": True},
    )

    with TaskGroup(group_id="drop_seeds_if_exist") as drop_seeds:
        for project in project_seeds:
            for seed in project["seeds"]:
                DbtRunOperator(
                    task_id=f"drop_{seed}_if_exists",
                    command="run-operation",
                    operation="drop_table",
                    args={"table_name": seed},
                    project_dir=f"/usr/local/airflow/dbt/{project['project']}",
                    schema="public",
                    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
                    conn_id="postgres",
                    dbt_flags=["--no-write-json", "--no-version-check", "--no-analytics"],
                )

    create_seeds = DbtSeedOperator(
        task_id=f"jaffle_shop_seed",
        project_dir=f"/usr/local/airflow/dbt/jaffle_shop",
        schema="public",
        dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        conn_id="postgres",
        outlets=[Dataset(f"SEED://JAFFLE_SHOP")],
     )

    install_deps >> drop_seeds >> create_seeds
