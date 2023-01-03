from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from task_1 import ms_1
from task_2 import ms_2
from task_visualization import start_visualize_server

import pandas as pd
from sqlalchemy import create_engine


def load_to_postgres(
    path="/opt/airflow/data/",
    filename="accidents_cleaned_milestone2.csv",
    lookup_table="encodings.csv",
    port=5432,
):

    engine = create_engine(f"postgresql://root:root@pgdatabase:{port}/accidents")
    if engine.connect():
        print("connected succesfully")
    else:
        print("failed to connect")

    # "accidents_cleaned_milestone2.csv"
    df = pd.read_csv(f"{path}/{filename}")
    df = df.head(1000)  # to make load task faster
    df.to_sql(name="UK_Accidents_2011", con=engine, if_exists="replace")
    # "encodings.csv"
    df = pd.read_csv(f"{path}/{lookup_table}")
    df.to_sql(name="lookup_table", con=engine, if_exists="replace")
    df.to_sql(name="titanic_passengers", con=engine, if_exists="replace")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
}

dag = DAG(
    "accidents_etl_pipeline",
    default_args=default_args,
    description="accidents etl pipeline",
)
with DAG(
    dag_id="accidents_etl_pipeline",
    schedule_interval="@once",
    default_args=default_args,
    tags=["accidents-pipeline"],
) as dag:
    extract_clean_task = PythonOperator(
        task_id="extract_dataset",
        python_callable=ms_1,
        op_kwargs={
            "path": "/opt/airflow/data/",
            "filename": "2011_Accidents_UK.csv",
        },
    )
    encoding_load_task = PythonOperator(
        task_id="add_feature",
        python_callable=ms_2,
        op_kwargs={
            "path": "/opt/airflow/data/",
            "cleaned_file": "accidents_cleaned.csv",
            "new_dataset": "2018_Accidents_UK.csv",
        },
    )
    load_to_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        op_kwargs={
            "path": "/opt/airflow/data/",
            "filename": "accidents_cleaned_milestone2.csv",
            "lookup_table": "encodings.csv",
            "port": 5432,
        },
    )
    create_dashboard_task = PythonOperator(
        task_id="visualize_accidents",
        python_callable=start_visualize_server,
        op_kwargs={
            "path": "/opt/airflow/data/",
            "filename": "accidents_cleaned_milestone2.csv",
            "port": 8055,
        },
    )

    (
        extract_clean_task
        >> encoding_load_task
        >> load_to_postgres_task
        >> create_dashboard_task
    )
