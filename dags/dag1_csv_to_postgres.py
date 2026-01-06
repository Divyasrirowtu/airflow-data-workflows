from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

dag = DAG(
    dag_id='csv_to_postgres_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

def create_employee_table():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    create_sql = """
    CREATE TABLE IF NOT EXISTS raw_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary FLOAT,
        join_date DATE
    );
    """
    hook.run(create_sql)

def truncate_employee_table():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("TRUNCATE TABLE raw_employee_data;")

def load_csv_data():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pd.read_csv("/opt/airflow/data/input.csv")
    for _, row in df.iterrows():
        hook.run(
            "INSERT INTO raw_employee_data (id,name,age,city,salary,join_date) VALUES (%s,%s,%s,%s,%s,%s)",
            parameters=(row['id'], row['name'], row['age'], row['city'], row['salary'], row['join_date'])
        )
    return len(df)

create_table_task = PythonOperator(
    task_id='create_table_if_not_exists',
    python_callable=create_employee_table,
    dag=dag
)

truncate_table_task = PythonOperator(
    task_id='truncate_table',
    python_callable=truncate_employee_table,
    dag=dag
)

load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_data,
    dag=dag
)

create_table_task >> truncate_table_task >> load_csv_task
