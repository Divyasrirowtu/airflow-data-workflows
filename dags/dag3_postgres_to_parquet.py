from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

dag = DAG(
    dag_id='postgres_to_parquet_export',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@weekly',
    catchup=False
)

def check_table_exists(table_name: str):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = hook.get_pandas_df(f"SELECT * FROM {table_name} LIMIT 1")
    if df.empty:
        raise ValueError(f"Table {table_name} does not exist or has no data")
    return True

def export_table_to_parquet(table_name: str, output_path: str):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = hook.get_pandas_df(f"SELECT * FROM {table_name}")
    
    # Save to Parquet
    df.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)
    
    file_size = os.path.getsize(output_path)
    return {
        'file_path': output_path,
        'row_count': len(df),
        'file_size_bytes': file_size
    }

def validate_parquet(file_path: str):
    df = pd.read_parquet(file_path, engine='pyarrow')
    if df.empty:
        raise ValueError("Parquet file is empty")
    expected_columns = ['id','name','age','city','salary','join_date','full_info','age_group','salary_category','year_joined']
    missing = [c for c in expected_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in Parquet: {missing}")
    return True

# Define tasks
check_table_task = PythonOperator(
    task_id='check_source_table_exists',
    python_callable=check_table_exists,
    op_kwargs={'table_name': 'transformed_employee_data'},
    dag=dag
)

export_task = PythonOperator(
    task_id='export_to_parquet',
    python_callable=export_table_to_parquet,
    op_kwargs={
        'table_name': 'transformed_employee_data',
        'output_path': '/opt/airflow/output/employee_data_{{ ds }}.parquet'
    },
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_parquet_file',
    python_callable=validate_parquet,
    op_kwargs={'file_path': '/opt/airflow/output/employee_data_{{ ds }}.parquet'},
    dag=dag
)

# Set dependencies
check_table_task >> export_task >> validate_task
