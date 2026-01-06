from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

dag = DAG(
    dag_id='data_transformation_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

def create_transformed_table():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    create_sql = """
    CREATE TABLE IF NOT EXISTS transformed_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary FLOAT,
        join_date DATE,
        full_info VARCHAR(500),
        age_group VARCHAR(20),
        salary_category VARCHAR(20),
        year_joined INTEGER
    );
    """
    hook.run(create_sql)

def transform_data():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    df = hook.get_pandas_df("SELECT * FROM raw_employee_data")
    
    # Transformations
    df['full_info'] = df['name'] + " - " + df['city']
    df['age_group'] = df['age'].apply(lambda x: 'Young' if x < 30 else ('Mid' if x < 50 else 'Senior'))
    df['salary_category'] = df['salary'].apply(lambda x: 'Low' if x < 50000 else ('Medium' if x < 80000 else 'High'))
    df['year_joined'] = pd.to_datetime(df['join_date']).dt.year
    
    # Insert into transformed table
    for _, row in df.iterrows():
        hook.run(
            """INSERT INTO transformed_employee_data 
               (id,name,age,city,salary,join_date,full_info,age_group,salary_category,year_joined)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            parameters=(
                row['id'], row['name'], row['age'], row['city'], row['salary'], row['join_date'],
                row['full_info'], row['age_group'], row['salary_category'], row['year_joined']
            )
        )
    
    return {'rows_processed': len(df), 'rows_inserted': len(df)}

# Define tasks
create_table_task = PythonOperator(
    task_id='create_transformed_table',
    python_callable=create_transformed_table,
    dag=dag
)

transform_and_load_task = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_data,
    dag=dag
)

# Set dependencies
create_table_task >> transform_and_load_task
