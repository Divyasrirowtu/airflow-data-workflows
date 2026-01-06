from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG(
    dag_id='conditional_workflow_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

# Branching logic
def determine_branch(**context):
    exec_date = context['execution_date']
    day = exec_date.weekday()  # 0=Monday, 6=Sunday
    if day <= 2:  # Monday-Wednesday
        return 'weekday_processing'
    elif day <= 4:  # Thursday-Friday
        return 'end_of_week_processing'
    else:  # Saturday-Sunday
        return 'weekend_processing'

# Task implementations
def weekday_process():
    return {'day_name': 'Weekday', 'task_type': 'weekday', 'record_count': 100}

def end_of_week_process():
    return {'day_name': 'EndOfWeek', 'task_type': 'end_of_week', 'weekly_summary': 'Summary complete'}

def weekend_process():
    return {'day_name': 'Weekend', 'task_type': 'weekend', 'cleanup_status': 'Cleanup done'}

# Start and end tasks
start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success', dag=dag)

# Branch task
branch_task = BranchPythonOperator(
    task_id='branch_by_day',
    python_callable=determine_branch,
    dag=dag
)

# Weekday branch
weekday_task = PythonOperator(task_id='weekday_processing', python_callable=weekday_process, dag=dag)
weekday_summary_task = EmptyOperator(task_id='weekday_summary', dag=dag)

# End-of-week branch
end_of_week_task = PythonOperator(task_id='end_of_week_processing', python_callable=end_of_week_process, dag=dag)
end_of_week_report_task = EmptyOperator(task_id='end_of_week_report', dag=dag)

# Weekend branch
weekend_task = PythonOperator(task_id='weekend_processing', python_callable=weekend_process, dag=dag)
weekend_cleanup_task = EmptyOperator(task_id='weekend_cleanup', dag=dag)

# Set dependencies
start_task >> branch_task

# Weekday branch
branch_task >> weekday_task >> weekday_summary_task >> end_task

# End-of-week branch
branch_task >> end_of_week_task >> end_of_week_report_task >> end_task

# Weekend branch
branch_task >> weekend_task >> weekend_cleanup_task >> end_task
