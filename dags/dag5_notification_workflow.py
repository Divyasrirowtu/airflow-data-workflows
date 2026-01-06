from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import random

dag = DAG(
    dag_id='notification_workflow',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

# Callbacks
def send_success_notification(context):
    ti = context['task_instance']
    message = f"Task {ti.task_id} succeeded on {ti.execution_date}"
    print(message)
    return {'notification_type': 'success', 'status': 'sent', 'message': message, 'timestamp': str(ti.execution_date)}

def send_failure_notification(context):
    ti = context['task_instance']
    message = f"Task {ti.task_id} failed on {ti.execution_date}"
    print(message)
    return {'notification_type': 'failure', 'status': 'sent', 'message': message, 'error': str(context.get('exception')), 'timestamp': str(ti.execution_date)}

# Risky operation
def risky_operation(**context):
    exec_day = context['execution_date'].day
    if exec_day % 5 == 0:
        raise Exception("Simulated failure: day divisible by 5")
    return {'status': 'success', 'execution_date': str(context['execution_date']), 'success': True}

# Cleanup task
def cleanup_task():
    print("Cleanup done")
    return {'cleanup_status': 'completed', 'timestamp': str(datetime.now())}

# Tasks
start_task = EmptyOperator(task_id='start_task', dag=dag)

risky_task = PythonOperator(
    task_id='risky_operation',
    python_callable=risky_operation,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    dag=dag
)

success_notification_task = EmptyOperator(task_id='success_notification', trigger_rule='all_success', dag=dag)
failure_notification_task = EmptyOperator(task_id='failure_notification', trigger_rule='all_failed', dag=dag)
always_execute_task = PythonOperator(task_id='always_execute', python_callable=cleanup_task, trigger_rule='all_done', dag=dag)

# Set dependencies
start_task >> risky_task >> [success_notification_task, failure_notification_task]
[success_notification_task, failure_notification_task] >> always_execute_task
