from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Create the DAG
dag = DAG(
    'test_dags',
    default_args=default_args,
    schedule_interval='5 4 * * *',
    catchup=False,
)

# Define a simple Python function
def print_hello():
    print("Hello from Airflow!")

# Define the tasks
start = DummyOperator(task_id='start', dag=dag)
hello_task = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Set the task dependencies
start >> hello_task >> end
