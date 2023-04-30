from airflow import DAG as dag_operator
from datetime import datetime 
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator
#from airflow.operators.spark_submit import SparkSubmitOperator 

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator    


with dag_operator("my_dag",start_date=datetime(2023,1,1),schedule_interval="@daily", catchup = False,) as dag:
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello World"',
    )

    """getZones = PythonOperator(
    task_id='getZones',
    application='essai_docker_ariflow_pyspark/tasks/zones.py',
    conn_id='spark_local',
    verbose=True,
    )"""
    getZones = PythonOperator(
    task_id='getZones',
    python='essai_docker_ariflow_pyspark/tasks/zones.py',
    python_callable=getzones,
    verbose=True,
    )



task1 >> getZones