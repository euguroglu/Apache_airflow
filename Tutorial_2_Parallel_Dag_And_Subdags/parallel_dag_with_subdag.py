from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel_dag import subdag_parallel_dag

from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG('parallel_dag',schedule_interval='@daily',
        default_args=default_args,catchup=False) as dag:

        bash_operator_1 = BashOperator(
            task_id = "bash_operator_1",
            bash_command = 'sleep 3'
        )

        processing = SubDagOperator(
            task_id = "processing_tasks",
            subdag = subdag_parallel_dag('parallel_dag', 'processing_tasks', default_args)
        )

        bash_operator_4 = BashOperator(
            task_id = "bash_operator_4",
            bash_command = 'sleep 3'
        )

        bash_operator_1 << processing << bash_operator_4
