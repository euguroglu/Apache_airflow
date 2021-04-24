from airflow import DAG
from airflow.operators.bash import BashOperator

def subdag_parallel_dag(parent_dag_id, child_dag_id, default_args):
    with DAG(dag_id=f'{parent_dag_id}.{child_dag_id}', default_args=default_args) as dag:
        bash_operator_2 = BashOperator(
            task_id = "bash_operator_2",
            bash_command = 'sleep 3'
        )

        bash_operator_3 = BashOperator(
            task_id = "bash_operator_3",
            bash_command = 'sleep 3'
        )

        return dag
