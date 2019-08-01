"""
Testing BranchPythonOperator
"""
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 29),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def id_filter(id, id_list):
    if id in id_list:
        return 'task1_{}'.format(id)
    else:
        return "null_{}".format(id)


id_list = ["A", "B", "C"]
id_list_part = ["A", "B"]

with DAG('branch_advanced_test_dag', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:

    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )

    for id in ["A", "B", "C"]:
        branching = BranchPythonOperator(
            task_id='branching_{}'.format(id),
            # python_callable=lambda: id_filter(id, id_list_part), # wrong!!! have late binding issue
            python_callable=lambda id=id: id_filter(id, id_list_part),
            trigger_rule="all_done"
        )
        # check this out: https://stackoverflow.com/a/55206304/8280662 & https://docs.python-guide.org/writing/gotchas/#late-binding-closures

        task1 = DummyOperator(
            task_id='task1_{}'.format(id),
            trigger_rule='one_success',
        )

        task2 = DummyOperator(
            task_id="task2_{}".format(id),
        )

        null = DummyOperator(
            task_id="null_{}".format(id)
        )

        join = DummyOperator(
            task_id='join_{}'.format(id),
            trigger_rule='one_success',
        )

        start >> branching
        branching >> task1 >> task2 >> join
        branching >> null >> join
        join >> end

