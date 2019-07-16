import json
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('example_http_operator', default_args=default_args)


t1 = SimpleHttpOperator(
    task_id='post_op',
    endpoint='api/v1.0/nodes',
    data=json.dumps({"priority": 5}),
    headers={"Content-Type": "application/json"},
    dag=dag,
)

t1