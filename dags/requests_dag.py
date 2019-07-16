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


dag = DAG('http_operator', default_args=default_args)


t1 = SimpleHttpOperator(
    task_id='get_tags',
    http_conn_id='recsys_api',
    endpoint='recsys/api/tags?all_live=false',
    method='GET',
    # data=json.dumps({"priority": 5}),
    # headers={"Content-Type": "application/json"},
    dag=dag,
)

t2 = SimpleHttpOperator(
    task_id='post_tag_ttl',
    http_conn_id='recsys_api',
    endpoint='recsys/api/tags',
    method='POST',
    data=json.dumps({"tag_id": "3cb-6bf0-446c-96ec-4d24b544f6e1-jmkfedo7", "ttl": 15}),
    headers={"accept": "application/json", "Content-Type": "application/json"},
    dag=dag,
)

t3 = SimpleHttpOperator(
    task_id='load_model',
    http_conn_id='recsys_api',
    endpoint='recsys/api/tags/c2a703cb-6bf0-446c-96ec-4d24b544f6e1-jmkfedo7/models',
    method='POST',
    data=json.dumps({"model_type": "item2vec", "model_path": "gs://cloudiq-jay-dev-01-long-term-storage/recommendations/y=2019/m=02/d=18/c2a703cb-6bf0-446c-96ec-4d24b544f6e1-jmkfedo7/embedding_vector.txt", "model_metadata_path": "gs://cloudiq-jay-dev-01-long-term-storage/recommendations/y=2019/m=02/d=18/c2a703cb-6bf0-446c-96ec-4d24b544f6e1-jmkfedo7/products.jsonl"}),
    headers={"accept": "application/json", "Content-Type": "application/json"},
    dag=dag,
)


t4 = SimpleHttpOperator(
    task_id='recommend',
    http_conn_id='recsys_api',
    endpoint='recsys/api/tags/c2a703cb-6bf0-446c-96ec-4d24b544f6e1-jmkfedo7/recommendations',
    method='POST',
    data=json.dumps({"basket": [{ "name": "string", "url": "/e27-plain-antique-brass-lampholder-with-20mm-base-thread-4303534/", "image": "string"}]}),
    headers={"accept": "application/json", "Content-Type": "application/json"},
    xcom_push=True,
    dag=dag,
)
# you don't have a get method. So clicking the url will be: The method is not allowed for the requested URL.


t1 > t2 > t3 > t4

# configs:
# Conn Id   : recsys_api
# Conn Type : HTTP
# Host      : localhost

