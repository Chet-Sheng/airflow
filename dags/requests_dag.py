from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

from airflow.hooks.http_hook import HttpHook

import json
from datetime import timedelta, datetime

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator

from airflow.contrib.operators.mlengine_operator import MLEngineTrainingOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor

MODEL_NAME = "train_item2vec"
ts = "{{ execution_date.strftime('%Y%m%d_%H%M%S') }}"

PROJECT_ID = 'davide-playgroud-0001'
BUCKET = 'davide-playgroud-0001-ml-platform'
REGION = 'europe-west1'


default_args = {
    'owner': 'Jay',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=31),
}


def get_list_of_tags_from_api():
    tags = []
    http = HttpHook(method='GET', http_conn_id='recsys_api')

    tags_ttls = json.loads(
        http.run(
            endpoint='recsys/api/tags?all_live=false',
            data=None,
            headers=None,
            extra_options=None
        ).text
    )
    for i in tags_ttls:
        tags.append(i['tag_id'])
    return tags


tags = get_list_of_tags_from_api()
print(tags)


dag = DAG('RecsysAPI', default_args=default_args, schedule_interval='0 */2 * * *')


for tag_id in tags:

    output_dir = BUCKET
    job_id = '{0}_item2vec_training_{1}'.format(tag_id[:8], ts)
    # path_merchant = "gs://cloudiq-jay-dev-01-long-term-storage/recommendations/y=2019/m=02/d=18/{}".format(tag_id)  # [TODO]
    path_merchant = "gs://davide-playgroud-0001-ml-platform/assets/{}".format(tag_id)  # [TODO]
    training_file = path_merchant + "/train.txt"
    eval_file = training_file  # [TODO]
    testing_file = path_merchant + "/test.txt"
    product_file = path_merchant + '/products.jsonl'
    # JOB_DIR = 'gs://cloudiq-jay-dev-01-temp/product-recommender/' + MODEL_NAME + '_' + ts
    JOB_DIR = 'gs://davide-playgroud-0001-ml-platform/assets/' + MODEL_NAME + '_' + ts
    PACKAGE_URI = 'gs://davide-playgroud-0001-ml-platform/assets/trainer-0.1.tar.gz'

    training_args = ['--job-dir=' + JOB_DIR,
                     '--train-files=' + training_file,
                     '--eval-files=' + eval_file,
                     '--test-files=' + testing_file,
                     '--products-files=' + product_file,
                     '--learning-rate=0.01',
                     '--adam-beta2=0.99',
                     '--train-steps= 10000',
                     '--train-batch-size=268',
                     '--eval-steps=1000',
                     '--eval-batch-size=268',
                     '--embed-dim=16',
                     '--optimizer=adam',
                     '--top-k=10',
                     '--eval-every-secs=20',
                     '--save-checkpoints-steps=500',
                     '--dropout-input=0',
                     '--num-skips=1',
                     '--num-sampled=64',
                     '--exporter=final',
                     '--is-experiemnt']

    train_data_sensor = GoogleCloudStorageObjectSensor(
        task_id='TrainDataSensor',
        # gcp_conn_id='google_cloud_default',
        google_cloud_conn_id='google_cloud_default',  # [TODO] set this conn_id
        bucket=BUCKET,
        object='assets/{}/train.txt'.format(tag_id),
        dag=dag
    )

    ml_engine_training = MLEngineTrainingOperator(
        task_id='MLEngineTraining',
        project_id=PROJECT_ID,
        job_id=job_id,
        package_uris=[PACKAGE_URI],
        training_python_module='trainer.task',
        training_args=training_args,
        region=REGION,
        scale_tier='BASIC',
        gcp_conn_id='google_cloud_default',
        dag=dag,
    )

    load_model = SimpleHttpOperator(
        task_id='LoadModel',
        http_conn_id='recsys_api',
        endpoint='recsys/api/tags/{}/models'.format(tag_id),
        method='POST',
        data=json.dumps({"model_type": "item2vec", "model_path": path_merchant+"/embedding_vector.txt", "model_metadata_path": product_file}),
        headers={"accept": "application/json", "Content-Type": "application/json"},
        dag=dag,
    )

    # recommendation = SimpleHttpOperator(
    #     task_id='Recommendation',
    #     http_conn_id='recsys_api',
    #     endpoint='recsys/api/tags/{}/recommendations'.format(tag_id),
    #     method='POST',
    #     data=json.dumps({"basket": [{ "name": "string", "url": "/e27-plain-antique-brass-lampholder-with-20mm-base-thread-4303534/", "image": "string"}]}),
    #     headers={"accept": "application/json", "Content-Type": "application/json"},
    #     xcom_push=True,
    #     dag=dag,
    # )
    # you don't have a get method. So clicking the url will be: The method is not allowed for the requested URL.

    train_data_sensor > ml_engine_training > load_model


# Configs: Running Locally for Now (testing)
# Conn Id   : recsys_api
# Conn Type : HTTP
# Host      : localhost


