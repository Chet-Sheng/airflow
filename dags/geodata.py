import logging
import os

import wget
import zipfile
import shutil

from datetime import timedelta, datetime

# from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
# from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.gcp_bigquery_plugin import \
    GoogleCloudStorageToBigQueryOperatorNg as GoogleCloudStorageToBigQueryOperator

logger = logging.getLogger(__name__)

ip_schema = [
    {'name': 'network', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'geoname_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'registered_country_geoname_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'represented_country_geoname_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'is_anonymous_proxy', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'is_satellite_provider', 'type': 'INTEGER', 'mode': 'NULLABLE'}
]
location_schema = [
    {'name': 'geoname_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'locale_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'continent_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'continent_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'country_iso_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'country_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'is_in_european_union', 'type': 'INTEGER', 'mode': 'NULLABLE'},
]

default_args = {
    "owner": "Jay",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 20),
    "retries": 3,
    "catchup": False,  # only run latest
    "retry_delay": timedelta(minutes=31),
}

dag = DAG(
    dag_id='AddGeolocation',
    default_args=default_args,
    schedule_interval=None,
)


def unzip_file(local_file, extract_dir):
    with zipfile.ZipFile(local_file, 'r') as zip_ref:
        zip_ref.extractall(path=extract_dir)


def get_geolite(remote_path, local_file_name, **kwargs):
    file_dir = "/tmp/geolite_{}".format(kwargs['ds_nodash'])
    if os.path.exists(file_dir) and os.path.isdir(file_dir):
        shutil.rmtree(file_dir)

    os.mkdir(file_dir)

    local_file_path = file_dir + '/' + local_file_name
    wget.download(remote_path,
                  local_file_path,
                  bar=None)
    unzip_file(local_file_path, file_dir)

    src_dir = [x[0] for x in os.walk(file_dir) if x[0] is not file_dir][0]
    os.symlink(src_dir, file_dir + '/GeoLite2-Country-CSV')


get_geolite_task = PythonOperator(
    task_id='GetGeolite',
    python_callable=get_geolite,
    provide_context=True,
    op_kwargs={
        'remote_path': "https://geolite.maxmind.com/download/geoip/database/GeoLite2-Country-CSV.zip",
        'local_file_name': "GeoLite2-Country-CSV.zip"
    },
    dag=dag
)

upload_geoip_to_gcs = FileToGoogleCloudStorageOperator(
    task_id='GeoIPToGCSOperator',
    src="/tmp/geolite_{{ ds_nodash }}/GeoLite2-Country-CSV/GeoLite2-Country-Blocks-IPv4.csv",
    dst="geolite/{{ execution_date.strftime('y=%Y/m=%m/d=%d') }}/GeoLite2-Country-Blocks-IPv4.csv",
    bucket="cloudiq-jay-dev-01-long-term-storage",
    mime_type='text/csv',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)

upload_geolocation_to_gcs = FileToGoogleCloudStorageOperator(
    task_id='GeoLocationToGCSOperator',
    src="/tmp/geolite_{{ ds_nodash }}/GeoLite2-Country-CSV/GeoLite2-Country-Locations-en.csv",
    dst="geolite/{{ execution_date.strftime('y=%Y/m=%m/d=%d') }}/GeoLite2-Country-Locations-en.csv",
    bucket="cloudiq-jay-dev-01-long-term-storage",
    mime_type='text/csv',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)

load_csv_ip = GoogleCloudStorageToBigQueryOperator(
    task_id='GCSToBigQueryIPv4',
    bucket="cloudiq-jay-dev-01-long-term-storage",
    source_objects=["geolite/{{ execution_date.strftime('y=%Y/m=%m/d=%d') }}/GeoLite2-Country-Blocks-IPv4.csv"],
    destination_project_dataset_table='analytics_platform_temp.geolite2_courtry_blocks_ipv4',
    source_format='CSV',
    schema_fields=ip_schema,
    skip_leading_rows=1,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='google_cloud_default',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)

load_csv_location = GoogleCloudStorageToBigQueryOperator(
    task_id='GCSToBigQueryLocation',
    bucket="cloudiq-jay-dev-01-long-term-storage",
    source_objects=["geolite/{{ execution_date.strftime('y=%Y/m=%m/d=%d') }}/GeoLite2-Country-Locations-en.csv"],
    destination_project_dataset_table='analytics_platform_temp.geoLite2_country_locations',
    source_format='CSV',
    schema_fields=location_schema,
    skip_leading_rows=1,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='google_cloud_default',
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)

end = DummyOperator(task_id="End", dag=dag)

get_geolite_task >> upload_geoip_to_gcs >> load_csv_ip >> end
get_geolite_task >> upload_geolocation_to_gcs >> load_csv_location >> end
