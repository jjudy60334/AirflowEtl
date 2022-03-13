import logging
import os
import sys
from datetime import timedelta

# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG

sys.path.append(os.path.join(os.path.dirname(__file__), '../..', 'plugins'))
# from tools import task_slack_alert, task_fail_slack_alert, filter_execution_date  # noqa: E402
from operators import SQLTemplatedPythonOperator, SQLTemplatedKubernetesPodOperator  # noqa

log = logging.getLogger()
docs = """
kklook 爬蟲專案

"""
job_name = 'crawl_klook_mongo'  # 如果是 k8sPodOperator 不支援底線

# basic setting
config = Variable.get("config", deserialize_json=True)
template_searchpath = os.path.join(config['sql_path'], job_name)
utils_path = os.path.join(config['sql_path'], 'utils')

# Setting varibles for this job
schedule_interval = "00 16 * * *"

stage = Variable.get("stage")
# stage = 'prod'
# stage = 'staging'
target_conn = {
    "staging": "mongo",
    "production": "mongo"}
image = "clairewu/crawler:1.0.1"

mongo_conn = BaseHook.get_connection(target_conn[stage])
params = {
    "MongoDb": "klook",
    "Mongoport": mongo_conn.port,
    "MongoHost": mongo_conn.host,
    "start_date": '{{ tomorrow_ds }}',
    "Taiwan_country_id": 14,
    "size": 50,
    "schema_path": "crawler/schema"  # 改名new 跟其他跟播劇一樣
}

# Setting dags

default_args = {
    'owner': 'airflow',
    'start_date': '2022-03-10',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'wait_for_downstream': False,
    'depends_on_past': False,
    'params': params,
    'sleep_time': 1,
    # 'on_failure_callback': task_fail_slack_alert(slack_conn_id),
}

dag = DAG(job_name, default_args=default_args,
          template_searchpath=[template_searchpath, utils_path],
          schedule_interval=schedule_interval,
          max_active_runs=1, tags=['crawler'])
dag.doc_md = docs
with dag:
    COMPLETE = DummyOperator(
        task_id='All_Tasks_Completed'
    )
    t1 = DockerOperator(
        task_id="Activity_crawler",
        dag=dag,
        image=image,
        api_version='auto',
        command=["python",
                 "/app/crawler.py",
                 "klook-activity-pipeline",
                 "--country_id", params['Taiwan_country_id'],
                 "--size", params['size'],
                 "--schema_path", params['schema_path'],
                 "--mongo_db", params["MongoDb"],
                 "--mongo_port", params["Mongoport"],
                 "--mongo_host", params["MongoHost"],
                 ],
        docker_url='tcp://host.docker.internal:1234',
        container_name=image
    )
    t2 = DockerOperator(
        task_id="Review_crawler",
        image=image,
        dag=dag,
        command=["python",
                 "/app/crawler.py",
                 "klook-activity-pipeline",
                 "--activity_ids", "{{ ti.xcom_pull(task_ids='Activity_crawler') }}",
                 "--size", params['size'],
                 "--schema_path", params['schema_path'],
                 "--mongodb", params["MongoDb"],
                 "--mongoport", params["Mongoport"],
                 "--mongohost", params["MongoHost"],
                 ],

        docker_url='tcp://host.docker.internal:1234',
    )
    t1 >> t2 >> COMPLETE
