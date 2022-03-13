import logging
import os
import sys
from datetime import timedelta

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
sys.path.append(os.path.join(os.path.dirname(__file__), '../..', 'plugins'))
from tools import fileter_mongo2csv_github
# noqa: E402

log = logging.getLogger()
docs = """
output 4顆星以下的評論

"""
job_name = 'output_low_rate_klook'  # 如果是 k8sPodOperator 不支援底線

# basic setting
config = Variable.get("config", deserialize_json=True)
template_searchpath = os.path.join(config['sql_path'], job_name)
utils_path = os.path.join(config['sql_path'], 'utils')

# Setting varibles for this job
schedule_interval = "00 16 * * *"

stage = Variable.get("stage")

target_conn = {
    "staging": "mongo",
    "production": "mongo"}

mongo_conn = BaseHook.get_connection(target_conn[stage])
git_conn = BaseHook.get_connection("git")
folder = 'LowRateReview'
file_name = 'output_low_rate_klook.csv'
params = {
    "collection": 'review',
    "filter": {'score': {"$lt": 4}},
    "MongoDb": mongo_conn.schema,
    "MongoPort": 27017,
    "MongoHost": mongo_conn.host,
    "MongoUserName": mongo_conn.login,
    "MongoPassword": mongo_conn.password,
    "start_date": '{{ tomorrow_ds }}',
    'token': git_conn.extra,
    'push_repo': 'jjudy60334/LowRateReview',
    'output_file_path': os.path.join(os.path.dirname(__file__), folder),
    'file_name': file_name}

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

}

dag = DAG(job_name, default_args=default_args,
          template_searchpath=[template_searchpath, utils_path],
          schedule_interval=schedule_interval,
          max_active_runs=1, tags=['crawler'])
dag.doc_md = docs
with dag:
    PREVIOUS = ExternalTaskSensor(
        task_id='Previous_Run_crawl_klook_mongo',
        external_dag_id='crawl_klook_mongo',
        timeout=5 * 60,
        external_task_id='All_Tasks_Completed',
        allowed_states=['success']
    )

    COMPLETE = DummyOperator(
        task_id='All_Tasks_Completed'
    )

    fetch_low_rate = PythonOperator(
        task_id='fileter_mongo2csv_github',
        provide_context=True,
        python_callable=fileter_mongo2csv_github,
        op_kwargs={
            'mongo_host': params["MongoHost"],
            'mongo_port': params["MongoPort"],
            'mongo_user_name': params["MongoUserName"],
            'mongo_password': params["MongoPassword"],
            'db': params["MongoDb"],
            'collection': params['collection'],
            'collection_filter': params["filter"],
            'commit_message': params['start_date'],
            'output_file_path': params['output_file_path'],
            'file_name': params['file_name']})
    git_push_command = f"""
            cd {params['output_file_path']};
            git config --global user.email "airflow@example.com"
            git config --global user.name "airflow"
            git add {params['file_name']};
            git commit -m "{params['start_date']}";
            git push https://{params['token']}@github.com/{params['push_repo']}.git;
            """

    gitpush_1_data = BashOperator(task_id='gitpush_1_data',
                                  bash_command=git_push_command)

    PREVIOUS >> fetch_low_rate >> gitpush_1_data >> COMPLETE

    #
