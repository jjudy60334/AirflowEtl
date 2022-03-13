import logging
from copy import deepcopy
from croniter import croniter
from datetime import datetime

import pandas as pd
import pyarrow  # noqa
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from pyathena import connect as pyathena_conn
from pyathena.converter import Converter, _DEFAULT_CONVERTERS, _to_default
from pyathena.pandas_cursor import PandasCursor
import pandas as pd
from pymongo import MongoClient
logger = logging.getLogger()


def task_fail_slack_alert(slack_conn_id, slack_conn_channel=None):
    if slack_conn_channel is None:
        config = Variable.get("config", deserialize_json=True)
        slack_conn_channel = config.get('slack_conn_channel')

    def send_slack_alert(context):
        task_id = context.get('task_instance').task_id
        slack_msg = """
                :red_circle: Task Failed.
                *Task*: {task}
                *Dag*: {dag}
                *Execution Time*: {exec_date}
                *Log Url*: {log_url}
                """.format(task=task_id,
                           dag=context.get('task_instance').dag_id,
                           exec_date=context.get('execution_date'),
                           log_url=context.get('task_instance').log_url)

        failed_alert = SlackWebhookOperator(
            http_conn_id=slack_conn_id,
            webhook_token=BaseHook.get_connection(slack_conn_id).password,
            message=slack_msg,
            channel=slack_conn_channel,
            # channel='#test',
            username='airflow',
            task_id=task_id)
        return failed_alert.execute(context)
    return send_slack_alert


def task_slack_alert(slack_conn_id, channel, message):
    if not channel.startswith('#'):
        channel = f"#{channel}"

    def send_slack_alert(context):
        task_id = context.get('task_instance').task_id
        slack_msg = f"""
                :red_circle: {message}.
                *Task*: {task_id}
                *Dag*: {context.get('task_instance').dag_id}
                *Execution Time*: {context.get('execution_date')}
                *Log Url*: {context.get('task_instance').log_url}
                """

        failed_alert = SlackWebhookOperator(
            http_conn_id=slack_conn_id,
            webhook_token=BaseHook.get_connection(slack_conn_id).password,
            message=slack_msg,
            channel=channel,
            username='airflow',
            task_id=task_id)
        return failed_alert.execute(context)
    return send_slack_alert


def sql_reader(sql_file):
    sql = open(sql_file, 'r', encoding='utf8')
    sql_string = sql.read()
    sql.close()
    return sql_string


def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]


def read_mongo(
        db, collection, query={},
        host='localhost', port=27017, username=None,
        password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id:
        del df['_id']
    return df


def fileter_mongo2csv_github(
        mongo_host, mongo_user_name, mongo_password, mongo_port, db, collection, output_file_path, file_name,
        collection_filter):
    df = read_mongo(host=mongo_host, username=mongo_user_name, password=mongo_password,
                    port=mongo_port, db=db, collection=collection, query=collection_filter)
    df.to_csv(f'{output_file_path}/{file_name}')
    return True
