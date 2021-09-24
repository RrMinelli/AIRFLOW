import airflow
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pymongo
import json
import pandas as pd

def create_dag(dag_id,
               schedule_interval='30 3 * * *'):

    # default args to the DAG
    default_args = {
        'owner': 'iron',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(1),
        'on_failure_callback': SlackAlert().task_fail_slack_alert
    }

    # dag parameters
    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        max_active_runs=1,
        tags=['mongodb', 'extraction']
    )

    def test_iron():
        mongo = MongoHook(conn_id=iron_analytics_db)
        db = mongo.analytics_db
        for x in db["_user"].find():
            df = pd.json_normalize(x)
        print(df)


    query_mongo_task = PythonOperator(
        task_id='test_conn',
        python_callable=test_iron,
        # requirements=["pymongo"],
        provide_context=True,
        dag=dag
    )

    query_mongo_task

    return dag