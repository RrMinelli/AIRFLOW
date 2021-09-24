# [START import_module]
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
# [END import_module]

# [START Query MongoDB Data Collection Produtos]
# def query_mongo_collection():
import pymongo
import json
import pandas as pd
from pandas.io.json import json_normalize

# [START MongoDB Connector]
    # client = pymongo.MongoClient('mongodb://root:VQLnZB1QIp@mongodb.airflow.svc.cluster.local:27017')
# def test_iron():
    # from airflow.hooks.base_hook import BaseHook
    # conn = BaseHook.get_connection('iron_analytics_db')
    # show_collection = conn._user.find().pretty()
    # db = conn.
    # print(conn)
    # print(show_collection)
    # db = client.analytics_db
    # show_collection = db['_user'].find().pretty()
    # print(show_collection)


#Teste Cris INICIO
def test_iron():
    # from mongo_plugin.hooks.mongo_hook import MongoHook
    from airflow.providers.mongo.hooks.mongo import MongoHook  
    mongo = MongoHook(conn_id=iron_analytics_db)
    db = mongo.analytics_db
    for x in db["_user"].find():
        df = pd.json_normalize(x)
    print(df)
#Teste Cris FINAL
# [END MongoDB Connector]

# [START default_args]
default_args = {
    'owner': 'david fachini',
    'depends_on_past': False,
    'email': ['david.fachini@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'iron-test',
    default_args=default_args,
    description='Test Connection with Mongo-Iron',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['mongodb', 'conn', 'iron'],
) as dag:
# [END instantiate_dag]

# [START basic_task]
    # query_mongo_task = PythonVirtualenvOperator(
    query_mongo_task = PythonOperator(
        task_id='test_conn',
        python_callable=test_iron,
        # requirements=["pymongo"],
        provide_context=True,
        # ti.xcom_push=True,
    )

query_mongo_task