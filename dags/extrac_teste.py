import airflow
import mongodb.config as config
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.operators.picpay_plugin import EmrStepSensor
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
from slack_alert import SlackAlert
import tags


def create_dag(dag_id,
               raw_path,
               emr_conn_id,
               spark_task_path,
               spark_task_name,
               extraction_file,
               schedule_interval='30 3 * * *'):

    # default args to the DAG
    default_args = {
        'owner': 'picpay',
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
        tags=[tags.MONGODB, tags.EXTRACTION]
    )

    # json step to move the spark file located on s3 to /home/hadoop
    copy_spark_files_step = [{
        'Name': 'setup - copy spark task files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', spark_task_path, '/home/hadoop/']
        }
    }]

    # partition size for EMR
    partition_size = Variable.get(
        "extract_mongodb_collections_partition_size", default_var=22
    )

    # EMR cluster name
    job_flow_overrides = {
        'Name': 'Extract mongodb collections using EMR/Spark'
    }

    clusters = config.get_clusters(extraction_file)

    # init extraction
    extraction_init = DummyOperator(
        task_id='extraction_init',
        dag=dag
    )

    # waits for all extractions
    extraction_done = DummyOperator(
        task_id='extraction_done',
        trigger_rule='all_done',
        dag=dag
    )

    for cluster in clusters:

        # creates an EMR cluster on devops aws account
        cluster_creator = EmrCreateJobFlowOperator(
            task_id=f'create_job_flow_{cluster}',
            job_flow_overrides=job_flow_overrides,
            aws_conn_id='aws_devops',
            emr_conn_id=emr_conn_id,
            retries=4,
            retry_delay=timedelta(minutes=1),
            priority_weight=2000,
            weight_rule='absolute',
            dag=dag
        )

        extraction_init.set_downstream(cluster_creator)

        # moves the spark file located on s3 to /home/hadoop
        step_copy_spark_files = EmrAddStepsOperator(
            task_id=f'add_copy_spark_step_{cluster}',
            job_flow_id="{{{{ task_instance.xcom_pull('create_job_flow_{}', key='return_value') }}}}".format(cluster),
            aws_conn_id='aws_devops',
            steps=copy_spark_files_step,
            retries=4,
            retry_delay=timedelta(minutes=1),
            priority_weight=1500,
            weight_rule='absolute',
            dag=dag
        )

         # gets a list with each EMR step
        extract_table_steps = config.get_extract_table_steps(
            spark_task_name, cluster, raw_path, extraction_file, partition_size
        )

        # creates EMR extraction tasks
        emr_step_list = []  # list with all EMR steps
        emr_sensor_list = []  # list with all EMR step sensors
        step_priority = 1000
        sensor_priority = 500

        for step in extract_table_steps:
            step_list = [step]

            add_emr_step = EmrAddStepsOperator(
                task_id='add_emr_step_{}_{}'.format(cluster, step['Name']),
                job_flow_id="{{{{ task_instance.xcom_pull('create_job_flow_{}', key='return_value') }}}}".format(cluster),
                aws_conn_id='aws_devops',
                steps=step_list,
                retries=4,
                retry_delay=timedelta(minutes=1),
                priority_weight=step_priority,
                weight_rule='absolute',
                dag=dag
            )

            emr_step_sensor = EmrStepSensor(
                task_id=step['Name'],
                job_flow_id="{{{{ task_instance.xcom_pull('create_job_flow_{}', key='return_value') }}}}".format(cluster),
                step_id="{{{{ task_instance.xcom_pull('add_emr_step_{}_{}', key='return_value')[0] }}}}".format(
                    cluster, step['Name']),
                aws_conn_id='aws_devops',
                retries=2,
                priority_weight=sensor_priority,
                weight_rule='absolute',
                retry_delay=timedelta(minutes=1),
                dag=dag
            )

            step_priority = step_priority - 1
            sensor_priority = sensor_priority - 1

            emr_step_list.append(add_emr_step)
            emr_sensor_list.append(emr_step_sensor)

        # waits for all extractions
        cluster_done = DummyOperator(
            task_id=f'cluster_done_{cluster}',
            trigger_rule='all_done',
            dag=dag
        )


        # dependency
        cluster_creator.set_downstream(step_copy_spark_files)

        for elem in range(len(emr_step_list)):
            step_copy_spark_files.set_downstream(emr_step_list[elem])
            emr_step_list[elem].set_downstream(emr_sensor_list[elem])
            emr_sensor_list[elem].set_downstream(cluster_done)
            cluster_done.set_downstream(extraction_done)

    return dag