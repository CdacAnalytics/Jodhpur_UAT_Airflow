from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.generic_transfer import GenericTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
import logging

from datetime import datetime, timedelta
import pendulum
from airflow.utils.email import send_email
from email.mime.text import MIMEText
import smtplib

# setting the time as indian standard time. We have to set this if we want to schedule a pipeline 
time_zone = pendulum.timezone("Asia/Kolkata")

def send_alert(context):
    print('Task failed sending an Email')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    No_of_retries = default_args['retries']
    retry_delay = default_args['retry_delay']

    subject = f'Airflow Alert: Task Failure in {dag_id}'
    body = f"""
    <br>Task ID: {task_id}</br>
    <br>DAG ID: {dag_id}</br>
    <br>Execution Date: {execution_date}</br>
    <br>Retries: {No_of_retries}</br>
    <br>Delay_between_retry: {retry_delay}</br>
    <br>Task failed and retries exhausted. Manual intervention required.</br>
    """
    
    # Using Airflow's send_email function for consistency and better integration
    send_email(
        to='gauravnagraleofficial@gmail.com',
        subject=subject,
        html_content=body
    )


def print_data(ti, **kwargs):
    source_row_count = ti.xcom_pull(task_ids='source_rowcnt')
    logging.info('Pulled source row count: %s', source_row_count)



default_args = {
    'owner': 'Gaurav',
    'start_date': datetime(2023, 11, 22, tzinfo=time_zone),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_alert
}

# Define the DAG
with DAG(
        dag_id="test_DAG",
        default_args=default_args,
        description="Transferring the data from the source to destination DB",
        schedule_interval='0 0 * * *',  # Schedule interval set to every day at midnight
        # 5 - Mins , 11-Hours ,* - any day of week ,*- any month,*-any day of week 
        catchup=False
    ) as dag:

    source_row_count = PostgresOperator(
        task_id = 'source_rowcnt',
        postgres_conn_id = 'postgres',
        sql = '''
            select count(hrgnum_puk) as source_row_count
            from hrgt_episode_dtl
            where gnum_isvalid = 1
            and gnum_hospital_code = 22914
            and TRUNC(gdt_entry_date) = TRUNC(SYSDATE);
            ''',
        do_xcom_push = True # this is how you push the query data 
    )

    compare_count = PythonOperator (
        task_id = 'compare_row',
        provide_context=True,
        python_callable=print_data
    )

    source_row_count >> compare_count

