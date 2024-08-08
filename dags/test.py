from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.generic_transfer import GenericTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
import logging
import pendulum

# Setting the time as Indian Standard Time
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

def print_data():
    sql_query1 = ''' 
        select count(*) as dest_row_count
        from air_dag_success
        where TRUNC(execution_date) = TRUNC(SYSDATE);
    '''

    sql_query2 = ''' 
        select count(hrgnum_puk) as source_row_count
        from hrgt_episode_dtl
        where gnum_isvalid = 1
        and gnum_hospital_code = 22914
        --and TRUNC(gdt_entry_date) = TRUNC(SYSDATE);'''
    
    dest_hook_dest = PostgresHook(postgres_conn_id='destination_conn_id', schema='Airflow_destination')
    sour_hook_dest = PostgresHook(postgres_conn_id='aiimsnew_conn', schema='aiimsnew')

    dest_row_count = dest_hook_dest.get_records(sql_query1)
    sour_row_count = sour_hook_dest.get_records(sql_query2)

    logging.info(f"Query result: {dest_row_count}")
    logging.info(f"Query result: {sour_row_count}")
    return dest_row_count,sour_row_count

def print_xcom_result(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='compare_row')
    dest_row_count, sour_row_count = result
    logging.info(f"XCom destination row count: {dest_row_count}")
    logging.info(f"XCom source row count: {sour_row_count}")
    if sour_row_count != dest_row_count:
        send_alert(kwargs)
    else:
        logging.info('Row counts match')

default_args = {
    'owner': 'Gaurav',
    'start_date': datetime(2023, 11, 22, tzinfo=time_zone),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_alert
}

with DAG(
    dag_id="test_DAG",
    default_args=default_args,
    description="Transferring the data from the source to destination DB",
    schedule_interval='0 0 * * *',  # Schedule interval set to every day at midnight
    catchup=False
) as dag:

    compare_count = PythonOperator(
        task_id='compare_row',
        provide_context=True,
        python_callable=print_data
    )

    log_xcom_result = PythonOperator(
        task_id='log_xcom_result',
        provide_context=True,
        python_callable=print_xcom_result,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    compare_count >> log_xcom_result
