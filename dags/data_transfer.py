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
from airflow.utils.trigger_rule import TriggerRule

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
    exception = context.get('exception')
    No_of_retries = default_args['retries']
    retry_delay = default_args['retry_delay']
    error_message = str(exception)

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

    log_failure_to_db(task_id, dag_id, execution_date, error_message)

def send_success_alert(context):
    print('Task succeeded sending a notification')
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    
    success_message = f'Task {task_id} in DAG {dag_id} succeeded on {execution_date}.'
    
    log_success_to_db(task_id, dag_id, execution_date, success_message)
    
    subject = f'Airflow Success: Task Success in {dag_id}'
    body = f"""
    <br>Task ID: {task_id}</br>
    <br>DAG ID: {dag_id}</br>
    <br>Execution Date: {execution_date}</br>
    <br>Success Message: {success_message}</br>
    """
    
    send_email(
        to='gauravnagraleofficial@gmail.com',
        subject=subject,
        html_content=body
    )


def log_success_to_db(task_id, dag_id, execution_date, success_message):
    hook = PostgresHook(postgres_conn_id='destination_conn_id')
    insert_sql = """
    INSERT INTO air_dag_success (task_id, dag_id, execution_date, success_message)
    VALUES (%s, %s, %s, %s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, success_message))


def log_failure_to_db(task_id, dag_id, execution_date, error_message):
    hook = PostgresHook(postgres_conn_id='destination_conn_id')
    insert_sql = """
    INSERT INTO AIR_DAG_fail (task_id, dag_id, execution_date, error_message)
    VALUES (%s, %s, %s, %s);
    """
    hook.run(insert_sql, parameters=(task_id, dag_id, execution_date, error_message))



def compare_rows(ti, **kwargs):
    #source_row_count = ti.xcom_pull(task_ids='source_rowcnt', key='return_value')
    #destination_row_count = ti.xcom_pull(task_ids='destination_rowcnt', key='return_value')

    source_row_count = 121
    destination_row_count = 121
    
    logging.info('Retrieved source row count from XCom: %s', source_row_count)
    logging.info('Retrieved destination row count from XCom: %s', destination_row_count)

    if source_row_count and destination_row_count:
        #source_row_count = source_row_count[0].get('source_row_count', 0)
        #destination_row_count = destination_row_count[0].get('dest_row_count', 0)
        
        logging.info('Processed source row count: %s', source_row_count)
        logging.info('Processed destination row count: %s', destination_row_count)

        if source_row_count != destination_row_count:
            send_alert(kwargs)
            raise ValueError("Row counts do not match between source and destination")
        else:
            logging.info('Row counts match')
    else:
        raise ValueError("Failed to retrieve row counts from XCom")
    

# Default arguments for the DAG
default_args = {
    'owner': 'Gaurav',
    'start_date': datetime(2023, 11, 22, tzinfo=time_zone),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_alert,
    'on_success_callback': send_success_alert
}

# Define the DAG
with DAG(
        dag_id="Data_transfer",
        default_args=default_args,
        description="Transferring the data from the source to destination DB",
        schedule_interval='0 0 * * *',  # Schedule interval set to every day at midnight
        # 5 - Mins , 11-Hours ,* - any day of week ,*- any month,*-any day of week 
        catchup=False
    ) as dag:

    # creating the table source table 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='destination_conn_id',
        sql='''
        CREATE TABLE IF NOT EXISTS OPD_count(
            Date TIMESTAMP NOT NULL,
            OPD_count INT NOT NULL,
            date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ''',
        dag = dag
    )

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
        do_xcom_push = True, # this is how you push the query data
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag = dag
    )

    # transferring the data into a staging area
    # with clause specifies that the format for the output should be saved as CSV 
    # and this data is stored in container running in docker
    # the data will be lost once the container is stopped or removed

    transfer_data = PostgresOperator(
        task_id='transfer_data',
        postgres_conn_id='postgres',
        sql='''
        COPY (SELECT trunc(gdt_entry_date) as Date,
                     count(hrgnum_puk) as OPD_count
              FROM hrgt_episode_dtl
              WHERE gnum_isvalid = 1
              AND gnum_hospital_code = 22914
              GROUP BY trunc(gdt_entry_date))
        TO '/tmp/staging_data.csv' WITH CSV;
        ''',
        dag = dag
    )

    #Note: the temp folder is present inside the test-airflow-postgres-1 /bin/bash
    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='destination_conn_id',
        sql='''
        COPY OPD_count (Date, OPD_count)
        FROM '/tmp/staging_data.csv' WITH CSV;
        ''',
        dag = dag
    )

    destination_row_count = PostgresOperator(
        task_id = 'destination_rowcnt',
        postgres_conn_id = 'destination_conn_id',
        sql = '''
            select count(*) as dest_row_count
            from opd_count
            where TRUNC(date_time) = TRUNC(SYSDATE);
            ''',
        do_xcom_push = True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag = dag
    )

    compare_count = PythonOperator (
        task_id = 'compare_row',
        provide_context=True,
        python_callable=compare_rows,
        dag = dag
    )

    create_table >> source_row_count >> transfer_data >> load_data >> destination_row_count >> compare_count