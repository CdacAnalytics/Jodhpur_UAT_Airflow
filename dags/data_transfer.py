from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.generic_transfer import GenericTransfer
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
    
    subject = f'Airflow Alert: Task Failure in {dag_id}'
    body = f"""
    Task ID: {task_id}
    DAG ID: {dag_id}
    Execution Date: {execution_date}
    
    Task failed and retries exhausted. Manual intervention required.
    """
    
    # Using Airflow's send_email function for consistency and better integration
    send_email(
        to='v.harshalaharicdac@gmail.com',
        subject=subject,
        html_content=body
    )

# Default arguments for the DAG
default_args = {
    'owner': 'Gaurav',
    'start_date': datetime(2023, 11, 22, tzinfo=time_zone),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_alert
}

# Define the DAG
with DAG(
        dag_id="Data_transfer",
        default_args=default_args,
        description="Transferring the data from the source to destination DB",
        schedule_interval='0 0 * * *',  # Schedule interval set to every day at midnight
        catchup=False
    ) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS OPD_count(
            Date TIMESTAMP NOT NULL,
            OPD_count INT NOT NULL,
            date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
    )

    transfer_data = PostgresOperator(
        task_id='transfer_data',
        postgres_conn_id='source_conn_id',
        sql='''
        COPY (SELECT trunc(gdt_entry_date) as Date,
                     count(hrgnum_puk) as OPD_count
              FROM hrgt_episode_dtl
              WHERE gnum_isvalid = 1
              AND gnum_hospital_code = 22914
              GROUP BY trunc(gdt_entry_date))
        TO '/tmp/staging_data.csv' WITH CSV;
        '''
    )

    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='destination_conn_id',
        sql='''
        COPY OPD_count (Date, OPD_count)
        FROM '/tmp/staging_data.csv' WITH CSV;
        '''
    )

    create_table >> transfer_data >> load_data