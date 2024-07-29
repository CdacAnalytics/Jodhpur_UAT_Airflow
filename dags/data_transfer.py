from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.generic_transfer import GenericTransfer
from datetime import datetime, timedelta


from email.mime.text import MIMEText
import smtplib

def send_alert(context):
    msg = MIMEText("Task failed and retries exhausted. Manual intervention required.")
    msg['Subject'] = 'Airflow Alert: Data Transfer Failure'
    msg['From'] = 'gauravnagrale5@gmail.com'
    msg['To'] = 'vharshalahari@cdac.in'

    # Connect to Gmail's SMTP server
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()  # Upgrade the connection to a secure encrypted SSL/TLS connection
    s.login('gauravnagrale5@gmail.com', 'Shibu@2675')  # Login to the SMTP server
    s.sendmail('gauravnagrale5@gmail.com', ['vharshalahari@cdac.in'], msg.as_string())
    s.quit()


# if the DAG is failed for some reason this will ensure that the DAG attempts it onemore time or n no of times and we can also set after how many mins the step will be retried
default_args = {
    'owner': 'Gaurav', # this name can be used to monitoring and notification
    'retries': 2, # This defines no of times the Task should be repeated if the task is failed
    'retry_delay': timedelta(minutes=5), # delay between the retry
    'on_failure_callback': send_alert
}

with DAG(
        dag_id="Data_transfer", # DGA name
        start_date=datetime(2023, 11, 22),  # for this date the DGA will start running
        default_args=default_args, 
        description="Transfering the data from the source to destination DB",
        #schedule_interval='@daily',
        schedule_interval=timedelta(hours=24), # When this DAG intervals
        catchup=False  # This means that the DAG will not run for any missed intervals between the start_date and the current date.
        ) as dag:
    
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql = ''' 
        CREATE TABLE IF NOT EXISTS OPD_count(
            Date TIMESTAMP Not null ,
            OPD_count int Not null,
            date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
        '''
    )

    # Transfering the source data to the destination data: 
    # GenericTransfer task to upload data into the source table
    # first transfering the data to a staging area and then to the destination
    transfer_data = PostgresOperator(
        task_id='transfer_data',
        postgres_conn_id='source_conn_id', # defines at the AIRFLOW UI 
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
        postgres_conn_id='destination_conn_id',# defines at the AIRFLOW UI 
        sql='''
        COPY OPD_count (Date, OPD_count)
        FROM '/tmp/staging_data.csv' WITH CSV;
        '''
    )
    create_table >> transfer_data >> load_data