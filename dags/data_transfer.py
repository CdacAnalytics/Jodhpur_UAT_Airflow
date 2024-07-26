from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.generic_transfer import GenericTransfer


from datetime import datetime, timedelta


default_args = {
    'owner': 'Gaurav',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
        dag_id="Data_transfer",
        start_date=datetime(2023, 11, 22),
        default_args=default_args,
        description="Transfering the data from the source to destination DB",
        #schedule_interval='@daily',
        schedule_interval=timedelta(minutes=30),
        catchup=False
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