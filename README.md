**How to run start the AIRFLOW-server**: docker-compose up -d 
**How to stop the AIRFLOW-server:** docker-compose down -v (Dont shutdowm your server coz it will delete all the connections that you have created with DB)
**How to get into airflow CMD:**  Step1: docker-comose ps
                                  step2: docker exec -it test-airflow-airflow-scheduler-1 /bin/bash
                                  step3: airflow tasks test [your DAG name] [your_table_name] 2023-01-01
