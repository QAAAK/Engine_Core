from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
        "email": ["santalovdv@mts.ru"],  # santalovdv
        "email_on_failure": True,
        'start_date': "2024-06-04",
        "ssh_conn_id": "fob2b-en-001.msk.bd-cloud.mts.ru",
}



with DAG (
    dag_id = "load_guide_tr",
    default_args=default_args,   
   schedule_interval = '0 4 * * *',
    catchup = False) as dag:

    t1 = PostgresOperator( 
    task_id ="LOAD",
    postgres_conn_id = "core_gp", # из вебинтерфейса airflow > admin > connections
    sql =""" truncate table dds.spravochnik_tr 
            insert into dds.spravochnik_tr (
                region_id,
                region,
                reg_week2,
                tr 
            )
            (select 
                region_id,
                region,
                reg_week2,
                tr from dds.hive_spravochnik_tr)"""
    )

t1
