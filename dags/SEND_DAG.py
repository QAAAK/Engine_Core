from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.email import EmailOperator

default_args = {
        "email": ["santalovdv@mts.ru"],  # santalovdv
        "email_on_failure": True,
        'start_date': "2022-06-01",
        "ssh_conn_id": "oka-analyze-en-001.krd.bd-cloud.mts.ru",
}



with DAG (
    dag_id = "PILOT_SEND",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:

    t1 = PostgresOperator( 
    task_id ="LOAD",
    postgres_conn_id = "core_gp", # из вебинтерфейса airflow > admin > connections
    sql = """
            set role analyze_bi_owner;
            
            drop table if exists a_data_b2b.table_dag;
            
            CREATE TABLE a_data_b2b.table_dag (
                id int4 null,
                name varchar null,
                dt_birth date null
            ) distributed randomly;
        """
    ),   
    t2 = EmailOperator(
        task_id="send_test_email",
        to= "santalovdv@mts.ru",
        subject="Отчет о выполнении операции",
        html_content="<h3>Таблица была удалена а потом создана вновь</h3>" 
    )

t1 >> t2
