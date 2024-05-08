from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'santalovdv', # если не отработает, то 'owner': 'admin'
    'start_date': days_ago(0),
    'depends_on_past': False
}


with DAG (
    dag_id = "LOAD_EXTERNAL_TO_TARGET",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:

    t1 = PostgresOperator( 
    task_id ="LOAD",
    postgres_conn_id = "connection_name", # из вебинтерфейса airflow > admin > connections
    sql = """
            INSERT INTO schema_name.table_name (column_name) (select column_name from source_hive.table_name)
            ;
        """
    )

t1