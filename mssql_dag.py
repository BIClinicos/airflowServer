from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime, timedelta
#import pymssql 
import pyodbc
#from utils import load_df_to_sql
#from variables import sql_connid

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1),
}


def test_db():
    server = 'tcp:pruebasyonas.database.windows.net' 
    database = 'dbpruebas' 
    username = 'yonatan' 
    password = 'Pollitosk8' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    print('succesful connection')


with DAG('Test_load_df_to_mssql_2',
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1) as dag:

    start_task = DummyOperator(task_id='dummy_start')

    update_df_temp_1 = PythonOperator(
        task_id='update_df_temp_1',
        python_callable=test_db
    )

start_task >> update_df_temp_1
