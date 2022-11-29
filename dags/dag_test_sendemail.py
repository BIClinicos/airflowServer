import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators import email_operator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df

#  Se nombran las variables a utilizar en el dag

dag_name = 'dag_test_sendemail'
dirname = '/opt/airflow/dags/generated_files/'
query_test = "SELECT TOP (1000) * FROM [dbo].[users]"
today = date.today().strftime("%Y-%m-%d")
filename = f'Reporte_{today}.csv'
filename2 = f'Reporte_{today}.xlsx'
def func_get_table ():
    pass
    # df = sql_2_df(query_test, sql_conn_id=sql_connid_gomedisys)
    # print(df)
    # df.to_csv(dirname+filename, index=False)
    # df.to_excel(dirname+filename2, index=False)


  
    
# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}
# Se declara el DAG con sus respectivos parámetros
with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece el cargue de los datos el primer día de cada mes.
    schedule_interval= '0 8 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    get_table = PythonOperator(task_id = "get_table",
        python_callable = func_get_table)

    email_summary = email_operator.EmailOperator(
        task_id='email_summary',
        to='rafael.belalcazar@ia-aplicada.com',
        subject='Sample Email',
        html_content="HTML content",
        # files=[f'{dirname}{filename}',f'{dirname}{filename2}']
        )
        
    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_table >> email_summary >> task_end