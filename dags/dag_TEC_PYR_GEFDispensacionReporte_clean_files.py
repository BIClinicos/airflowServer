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
import subprocess

#  Se nombran las variables a utilizar en el dag
dag_name = 'dag_' + 'TEC_PYR_GEFDispensacionReporte_clean_files'
DIRNAME = '/dags/generated_files'


def func_list_and_clean_files():
    cwd = os.getcwd()
    cwd = cwd + DIRNAME
    onlyfiles = [os.path.join(cwd, f) for f in os.listdir(cwd) if os.path.isfile(os.path.join(cwd, f))]
    print(cwd)
    print(onlyfiles)
    for file in onlyfiles:
        comm = ['rm', file]
        process = subprocess.Popen(comm, stdout=(subprocess.PIPE))
        output, error = process.communicate()
        print(file, 'Delete sucessfully', output)


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
    # Se establece el cargue de los datos el día 3 de cada mes.
    schedule_interval= '50 2 6 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    # Se declara y se llama la función encargada de generar el reporte
    list_and_clean_files = PythonOperator(task_id = "list_and_clean_files",
        python_callable = func_list_and_clean_files,
        # email_on_failure=True, 
        # email='BI@clinicos.com.co',
        dag=dag
        )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> list_and_clean_files >> task_end