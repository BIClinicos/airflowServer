import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.wasb_hook import WasbHook
from datetime import datetime, timedelta
from datetime import date

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container_name = 'gestionfinanciera'
blob_name_existente= 'pruebaexistente.txt'
blob_name_subir = 'pruebasubida.txt'
file_path ='/opt/airflow/dags/subir.txt'
file_path2 ='/opt/airflow/dags/bajar.txt'

def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container_name,blob_name_existente))

def file_upload():
    wb.load_file(file_path,container_name,blob_name_subir)
    print ('Archivo subido con python operator')
    return('Blob uploaded sucessfully')

def file_delete():
    wb.delete_file(container_name,blob_name_subir, is_prefix=False, ignore_if_missing=True)
    print ('Archivo borrado con python operator')
    return('Blob deleted sucessfully')

def file_get():
    wb.get_file(file_path2, container_name, blob_name_subir)
    print ('Archivo halado con python operator')
    return('Blob getted sucessfully')

def respond():
    return 'Task ended'


default_args = {
    'owner': 'datamiles',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

with DAG('dag_test_blob',
    catchup=False,
    default_args=default_args,
    # schedule_interval='*/10 * * * *',
    schedule_interval=None,
    max_active_runs=1
    ) as dag:

    start_task = DummyOperator(task_id='dummy_start')

    check_connection_opr = PythonOperator(task_id='connection',
                                          python_callable=check_connection)
    
    file_delete_opr = PythonOperator(task_id='file_deleter',
                                     python_callable=file_delete)
    
    file_upload_opr = PythonOperator(task_id='file_uploader',
                                     python_callable=file_upload)
   
    file_get_opr = PythonOperator(task_id='file_getter',
                                  python_callable=file_get)
 

    opr_respond = PythonOperator(task_id='task_end',
                                 python_callable=respond)
                                 

start_task >> check_connection_opr >> file_delete_opr >> file_upload_opr  >> file_get_opr >> opr_respond