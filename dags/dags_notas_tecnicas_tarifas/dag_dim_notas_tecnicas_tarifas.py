import os,sys

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta,date

import pandas as pd

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(root_dir)
from utils import load_df_to_sql, read_excel
from variables import sql_connid,sql_connid_gomedisys


#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'TmpTblHApoyoDiag'
db_table = "TblHApoyoDiag"
dag_name = 'dag_' + db_table

# Para correr manualmente las fechas
#fecha_texto = '2023-04-21 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

# Para correr fechas con delta
now = datetime.now()
last_week = datetime(2023,1,1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')


path_folder = os.path.dirname(__file__)
file_name_apoyo_diag = "TblHApoyoDiag.xlsx"
file_name_tarifas_consultas = "TblHTarifarioConsultas.xlsx"

def load_tblhapoyo_diag():
    data = read_excel(path_folder,file_name_apoyo_diag,None)
    load_df_to_sql(data, db_tmp_table, sql_connid)

def load_tblhtarifario_consultas():
    data = read_excel(path_folder,file_name_tarifas_consultas,None)
    load_df_to_sql(data, db_tmp_table, sql_connid)
    
# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}



with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval='15 7 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    
    
    load_apoyodiag_stating= PythonOperator(
                                     task_id = "load_apoyodiag_stating",
                                     python_callable = load_tblhapoyo_diag,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    load_tblhtarifarioconsultas_stating= PythonOperator(
                                     task_id = "load_tblhtarifarioconsultas_stating",
                                     python_callable = load_tblhtarifario_consultas,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> load_apoyodiag_stating >>load_tblhtarifarioconsultas_stating>>task_end
