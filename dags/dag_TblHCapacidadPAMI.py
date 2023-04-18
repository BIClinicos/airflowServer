import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from variables import sql_connid
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_contains_name,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pami-ecopetrol'
dirname = '/opt/airflow/dags/files_pami/'
filename = 'Instalada.xlsx'
db_tables = ['SAL_PRI_capacidad_instalada']
db_tmp_table = 'TmpTblHCapacidadPAMI'
dag_name = 'dag_TblHCapacidadPAMI'
container_to = 'historicos'


# Se realiza un chequeo de la conexión al blob storage
def check_connection(container, filename):
    print('Conexión OK')
    return wb.check_for_blob(container,filename)

# Función de transformación de los archivos xlsx
def transform_table(dir, filename):
    # Lectura y transformacion de df
    df = pd.read_excel(dir + filename, header = [0], sheet_name = 0)
    df.columns = ['sede', 'servicio', 'mes', 'capacidad_instalada']
    ### Cambiar de formato
    df['mes'] = pd.to_datetime(df['mes'], format='%Y/%m/%d')
    ### Converter a entera la capacidad
    df['capacidad_instalada'] = df['capacidad_instalada'].fillna(0) 
    df['capacidad_instalada'] = df['capacidad_instalada'].astype('int64') 
    ### Cambiar sede
    #dict_replace ={
    #    'Bulevar':'Clínicos IPS Sede Bulevar',
    #    'San Martin':'Clínicos IPS Sede San Martín',
    #    'Cartagnera':'Clínicos IPS Sede Cartagena'
    #}
    #df = df.replace({'sede':dict_replace})
    #
    print(df.info())
    print(df.head(5))
    return df


# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_TblHCapacidadPAMI():
    #
    path = dirname + filename
    print(path)
    check_connection(container, filename)
    file_get(path, container,filename, wb = wb)
    #
    df = transform_table(dirname, filename)
    print(df.columns)
    print(df.dtypes)
    print(df.tail(50))
    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)

    #for filename in filenames:
    #    move_to_history_contains_name(dirname, container, filename, container_to)


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
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '0 0 11 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_tmp_TblHCapacidadPAMI = PythonOperator(task_id = "Get_tmp_TblHCapacidadPAMI",
                                                  python_callable = func_get_TblHCapacidadPAMI,
                                                  email_on_failure=True, 
                                                  email='BI@clinicos.com.co',
                                                  dag=dag,
                                                  )
    
    # Carge de tabla de hechos
    load_TblHCapacidadPAMI = MsSqlOperator(task_id ='Load_TblHCapacidadPAMI',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblHCapacidadPAMI",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )
    
    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_tmp_TblHCapacidadPAMI >> load_TblHCapacidadPAMI >> task_end