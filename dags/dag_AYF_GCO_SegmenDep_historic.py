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
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'segmentacion'
dirname = '/opt/airflow/dags/files_segmentacion/'
filename = 'AYF_GCO_SegmenDep.csv'
db_table = "AYF_GCO_SegmenDep"
db_tmp_table = "tmp_AYF_GCO_SegmenDep"
dag_name = 'dag_' + db_table + '_historic'

# test comment
# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_tables (path):
    print("Leyendo dataframe")
    df = pd.read_csv(path, sep = ";")


    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ','_')
    df.columns = df.columns.str.replace('á','a')
    df.columns = df.columns.str.replace('é','e')
    df.columns = df.columns.str.replace('í','i')
    df.columns = df.columns.str.replace('ó','o')
    df.columns = df.columns.str.replace('ú','u')
    df.columns = df.columns.str.replace('ñ','ni')


    df = df.drop(['unnamed:_0'], axis=1)


    df['anio_mes'] = pd.to_datetime(df['anio_mes'], format="%Y-%m-%d")

    columns_int = [
            'anio',
            'cantidad',
            'femenino',
            'masculino',
            'numero_de_registros'
        ]


    for col in columns_int:
        df[col] = pd.to_numeric(df[col], downcast='integer')


    print("dataframe leido")
    print(df)
    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_AYF_GCO_SegmenDep ():

    
    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', -1)
    print(df.dtypes)
    print(df.columns)

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)


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
    # Se establece la ejecución del dag los días 3 de cada mes a las 5:00 am(Hora servidor)
    schedule_interval= '0 5 3 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_AYF_GCO_SegmenDep_python_task = PythonOperator(task_id = "get_AYF_GCO_SegmenDep",
        python_callable = func_get_AYF_GCO_SegmenDep)
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_AYF_GCO_SegmenDep = MsSqlOperator(task_id='Load_AYF_GCO_SegmenDep',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_AYF_GCO_SegmenDep",
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_AYF_GCO_SegmenDep_python_task >> load_AYF_GCO_SegmenDep >> task_end