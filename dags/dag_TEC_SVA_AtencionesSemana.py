import os
import xlrd
import re
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from utils import remove_accents_cols, regular_snake_case, remove_special_chars
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pqrs'
dirname = '/opt/airflow/dags/files_pqrs/'
filename = 'TEC_SVA_AtencionesSemana.xlsx'
db_table = "TEC_SVA_AtencionesSemana"
db_tmp_table = "tmp_TEC_SVA_AtencionesSemana"
dag_name = 'dag_' + db_table
container_to = 'historicos'


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def transform_tables (path):

    # lectura del dataframe desde el archivo csv
    # df = pd.read_csv(path, sep = ";")
    df = pd.read_excel(path)
    
    df.columns = remove_accents_cols(df.columns)

    df.columns = remove_special_chars(df.columns)

    df.columns = regular_snake_case(df.columns)
    
    print("Como está leyendo el dataframe inicialmente",df)
    print("Nombres y tipos de columnas leídos del dataframe sin transformar",df.dtypes)



    # Estandarización de las columnas de fechas a tipo fecha

    date_columns = ['semana']

    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        df[i] = pd.to_datetime(df[i], format="%Y-%m-%d", errors = 'coerce')


    df = df[['sede', 'unidad', 'semana', 'atenciones']]

    return df

def func_get_TEC_SVA_AtencionesSemana ():    

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    print(df.columns)
    print(df.dtypes)
    print(df.head(20))
    print(df)

    df = df.drop_duplicates(subset=['sede', 'unidad', 'semana'])
    
    print(df)


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
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '10 10 * * fri',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_TEC_SVA_AtencionesSemana_python_task = PythonOperator(task_id = "get_TEC_SVA_AtencionesSemana",
                                                              python_callable = func_get_TEC_SVA_AtencionesSemana,
                                                              email_on_failure=True, 
                                                              email='BI@clinicos.com.co',
                                                              dag=dag,
                                                              )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_TEC_SVA_AtencionesSemana = MsSqlOperator(task_id='Load_TEC_SVA_AtencionesSemana',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_TEC_SVA_AtencionesSemana",
                                       email_on_failure=True, 
                                       email='BI@clinicos.com.co',
                                       dag=dag,
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_TEC_SVA_AtencionesSemana_python_task >> load_TEC_SVA_AtencionesSemana>> task_end