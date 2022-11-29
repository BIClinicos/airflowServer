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
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix,get_files_xlsx_with_prefix, get_files_xlsx_contains_name, get_files_xlsx_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,remove_accents_cols,remove_special_chars,regular_camel_case,regular_snake_case,move_to_history_for_prefix
#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'rotacionpersonas'
dirname = '/opt/airflow/dags/files_rotacion_personas/'
filename = 'THM_VYD_PlazasApr.xlsx'
db_table = "THM_VYD_PlazasApr"
db_tmp_table = "tmp_THM_VYD_PlazasApr"
dag_name = 'dag_' + db_table
prefix = db_table


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_tables (path):

    df = pd.read_excel(path)

    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)
    # df.columns = df.columns.map(regular_camel_case)

    #df['fecha_corte'] = pd.to_datetime('today')
    #df['fecha_corte'] = df['fecha_corte'].apply(lambda x: x.strftime('%Y-%m'))
    #df['fecha_corte'] = pd.to_datetime(df['fecha_corte'], format="%Y-%m")

    print(df)
    print(df.dtypes)
    print(df.columns)

    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_THM_VYD_PlazasApr ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    # move_to_history_for_prefix(dirname, container, prefix, container_to='historicos')


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
    # se establece la ejecución a las 12:00 PM(Hora Servidor) todos los 7 de cada més.
    schedule_interval= '50 4 8 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_THM_VYD_PlazasApr_python_task = PythonOperator( task_id = "get_THM_VYD_PlazasApr",
                                                        python_callable = func_get_THM_VYD_PlazasApr,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_THM_VYD_PlazasApr = MsSqlOperator( task_id='Load_THM_VYD_PlazasApr',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql="EXECUTE sp_load_THM_VYD_PlazasApr",
                                            email_on_failure=True, 
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_THM_VYD_PlazasApr_python_task >> load_THM_VYD_PlazasApr >> task_end