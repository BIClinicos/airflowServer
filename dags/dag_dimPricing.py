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
from utils import load_df_to_sql,search_for_file_prefix, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,remove_accents_cols,remove_special_chars,regular_camel_case,regular_snake_case

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'compensardomi'
dirname = '/opt/airflow/dags/files_compensardomi/'
filename = 'dimPricing.xlsx'
db_table = "dimPricing"
db_tmp_table = "tmp_dimPricing"
dag_name = 'dag_' + db_table


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_tables (path):

    # lectura del archivo
    df = pd.read_excel(path, sheet_name='DOMICIALIARIA')

    #Estandarización nombres de columnas
    # remove_accents_cols,remove_special_chars,regular_camel_case
    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)
    df.columns = df.columns.map(regular_camel_case)

    drop_columns = ['transporte', 'epp']
    df = df.drop(drop_columns, axis = 1)

    # df['cups'] = pd.to_numeric(df['valorActividad'], errors = 'coerce', downcast="integer")
    # df['cups'] = df['cups'].fillna(0)
    # df['cups'] = df['cups'].astype(int)

    df['valorActividad'] = pd.to_numeric(df['valorActividad'], errors = 'coerce', downcast="integer")
    

    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_dimPricing ():

    path = dirname + filename
    file_get(path, container, filename, wb = wb)
    df = transform_tables (path)
    print(df)
    # print(df['cups'].head(30))
    # print(df['cups'].unique())
    # print(df['valorActividad'].head(30))
    # print(df['valorActividad'].unique())
    print(df.columns)
    print(df.dtypes)
    # df.to_excel(dirname + "output.xlsx")  

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
    # Se establece la ejecución del dag a las 8:00 am (hora servidor) todos los Jueves
    schedule_interval= '0 8 * * 04',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dimPricing_python_task = PythonOperator(task_id = "get_dimPricing",
                                                python_callable = func_get_dimPricing,
                                                email_on_failure=True, 
                                                email='BI@clinicos.com.co',
                                                dag=dag
                                                )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dimPricing = MsSqlOperator(task_id='Load_dimPricing',
                                    mssql_conn_id=sql_connid,
                                    autocommit=True,
                                    sql="EXECUTE sp_load_dimPricing",
                                    email_on_failure=True, 
                                    email='BI@clinicos.com.co',
                                    dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_dimPricing_python_task >> load_dimPricing >> task_end