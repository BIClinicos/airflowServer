from cmath import nan
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
container = 'satisfacciongral'
dirname = '/opt/airflow/dags/files_satisfacciongral/'
filename = 'TEC_SVA_SatisfaccionGral.xlsx'
db_table = "TEC_SVA_SatisfaccionGral"
db_tmp_table = "tmp_TEC_SVA_SatisfaccionGral"
dag_name = 'dag_' + db_table


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_data (path):
    # Lectura 
    df = pd.read_excel(path)
    # Estandarización de los nombres de columnas del dataframe
    df.columns = df.columns.str.replace('\n|\xa0|\t',' ',regex=True)
    df.columns = df.columns.str.replace(':','').str.strip()
    if 'Nombre completo' in df.columns:
        next
    elif 'Nombre' and 'Nombre2' in df.columns:
        df['Nombre completo'] = df['Nombre'] + ' ' + df['Nombre2']
    elif 'Nombre' in df.columns:
        df['Nombre completo'] = df['Nombre']
    elif 'Nombre' not in df.columns:
        df['Nombre completo'] = ''

    # Correcciones por cambio en formato 15/12/2022
    df['¿Cómo calificas tu experiencia global respecto a los servicios de salud que has recibido a través de Clínicos?'] = df['¿Cómo calificas tu experiencia global respecto a los servicios de salud que has recibido a través de tu punto de atención?']
    df['Basados en tu última atención médica ¿Recomendarías Clínicos con tus conocidos?'] = df['Basados en tu última atención médica ¿Recomendarías tu punto de atención con tus conocidos?']
    #

    df = df[[
            'ID',
            'Hora de inicio',
            'Hora de finalización',
            'Correo electrónico',
            'Nombre completo',
            'Número de documento',
            'EPS',
            'Contrato',
            'Sede de atención',
            '¿Cómo calificas nuestra atención medica prestada?',
            '¿El servicio médico prestado suplió tus necesidades?',
            '¿Cómo calificas tu experiencia global respecto a los servicios de salud que has recibido a través de Clínicos?',
            'Basados en tu última atención médica ¿Recomendarías Clínicos con tus conocidos?'
        ]]   
    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_TEC_SVA_SatisfaccionGral ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_data(path)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', -1)
    print(df.dtypes)
    print(df.head(20))

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
    # Se establece el cargue de los datos el primer día de cada mes.
    schedule_interval= '0 10 * * sat',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_TEC_SVA_SatisfaccionGral_python_task = PythonOperator(task_id = "get_TEC_SVA_SatisfaccionGral",
        python_callable = func_get_TEC_SVA_SatisfaccionGral,
        email_on_failure=True,
        email='BI@clinicos.com.co',
        dag=dag,)
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_TEC_SVA_SatisfaccionGral = MsSqlOperator(task_id='Load_TEC_SVA_SatisfaccionGral',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_TEC_SVA_SatisfaccionGral",
                                       email_on_failure=True,
                                       email='BI@clinicos.com.co',
                                       dag=dag,)

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_TEC_SVA_SatisfaccionGral_python_task >> load_TEC_SVA_SatisfaccionGral >> task_end