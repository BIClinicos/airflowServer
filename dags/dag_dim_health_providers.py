import os
import xlrd
import re
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from utils import remove_accents_cols, regular_snake_case, remove_special_chars, normalize_str_categorical
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel, read_html
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,normalize_str_categorical
# from bs4 import BeautifulSoup


#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pipeline'
dirname = '/opt/airflow/dags/files_pipeline/'
filename = 'Prestadores.xls'
db_table = 'dim_health_providers'
db_tmp_table = 'tmp_dim_health_providers'
dag_name = 'dag_' + db_table
container_to = 'historicos'

# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def transform_tables(path):
    
    raw_data = pd.read_html(path, header=0)
    print('raw_data',raw_data)
    df = raw_data[0]  #Esta función permite leer la lista cómo un dataframe

    print("df",df)
    print("df.dtypes",df.dtypes)
    # Estandarización de los nombres de columnas del dataframe
    
    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    #df.columns = regular_snake_case(df.columns) #esta función no deberíamos usarla dado qué las columnas ya vienen con snake_case

    print("columns formated",df.columns)

    cat_columns = [
        'depa_nombre', 
        'muni_nombre', 
        'nombre_prestador',
        'razon_social', 
        'clpr_nombre',
        'direccion', 
        'caracter', 
        'clase_persona', 
        'naju_nombre',
        'rep_legal'
        ]

    for i in cat_columns:
        df[i] = normalize_str_categorical(df[i].astype(str))

    # Estandarización de las columnas de fechas a tipo fecha

    date_columns = ['fecha_radicacion','fecha_vencimiento']

    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        df[i] = pd.to_datetime(df[i], format="%Y-%m-%d", errors = 'coerce')


    df = df[['depa_nombre', 'muni_nombre', 'codigo_habilitacion', 'nombre_prestador',
        'nits_nit', 'razon_social', 'clpr_codigo', 'clpr_nombre', 'ese', 'direccion', 
        'telefono','email','nivel','caracter', 'habilitado', 'fecha_radicacion', 
        'fecha_vencimiento','dv', 'clase_persona', 'naju_codigo', 
       'naju_nombre','numero_sede_principal', 'fecha_corte_REPS','rep_legal']]

    dict = {'depa_nombre':'dep_name', 'muni_nombre':'town_name', 'codigo_habilitacion':'habilitation_code', 
        'nombre_prestador':'lender_name','nits_nit':'nits_nit', 'razon_social':'business_code', 'clpr_codigo':'clpr_code', 
        'clpr_nombre':'clpr_name', 'ese':'ese', 'direccion':'adress', 'telefono':'telephone_number','email':'email',
        'nivel':'level','caracter':'character', 'habilitado':'enabled', 'fecha_radicacion':'filing_date', 
        'fecha_vencimiento':'expiration_date','dv':'dv', 'clase_persona':'person_type', 'naju_codigo':'naju_code', 
        'naju_nombre':'naju_name','numero_sede_principal':'principal_headquarter_number', 'fecha_corte_REPS':'REPS_cutoff_date',
        'rep_legal':'legal_representative'}

    df.rename(dict,axis='columns', inplace=True)

    return df

def func_get_dim_health_providers():


    path = dirname + filename
    print(path)

    df = transform_tables(path)
    print('columnas',df.columns)
    print(df.dtypes)
    print(df.head(20))
    print(df)


    df = df[
        ['dep_name', 
        'town_name',
        'habilitation_code',
        'lender_name',
        'nits_nit',
        'business_code',
        'clpr_code',
        'clpr_name',
        'ese',
        'adress',
        'telephone_number',
        'email',
        'level',
        'character',
        'enabled',
        'filing_date',
        'expiration_date',
        'dv', 'person_type',
        'naju_code',
        'naju_name',
        'principal_headquarter_number',
        'REPS_cutoff_date',
        'legal_representative'
        ]
    ]
 
    print('columnas de las tablas',df.columns) 

    
    df = df.drop_duplicates(subset=['habilitation_code'])

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)


# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}
# Se declara el DAG con sus respectivos parámetros
with DAG(
    dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '20 4 8 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    get_dim_health_providers = PythonOperator(task_id = "get_dim_health_providers",
                                                python_callable = func_get_dim_health_providers,
                                                email_on_failure=True, 
                                                email='BI@clinicos.com.co',
                                                dag=dag,
                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dim_health_providers = MsSqlOperator(task_id='Load_dim_health_providers',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_dim_health_providers",
                                       email_on_failure=True, 
                                       email='BI@clinicos.com.co',
                                       dag=dag,
                                       )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dim_health_providers >> load_dim_health_providers >>task_end