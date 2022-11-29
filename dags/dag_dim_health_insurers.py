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
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,remove_accents_cols, regular_snake_case, remove_special_chars, normalize_str_categorical

#  Se nombran las variables a utilizar en el dag
wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pipeline'
dirname = '/opt/airflow/dags/files_pipeline/'
filename = 'TablaReferencia_CodigoEAPByNit__1.xlsx'
container_to = 'historicos'
db_tmp_table = "tmp_dim_health_insurer"
dag_name = 'dag_' + 'dim_health_insurer'

# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

def transform_tables(path):
    df = pd.read_excel(path)


    # Estandarización de los nombres de columnas del dataframe
    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)
    
    names = {
        'codigo':'code',
        'nombre':'name',
        'descripcion':'description',
        'habilitado':'enabled',
        'extra_i_tiporegimen':'regime',
        'extra_ii_tipoid':'id_type',
        'extra_iii_nroid':'nit',
        'extra_iv_tipoeapb':'check_digit',
        'extra_v_poblacion':'nit_code',
        'extra_vi_email':'email_code',
        'extra_vii_orden':'order',
        'extra_viii':'type',
        'extra_ix':'type_detail',
        'extra_x':'email',
        'fecha_actualizacion':'update_date'
    }

    print('bf rename',df.columns)
    df=df.rename(columns=names)
    print('aft rename',df.columns)

    categorical_columns = ['code','name','enabled','regime','id_type','nit','nit_code','type','type_detail','email']
    for col in categorical_columns:
        df[col] = normalize_str_categorical(df[col].astype(str))
        

    df = df.drop_duplicates(subset=['code'])

    df['nit'].replace('.0','')

    df = df[['code', 'name', 'description', 'enabled','regime', 'id_type', 'nit',
       'check_digit', 'nit_code', 'email_code', 'order', 'type', 'type_detail',
       'email','update_date']]

    df['enabled'] = df['enabled'].str.contains('SI').astype(int)

    format_no_float_cols = ['nit','check_digit']
    for col in format_no_float_cols:
        df[col] = df[col].astype(str).str.replace('.0','', regex=False)

    return df
# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_dim_health_insurer ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)

    # PROCESAMIENTO
    df = transform_tables(path)
    
    print(df.columns)
    print(df.dtypes)
    print(df)

    # CARGA A BASE DE DATOS
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
    # TODO> set schedule interval
    schedule_interval= '30 4 8 * *', 
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dim_health_insurer_python_task = PythonOperator(task_id = "get_dim_health_insurer",
                                                            python_callable = func_get_dim_health_insurer,
                                                            email_on_failure=True, 
                                                            email='BI@clinicos.com.co',
                                                            dag=dag
                                                            )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dim_health_insurer = MsSqlOperator(task_id='Load_dim_health_insurer',
                                                mssql_conn_id=sql_connid,
                                                autocommit=True,
                                                sql="EXECUTE sp_load_dim_health_insurer",
                                                email_on_failure=True, 
                                                email='BI@clinicos.com.co',
                                                dag=dag
                                                )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dim_health_insurer_python_task >> load_dim_health_insurer >> task_end