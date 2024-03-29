import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime, timedelta
from datetime import date
from time import strptime
import pandas as pd
from pandas import read_excel
from variables import sql_connid
from utils import get_files_blob_with_prefix_args,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

# Fecha de ejecución del dag
today = date.today()
today = datetime.strptime('2023-06-01', '%Y-%m-%d')
format_month = today.strftime('%Y-%m')

#  Se nombran las variables a utilizar en el dag
wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'segmentacion'
dirname = '/opt/airflow/dags/files_segmentacion/'
filename = f'Atributos_{format_month}'
db_table = "AYF_GCO_SegmenDep"
db_tmp_table = "tmp_AYF_GCO_SegmenDep"
dag_name = 'dag_' + db_table

# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_tables (df):
    print("Leyendo dataframe")
    print("Como está leyendo el dataframe inicialmente",df)
    print("Nombres y tipos de columnas leídos del dataframe sin transformar",df.dtypes)

    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ','_')
    df.columns = df.columns.str.replace('á','a')
    df.columns = df.columns.str.replace('é','e')
    df.columns = df.columns.str.replace('í','i')
    df.columns = df.columns.str.replace('ó','o')
    df.columns = df.columns.str.replace('ú','u')
    df.columns = df.columns.str.replace('ñ','ni')

    df = df.drop(['tipo_identificacion'], axis=1)
    print("Como queda",df.dtypes)
    df['numero_mes'] = ''

    df.loc[df['mes'] == 'Enero', 'numero_mes'] = '1'
    df.loc[df['mes'] == 'Febrero', 'numero_mes'] = '2'
    df.loc[df['mes'] == 'Marzo', 'numero_mes'] = '3'
    df.loc[df['mes'] == 'Abril', 'numero_mes'] = '4'
    df.loc[df['mes'] == 'Mayo', 'numero_mes'] = '5'
    df.loc[df['mes'] == 'Junio', 'numero_mes'] = '6'
    df.loc[df['mes'] == 'Julio', 'numero_mes'] = '7'
    df.loc[df['mes'] == 'Agosto', 'numero_mes'] = '8'
    df.loc[df['mes'] == 'Septiembre', 'numero_mes'] = '9'
    df.loc[df['mes'] == 'Octubre', 'numero_mes'] = '10'
    df.loc[df['mes'] == 'Noviembre', 'numero_mes'] = '11'
    df.loc[df['mes'] == 'Diciembre', 'numero_mes'] = '12'

    df['anio_mes'] = df['anio'].astype(str) + '-' + df['numero_mes'] + '-' + '1'
    df['anio_mes'] = pd.to_datetime(df['anio_mes'], format="%Y-%m-%d")

    df = df.drop(['numero_mes'], axis=1)


    print("dataframe leido")
    print(df)
    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_AYF_GCO_SegmenDep ():

    print('filename',filename)
    df = get_files_blob_with_prefix_args(dirname,container,filename,wb, sep=',')
    df = transform_tables(df)
    # pd.set_option('display.max_rows', None)
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.width', None)
    # pd.set_option('display.max_colwidth', -1)
    print('DF transform_tables',df)
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
    # Se establece la ejecución del dag los días 15 de cada mes a las 12:00 am(Hora servidor)
    schedule_interval= '50 4 16 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_AYF_GCO_SegmenDep_python_task = PythonOperator(task_id = "get_AYF_GCO_SegmenDep",
        python_callable = func_get_AYF_GCO_SegmenDep,
        email_on_failure=True, 
        email='BI@clinicos.com.co',
        dag=dag
        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_AYF_GCO_SegmenDep = MsSqlOperator(task_id='Load_AYF_GCO_SegmenDep',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE sp_load_AYF_GCO_SegmenDep",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_AYF_GCO_SegmenDep_python_task >> load_AYF_GCO_SegmenDep >> task_end