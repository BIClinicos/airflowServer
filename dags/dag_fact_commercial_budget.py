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
# from selenium import webdriver
# from selenium.webdriver.chrome.options import options
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get, normalize_str_categorical


#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pipeline'
dirname = '/opt/airflow/dags/files_pipeline/'
filename = 'fact_commercial_budget.xlsx'
db_table = "fact_commercial_budget"
db_tmp_table = "tmp_fact_commercial_budget"
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
    df = pd.read_excel(path, header = 1)

    df = df.iloc[:3]

    # Columnas a dinamizar
    df_m = df.columns[2:14]

    # Se anula la dinamización de las columnas de los meses para crear una sola columna de fecha y otra columna de monto
    df = pd.melt(df.reset_index(), id_vars=["Unnamed: 0", 'Unnamed: 1'], value_vars=df_m, var_name='date', value_name='value')

    df = df.drop(['Unnamed: 1'], axis=1)

    df = df.rename(
        columns = {
            'Unnamed: 0' : 'classification',
            'date' : 'date',
            'value' : 'value'
        }
    )
    # Normalización de columna Classification
    df['classification'] = normalize_str_categorical(df['classification'])

    # Columna valué como tipo entero
    df['value'] = df['value'].astype(int)

    return df

def func_get_fact_commercial_budget ():    

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    

    print(df.columns)
    print(df.dtypes)
    print(df.head(20))
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
    schedule_interval= '40 2 6 6,12 *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_fact_commercial_budget_python_task = PythonOperator(task_id = "get_fact_commercial_budget",
                                                           python_callable = func_get_fact_commercial_budget,
                                                           email_on_failure=True, 
                                                           email='BI@clinicos.com.co',
                                                           dag=dag,
                                                           )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dim_contracts_classification = MsSqlOperator(task_id='Load_dim_contracts_classification',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_dim_contracts_classification",
                                       email_on_failure=True, 
                                       email='BI@clinicos.com.co',
                                       dag=dag,
                                       )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_commercial_budget = MsSqlOperator(task_id='Load_fact_commercial_budget',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_fact_commercial_budget",
                                       email_on_failure=True, 
                                       email='BI@clinicos.com.co',
                                       dag=dag,
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_fact_commercial_budget_python_task >> load_dim_contracts_classification >> load_fact_commercial_budget>> task_end