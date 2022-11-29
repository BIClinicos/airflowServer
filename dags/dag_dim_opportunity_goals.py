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
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix,get_files_xlsx_with_prefix, get_files_xlsx_contains_name, get_files_xlsx_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,remove_accents_cols,remove_special_chars,regular_camel_case,regular_snake_case,move_to_history_for_prefix,normalize_str_categorical
#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'oportunidad'
dirname = '/opt/airflow/dags/files_oportunidad/'
filename = 'dim_opportunity_goals.xlsx'
db_table = "dim_opportunity_goals"
db_tmp_table = "tmp_dim_opportunity_goals"
dag_name = 'dag_' + db_table
prefix = db_table


# Función de transformación de los archivos xlsx
def transform_tables (path):

    df = pd.read_excel(path)

    df = df.rename(
        columns = {
            'CLIENTE' : 'client',
            'PROCEDIMIENTO' : 'procedure',
            'CUPS' : 'cups',
            'META' : 'goal',
        }
    )

    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_dim_opportunity_goals ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    
    print(df)
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
    # se establece la ejecución a las 12:20 PM(Hora servidor) todos los sabados
    schedule_interval= '40 4 8 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')


    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dim_opportunity_goals_python_task = PythonOperator( task_id = "get_dim_opportunity_goals",
                                                        python_callable = func_get_dim_opportunity_goals,
                                                        # email_on_failure=True, 
                                                        # email='BI@clinicos.com.co',
                                                        # dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dim_opportunity_goals = MsSqlOperator( task_id='Load_dim_opportunity_goals',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql="EXECUTE sp_load_dim_opportunity_goals",
                                            # email_on_failure=True, 
                                            # email='BI@clinicos.com.co',
                                            # dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dim_opportunity_goals_python_task >> load_dim_opportunity_goals >> task_end