"""
Dag de prueba.

Importante: no borrar.

Autor: Luis Esteban Santamaría Blanco. Ingeniero de Datos Proyecto Masiva NEPS.

"""
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
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

db_table = "dimTestTable"
db_tmp_table = "tmp_TestTable"
dag_name = 'dag_' + db_table


def func_hola_mundo():
    print("Iniciando proceso DAG...")

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_dimTestTable():
    print("Hola mundo")

    query = f"""
        select top 100
            idDiagnostic
            ,code
            ,name as diagnosticDescription
            ,isMale
            ,isfemale
        from
            dbo.diagnostics
    """
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    print(df.columns)
    print(df.dtypes)
    print(df)

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)


# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}


with DAG(dag_name, catchup=False, default_args=default_args,
    # Se establece la ejecución del dag a las 8:50 am (hora servidor) todos los Jueves
    # schedule_interval= '50 8 * * 04',  # '@hourly'
    schedule_interval = None,
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    print_hola_mundo_python_task = PythonOperator(task_id = "print_hola_mundo",
                                                    python_callable = func_hola_mundo,
                                                    email_on_failure=True, 
                                                    # email='BI@clinicos.com.co',
                                                    email='Proyecto2@manar.com.co',
                                                    dag=dag
                                                    )

    # Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dimTestTable_python_task = PythonOperator(task_id = "get_dimTestTable",
                                                    python_callable = func_get_dimTestTable,
                                                    email_on_failure=True, 
                                                    # email='BI@clinicos.com.co',
                                                    email='Proyecto2@manar.com.co',
                                                    dag=dag
                                                    )
    

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>  print_hola_mundo_python_task >> get_dimTestTable_python_task >> task_end