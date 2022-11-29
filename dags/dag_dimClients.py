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

db_table = "dimClient"
db_tmp_table = "tmp_dimClient"
dag_name = 'dag_' + db_table

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_dimClient ():



# Se puso la condición con el or idUser = 24 por que el el isSupplier de este cliente aparece como cero, este idUser corresponde a ENTIDAD PROMOTORA DE SALUD SERVICIO OCCIDENTAL DE SALUD S.A. S.O.S. y existe un contrato con clinicos
    query = f"""
        Select
            idUser
            ,businessName
            ,documentNumber
            ,checkDigit
        from
            dbo.users
        where
            isSupplier = 1 or idUser = 24
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
# Se declara el DAG con sus respectivos parámetros
with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag a las 8:30 am hora servidor todos los Jueves
    schedule_interval= '30 8 * * 04',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dimClient_python_task = PythonOperator(task_id = "get_dimClient",
                                        python_callable = func_get_dimClient,
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                                )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dimClient = MsSqlOperator(task_id='Load_dimClient',
                                    mssql_conn_id=sql_connid,
                                    autocommit=True,
                                    sql="EXECUTE sp_load_dimClient",
                                    email_on_failure=True, 
                                    email='BI@clinicos.com.co',
                                    dag=dag
                                    )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dimClient_python_task >> load_dimClient >> task_end