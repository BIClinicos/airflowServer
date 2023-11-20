"""
Proyecto: Masiva NEPS

author dag: Luis Esteban Santamaría. Ingeniero de Datos.
Fecha creación: 08/09/2023

"""

# Librerias
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


#  Creación de variables

db_table = "TblDParametrosInformeOficina"
db_tmp_table = "TmpParametrosInformeOficina"
dag_name = 'dag_' + db_table

def func_get_dimParametrosInformeOficina ():


    query = f"""
        SELECT
            idCompany as idOficina,
            codeReport as codigoInforme,
            codeField as codigoCampo,
            valueField as valorCampo
        FROM companyReportParams;         
    """

    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    print(df.columns)
    print(df.dtypes)
    print(df.isna().sum()) # conteo de nulos por campo
    print(df)

    # Si la consulta no es vacía, carga el dataframe a la tabla temporal en BI.
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
    # Se establece la ejecución del dag a las 9:10 am (hora servidor) todos los Jueves
    schedule_interval= '5 19 * * SAT', # cron expression
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dimParametrosInformeOficina = PythonOperator(task_id = "get_dimParametrosInformeOficina",
                                                                python_callable = func_get_dimParametrosInformeOficina,
                                                                email_on_failure=True, 
                                                                email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dimParametrosInformeOficina = MsSqlOperator(task_id='Load_dimParametrosInformeOficina',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDParametrosInformeOficina",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dimParametrosInformeOficina >> load_dimParametrosInformeOficina >> task_end