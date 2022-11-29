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

db_table = "factControlActivities"
db_tmp_table = "tmp_factControlActivities"
dag_name = 'dag_' + db_table

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_factControlActivities ():


    now = datetime.now()
    last_week = now - timedelta(days=15)
    last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
    print(now)
    print(last_week)

    # LECTURA DE DATOS  
    query = f"""
        select
            EVE.idEHREvent,EVE.idPatient,EVE.idPractitioner, EVE.actionRecordedDate
            ,P.idProduct
            ,EAEG.idActualRegister
            ,EAEG.idNewRegister
        from
            dbo.encounters EN
            ,dbo.encounterRecords ENR
            ,dbo.EHREvents EVE
            ,dbo.encounterHCActivityEventGroups EAEG
            ,dbo.products P
                    
        where
            EN.idEncounter = EVE.idEncounter
            and EN.idEncounter = ENR.idEncounter
            and EN.dateStart > '{last_week}'
            and EVE.idEHREvent = EAEG.idEHREvent
            and EAEG.idProduct = P.idProduct
    """
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    # print(df.columns)
    # print(df.dtypes)
    # print(df)

    # PROCESAMIENTO
    df['actionRecordedDate'] = pd.to_datetime(df['actionRecordedDate'], errors='coerce')
    df['actionRecordedDate'] = df['actionRecordedDate'].astype(str)


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
    # Se establece la ejecución del dag a las 11:30 am (hora servidor) todos los Jueves
    schedule_interval= '30 11 * * 04',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_factControlActivities_python_task = PythonOperator(task_id = "get_factControlActivities",
                                                            python_callable = func_get_factControlActivities,
                                                            # email_on_failure=True, 
                                                            # email='BI@clinicos.com.co',
                                                            # dag=dag
                                                            )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_factControlActivities = MsSqlOperator(task_id='Load_factControlActivities',
                                                mssql_conn_id=sql_connid,
                                                autocommit=True,
                                                sql="EXECUTE sp_load_factControlActivities",
                                                # email_on_failure=True, 
                                                # email='BI@clinicos.com.co',
                                                # dag=dag,
                                                )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_factControlActivities_python_task >> load_factControlActivities >> task_end