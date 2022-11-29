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

db_table = "dimPractitioner"
db_tmp_table = "tmp_dimPractitioner"
dag_name = 'dag_' + db_table

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_dimPractitioner ():


    now = datetime.now()
    last_week = now - timedelta(days=15)
    last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
    print(now)
    print(last_week)


    query = f"""
        select
            US.idUser,US.firstGivenName,US.secondGiveName,US.firstFamilyName,US.secondFamilyName
            ,UCTD.name as documentType,US.documentNumber
            ,UP.birthDate,UP.homeAddress,UP.telecom
            ,UCAS.name as gender
        from 
            dbo.users US left join dbo.userPeople UP left join dbo.userConfAdministrativeSex UCAS on UCAS.idAdministrativeSex = UP.idAdministrativeSex on US.idUser = UP.idUser
            ,dbo.userConfTypeDocuments UCTD
        where
            UCTD.idTypeDocument = US.idDocumentType
            and US.idUser in (select 
                                    distinct(EVE.idPractitioner)
                                from
                                    dbo.EHREvents EVE
                                    ,dbo.encounters EN
                                where
                                    EN.idEncounter = EVE.idEncounter
                                    and EN.dateStart > '{last_week}'
                                    )
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
    # Se establece la ejecución del dag a las 9:40 am (hora servidor) todos los Jueves
    schedule_interval= '40 9 * * 04',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dimPractitioner_python_task = PythonOperator(task_id = "get_dimPractitioner",
                                                        python_callable = func_get_dimPractitioner,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dimPractitioner = MsSqlOperator(task_id='Load_dimPractitioner',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE sp_load_dimPractitioner",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dimPractitioner_python_task >> load_dimPractitioner >> task_end