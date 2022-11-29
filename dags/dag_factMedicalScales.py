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

db_table = "factMedicalScales"
db_tmp_table = "tmp_factMedicalScales"
dag_name = 'dag_' + db_table

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_factMedicalScales ():


    now = datetime.now()
    last_week = now - timedelta(days=15)
    last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
    print(now)
    print(last_week)

    # LECTURA DE DATOS  
    query = f"""
        select fms.*, EMD.idDiagnostic from (
        select
            EVE.idEHREvent,EVE.idPatient,EVE.idPractitioner, EVE.actionRecordedDate
            ,EMS.idEvaluation
            ,GA.idAction
            ,GS.idSpecialty,GS.name as specialityName
            ,CSV.[idScale],CSV.[name] as evaluationName--,CSV.minValue,CSV.maxValue
            ,sum(EMSQ.[value]) value
            
        from
            dbo.encounters EN
            ,dbo.encounterRecords ENR
            ,dbo.EHREvents EVE
            ,dbo.generalActions GA
            ,dbo.generalSpecialties GS
            ,dbo.EHREventMedicalScales EMS
            ,dbo.EHREventMedicalScaleQuestions EMSQ
            ,dbo.EHRConfScaleValorations CSV
        where
            EN.idEncounter = EVE.idEncounter
            and EN.dateStart > '{last_week}'
            and EN.idEncounter = ENR.idEncounter
            and EVE.idEHREvent = EMS.idEHREvent
            and EVE.idEHREvent = EMSQ.idEHREvent
            and GA.idAction = EVE.idAction
            and GS.idSpecialty = EVE.idSpeciality
            and CSV.[idScale] = 11
            and EMS.idScale = EMSQ.idScale
            and CSV.idScale = EMS.idScale
            and EMS.idEvaluation = CSV.[idRecord]
        group by 
            EMS.idEvaluation
            ,EVE.idEHREvent
            ,EVE.idPatient
            ,EVE.idPractitioner
            ,EVE.actionRecordedDate
            ,GA.idAction
            ,GS.idSpecialty
            ,GS.name
            ,CSV.[idScale]
            ,CSV.[name]
            ,CSV.minValue
            ,CSV.maxValue
            ) FMS LEFT JOIN (SELECT idDiagnostic, idEHREvent FROM EHREventMedicalDiagnostics WHERE isPrincipal = 1) EMD ON FMS.idEHREvent = EMD.idEHREvent 
    """
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    # print(df.columns)
    # print(df.dtypes)
    # print(df)

    # PROCESAMIENTO
    df['actionRecordedDate'] = pd.to_datetime(df['actionRecordedDate'], errors='coerce')
    df['actionRecordedDate'] = df['actionRecordedDate'].astype(str)

    # convert to int
    cols_int = ['idDiagnostic']
    for col in cols_int:
        df[col] = df[col].astype(str).str.replace('.0','')
        df[col] = df[col].replace('','nan')
        # df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')

    df = df.drop_duplicates(subset=['idEHREvent','idPatient','idPractitioner','idEvaluation','idAction','idSpecialty','idScale'])

    print(df.columns)
    print(df.dtypes)
    print(df)

    print(df[df['idEHREvent']==2214900])

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
    # Se establece la ejecución del dag a las 10:40 am (hora servidor) todos los Jueves
    schedule_interval= '40 10 * * 04',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_factMedicalScales_python_task = PythonOperator(task_id = "get_factMedicalScales",
                                                        python_callable = func_get_factMedicalScales,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_factMedicalScales = MsSqlOperator(task_id='Load_factMedicalScales',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql="EXECUTE sp_load_factMedicalScales",
                                            email_on_failure=True, 
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_factMedicalScales_python_task >> load_factMedicalScales >> task_end