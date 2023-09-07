"""
Proyecto: Masiva NEPS

author dag: Luis Esteban Santamaría. Ingeniero de Datos.
Fecha creación: 28/08/2023

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

db_table = "TblDMedicionesDeMonitoria"
db_tmp_table = "TmpMedicionesDeMonitoria"
dag_name = 'dag_' + db_table


# Para correr manualmente las fechas
fecha_texto = '2023-01-31 00:00:00'
now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

now = now.strftime('%Y-%m-%d %H:%M:%S')
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')



# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_dimMedicionesDeMonitoria ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)

    query_legacy = f"""
        SELECT
            Todo.idEventoEHR,
            Todo.idUsuarioPaciente,
            Todo.idIngreso,
            Todo.idMonitoreo,
            Todo.idMedicion,
            Todo.fechaEvento, 
            Todo.ValorARegistrar,
            Todo.nombreDelElemento,
            Todo.esNumerico,
            Todo.isActive,
            Todo.tieneNotasMedicas,
            Todo.esParaTelemedicina
        FROM (
            SELECT DISTINCT
                EHREvCust.idEHREvent as idEventoEHR,
                Enc.idUserPatient as idUsuarioPaciente,
                Eve.idEncounter as idIngreso,
                EHREvCust.idMonitoring as idMonitoreo,
                EHREvCust.idMedition as idMedicion,
                EHREvCust.value as ValorARegistrar,
                EHREvCUsM.name as nombreDelElemento,
                EHREvCUsM.isNumeric as esNumerico,
                EHREvCUsM.isActive as isActive,
                Eve.haveMedicalNotes as tieneNotasMedicas,
                Eve.isForTelemedicine as esParaTelemedicina,
                Eve.actionRecordedDate as fechaEvento,
                ROW_NUMBER() over( partition by EHREvCust.idEHREvent,
                                                Enc.idUserPatient,
                                                Eve.idEncounter,
                                                EHREvCust.idMonitoring,
                                                EHREvCust.idMedition ORDER BY Eve.actionRecordedDate DESC) as Indicador
            FROM EHREventICUMonitoringMeditions AS EHREvCust
            INNER JOIN EHRConfUCIMonitoringMeditions AS EHREvCUsM WITH(NOLOCK)
                ON EHREvCust.idMonitoring = EHREvCUsM.idMonitoring
            INNER JOIN EHREvents AS Eve WITH(NOLOCK)
                ON EHREvCust.idEHREvent = Eve.idEHREvent
            INNER JOIN encounters as Enc WITH(NOLOCK)
                ON Eve.idEncounter = Enc.idEncounter
        WHERE Eve.actionRecordedDate >='{last_week}' AND Eve.actionRecordedDate<'{now}') AS Todo
        WHERE Indicador=1
    """
    
    query = f"""
       SELECT
            MM.idIngreso,
            MM.idEventoEHR,
            MM.idUsuarioPaciente,
            MM.idMonitoria,
            MM.idEventoAEvaluar,
            MM.valorARegistrar,
            MM.descripcionElemento,
            MM.esNumerico,
            MM.isActive,
            MM.fechaEvento
        FROM (
        SELECT
            Eve.idEncounter as idIngreso,
            ICU.idEHREvent as idEventoEHR,
            ENC.idUserPatient as idUsuarioPaciente,
            ICU.idMonitoring as idMonitoria,
            ICU.idMedition as idEventoAEvaluar,
            ICU.value as valorARegistrar,
            UCI.name as descripcionElemento,
            UCI.isNumeric as esNumerico,
            Eve.isActive,
            Eve.actionRecordedDate as fechaEvento
        FROM EHREventICUMonitoringMeditions ICU
        INNER JOIN EHREvents AS Eve ON ICU.idEHREvent = Eve.idEHREvent
        INNER JOIN [dbo].[encounters] ENC on  Eve.idEncounter=ENC.idEncounter
        INNER JOIN EHRConfUCIMonitoringMeditions UCI ON ICU.idMonitoring = UCI.idMonitoring AND ICU.idMedition = UCI.idMedition
        WHERE Eve.actionRecordedDate >='{last_week}' AND Eve.actionRecordedDate<'{now}') AS MM
    """

    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    # conversión de campos
    df['fechaEvento'] = df['fechaEvento'].astype(str)

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
    schedule_interval= None, # '10 9 * * 04', # cron expression
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dimMedicionesDeMonitoria = PythonOperator(task_id = "get_dimMedicionesDeMonitoria",
                                                                python_callable = func_get_dimMedicionesDeMonitoria,
                                                                #email_on_failure=False, 
                                                                # email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dimMedicionesDeMonitoria = MsSqlOperator(task_id='Load_dimMedicionesDeMonitoria',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDMedicionesDeMonitoria",
                                        # email_on_failure=True, 
                                        # email='BI@clinicos.com.co',
                                        dag=dag
                                       )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dimMedicionesDeMonitoria >> load_dimMedicionesDeMonitoria >> task_end