import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.sensors.external_task_sensor  import ExternalTaskSensor
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,load_df_to_sql
import math


#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'TmpEventosDiagnosticosDetalles'
db_table = "TblHEventosDiagnosticosDetalles"
dag_name = 'dag_' + db_table


#Se halla las fechas de cargue de la data 

# Para correr manualmente las fechas
#fecha_texto = '2023-01-31 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

# Para correr fechas con delta
now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')


def func_get_events_diagnostics_detail():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)
    
    domiConsultas_query = f"""
    SELECT Todo.idAppointment,Todo.idDiagnostic,Todo.idEncounter,Todo.idUserPatient,Todo.actionRecordedDate 
    FROM (
    SELECT DISTINCT appointment.idAppointment,Dx.idDiagnostic,EVEN.idEncounter,ENC.idUserPatient,EVEN.actionRecordedDate
    ,ROW_NUMBER() over( partition by appointment.idAppointment,Dx.idDiagnostic,EVEN.idEncounter,enc.idUserPatient order by EVEN.actionRecordedDate desc) as Indicador
    FROM appointmentSchedulers AS AppS WITH (NOLOCK)
    INNER JOIN appointmentSchedulerSlots AS AppSd WITH (NOLOCK) ON AppS.idAppointmenScheduler = AppSd.idAppointmentScheduler
    INNER JOIN appointments AS appointment ON appointment.idAppointmentSchedulerSlots = AppSd.idAppointmentSchedulerSlots
    INNER JOIN encounters AS enc WITH(NOLOCK) ON appointment.idAdmission = enc.idEncounter
    INNER JOIN dbo.encounterRecords AS ENR WITH (NOLOCK) ON ENC.idEncounter = ENR.idEncounter
    INNER JOIN dbo.EHREvents AS EVEN WITH(NOLOCK) ON appointment.idUserPerson= EVEN.idPatient and EVEN.idEncounter IS NOT NULL
    INNER JOIN dbo.EHREventMedicalDiagnostics AS EVD WITH(NOLOCK) ON EVD.idEHREvent = EVEN.idEHREvent
    INNER JOIN dbo.diagnostics AS Dx WITH(NOLOCK) ON EVD.idDiagnostic = Dx.idDiagnostic
    WHERE EVEN.actionRecordedDate >='{last_week}' AND EVEN.actionRecordedDate<'{now}') as Todo
    WHERE Todo.Indicador=1
    """
    # Ejecutar la consulta capturandola en un dataframe
    df = sql_2_df(domiConsultas_query, sql_conn_id=sql_connid_gomedisys)
    
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['actionRecordedDate']
    for col in cols_dates:
        df[col] = df[col].astype(str)

    print(df.columns)
    print(df.dtypes)
    print(df)

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)


def execute_Sql():
     query = f"""
     delete from TmpEventosDiagnosticosDetalles where actionRecordedDate >='{last_week}' AND actionRecordedDate<'{now}'
     """
     hook = MsSqlHook(sql_connid)
     hook.run(query)


# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}

with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= "10 8 * * *",
    max_active_runs=1
    ) as dag:

   

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    
    extract_events_diagnostics_detail= PythonOperator(
                                     task_id = "extract_events_diagnostics_detail",
                                     python_callable = execute_Sql,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    get_events_diagnostics_detail= PythonOperator(
                                     task_id = "get_events_diagnostics_detail",
                                     python_callable = func_get_events_diagnostics_detail,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_events_diagnostics_detail = MsSqlOperator(task_id='load_fact_events_diagnostics_detail',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql="EXECUTE uspCarga_TblHEventosDiagnosticosDetalles",
                                            email_on_failure=True, 
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                            )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

#start_task >> get_examen_detail >> load_fact_examen_detail >> task_end
start_task >> extract_events_diagnostics_detail >>get_events_diagnostics_detail>> load_fact_events_diagnostics_detail>>task_end
