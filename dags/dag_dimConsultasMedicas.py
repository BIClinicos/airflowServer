"""
Proyecto: Masiva NEPS

author dag: Luis Esteban Santamaría. Ingeniero de Datos.
Fecha creación: 29/08/2023

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

db_table = "TblDConsultasMedicas"
db_tmp_table = "TmpConsultasMedicas"
dag_name = 'dag_' + db_table


# Para correr manualmente las fechas
fecha_texto = '2023-01-31 00:00:00'
now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

now = now.strftime('%Y-%m-%d %H:%M:%S')
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')



def func_get_dimConsultasMedicas ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)

    query_legacy = f"""
        SELECT
            Todo.idEventoEHR,
            Todo.idIngreso,
            Todo.idUsuarioPaciente,
            Todo.idEscala,
            Todo.idPregunta,
            Todo.idRespuesta,
            Todo.fechaEvento,
            Todo.aplicaParaHombres,
            Todo.aplicaParaMujeres,
            Todo.descripcionRespuesta,
            Todo.valorAnalizado,
            Todo.idResultadoEvaluacion,
            Todo.idRegistro,
            Todo.interpretacionEscala
        FROM (
            SELECT DISTINCT
                EventMSC.idEHREvent as idEventoEHR,
                Enc.idEncounter as idIngreso,
                Enc.idUserPatient as idUsuarioPaciente,

                EventMSC.idScale as idEscala,
                EventMSC.idQuestion as idPregunta,
                EventMSC.idAnswer as idRespuesta,
                ConfSQA.isForMale as aplicaParaHombres,
                ConfSQA.isForFemale as aplicaParaMujeres,
                ConfSQA.description as descripcionRespuesta,
                ConfSQA.value as valorAnalizado,

                EventMS.idEvaluation as idResultadoEvaluacion,
                ConfgSV.idRecord as idRegistro,
                ConfgSV.name as interpretacionEscala,

                Ev.actionRecordedDate as fechaEvento,

                ROW_NUMBER() over( partition by EventMSC.idEHREvent,
                                                Enc.idEncounter,
                                                Enc.idUserPatient,
                                                EventMSC.idScale,
                                                EventMSC.idQuestion,
                                                EventMSC.idAnswer,
                                                EventMS.idEvaluation,
                                                ConfgSV.idRecord ORDER BY Ev.actionRecordedDate DESC) as Indicador
            FROM EHREventMedicalScaleQuestions AS EventMSC WITH(NOLOCK)
            INNER JOIN EHRConfScaleQuestionAnswers AS ConfSQA WITH(NOLOCK)
                ON EventMSC.idScale = ConfSQA.idScale
                    AND ConfSQA.idQuestion = EventMSC.idQuestion
                    AND ConfSQA.idAnswer = EventMSC.idAnswer
            INNER JOIN EHREventMedicalScales AS EventMS WITH(NOLOCK) 
                ON EventMSC.idEHREvent = EventMS.idEHREvent
                    AND EventMSC.idScale = EventMS.idScale
            INNER JOIN EHRConfScaleValorations AS ConfgSV WITH(NOLOCK) 
                ON EventMS.idScale = ConfgSV.idScale
                    AND ConfgSV.idRecord = EventMS.idEvaluation
            INNER JOIN EHREvents AS Ev WITH(NOLOCK) 
                ON EventMSC.idEHREvent = Ev.idEHREvent
            INNER JOIN Encounters AS Enc WITH(NOLOCK) 
                ON Ev.idEncounter = Enc.idEncounter
            WHERE Ev.actionRecordedDate >='{last_week}' AND Ev.actionRecordedDate<'{now}') AS Todo
            WHERE Indicador=1
    """

    query = f"""
    SELECT
        CM.idEventoEHR,
        CM.idIngreso,
        CM.idUsuarioPaciente,
        CM.idEscala,
        CM.idPregunta,
        CM.idRespuesta,
        CM.idResultadoEvaluacion,
        CM.idRegistro,
        CM.aplicaParaHombres,
        CM.aplicaParaMujeres,
        CM.descripcionRespuesta,
        CM.valorAnalizado,
        CM.interpretacionEscala,
        CM.fechaEvento
    FROM (
        SELECT DISTINCT
            EventMSQ.idEHREvent as idEventoEHR,
            Enc.idEncounter as idIngreso,
            Enc.idUserPatient as idUsuarioPaciente,

            EventMSQ.idScale as idEscala,
            EventMSQ.idQuestion as idPregunta,
            EventMSQ.idAnswer as idRespuesta,
            ConfSQA.isForMale as aplicaParaHombres,
            ConfSQA.isForFemale as aplicaParaMujeres,
            ConfSQA.description as descripcionRespuesta,
            CAST(ConfSQA.value AS INT) as valorAnalizado,
            EventMS.idEvaluation as idResultadoEvaluacion,
            ConfgSV.idRecord as idRegistro,
            ConfgSV.name as interpretacionEscala,
            Ev.actionRecordedDate as fechaEvento

        FROM EHREventMedicalScaleQuestions AS EventMSQ WITH(NOLOCK)
        INNER JOIN EHRConfScaleQuestionAnswers AS ConfSQA WITH(NOLOCK) ON EventMSQ.idScale = ConfSQA.idScale AND ConfSQA.idQuestion = EventMSQ.idQuestion AND ConfSQA.idAnswer = EventMSQ.idAnswer
        INNER JOIN EHREventMedicalScales AS EventMS WITH(NOLOCK) ON EventMSQ.idEHREvent = EventMS.idEHREvent AND EventMSQ.idScale = EventMS.idScale
        INNER JOIN EHRConfScaleValorations AS ConfgSV WITH(NOLOCK) ON EventMS.idScale = ConfgSV.idScale AND ConfgSV.idRecord = EventMS.idEvaluation
        INNER JOIN EHREvents AS Ev WITH(NOLOCK) ON EventMSQ.idEHREvent = Ev.idEHREvent
        INNER JOIN Encounters AS Enc WITH(NOLOCK) ON Ev.idEncounter = Enc.idEncounter
        WHERE Eve.actionRecordedDate >='{last_week}' AND Eve.actionRecordedDate<'{now}') AS CM

    """

    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    # conversión de campos
    df['fechaEvento'] = df['fechaEvento'].astype(str)
    df['valorAnalizado'] = df['valorAnalizado'].astype(int)

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
    get_dimConsultasMedicas = PythonOperator(task_id = "get_dimConsultasMedicas",
                                                                python_callable = func_get_dimConsultasMedicas,
                                                                #email_on_failure=False, 
                                                                # email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dimConsultasMedicas = MsSqlOperator(task_id='Load_dimConsultasMedicas',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDConsultasMedicas",
                                        # email_on_failure=True, 
                                        # email='BI@clinicos.com.co',
                                        dag=dag
                                       )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dimConsultasMedicas >> load_dimConsultasMedicas >> task_end
