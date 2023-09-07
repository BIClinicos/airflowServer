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
db_table = "TblHMasiva"
db_tmp_table = "TmpMasiva"
dag_name = 'dag_' + db_table

# Para correr manualmente las fechas
fecha_texto = '2023-06-31 00:00:00'
now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
last_week=datetime.strptime('2023-06-01 00:00:00', '%Y-%m-%d %H:%M:%S')

now = now.strftime('%Y-%m-%d %H:%M:%S')
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

def func_get_factMasiva():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)

    # LECTURA DE DATOS
    query = f"""    
            DECLARE -- 3 meses.
                @idUserCompany INT= 1,
                -- @dateStart DATETIME = '2023/01/01', -- '2023/06/01',
                -- @dateEnd DATETIME = '2023/01/31', -- '2023/08/07',
                @OfficeFilter VARCHAR(MAX) = '1,17',--(SELECT STRING_AGG(idOffice,',') FROM companyOffices WHERE idUserCompany = 352666),
                @idIns VARCHAR(MAX) = '16,33,285991,20,266465,422816,289134,17,150579,358811,39,88813,4,24,22,25,150571,302708,26,289154,365849,266467,7,28,23,420,32,421',
                @idCont VARCHAR(MAX) = '83,81,79,76,84,77,88,82,78,80,92'


            SELECT
                Todo.idIngreso,
                Todo.idUsuario,
                Todo.idUsuarioPaciente,
                Todo.idEventoEHR,
                Todo.idOficina,
                Todo.idNombreAseguradora,
                Todo.idContratoPrincipal,
                Todo.idDiagnostico,
                Todo.idEsquemaActividad,
                Todo.idElementoAGuardar,
                Todo.idSignoVital,
                Todo.idRegistro,
                Todo.idEventoAEvaluar,
                Todo.idEscala,
                Todo.idPregunta,

                Todo.fechaRegistroEvento,
                Todo.fechaRealizacionEventoAlPaciente,
                Todo.fechaInicioPlan,

                Todo.ingreso,
                Todo.nitIPS,
                Todo.codigoHabilitacion,
                Todo.codigoSucursal
            FROM (
                SELECT
                    DISTINCT
                    Enc.idEncounter as idIngreso, 
                    Pat.idUser as idUsuario,
                    Enc.idUserPatient as idUsuarioPaciente, -- Conecta con Dim Users
                    EV.idEHREvent as idEventoEHR,
                    Enc.idOffice as idOficina, -- Conecta con TblDOficina
                    EncR.idPrincipalContractee as idNombreAseguradora,
                    EncR.idPrincipalContract as idContratoPrincipal,
                    Diag.idDiagnostic as idDiagnostico, -- Conecta con Dimension Diagnostics
                    -- TheDate -- Es con el delta 
                    EHREvCust.idConfigActivity as idEsquemaActividad,
                    EHREvCust.idElement as idElementoAGuardar,
                    EHRPaMe.idMeasurement as idSignoVital,
                    EHRPaMe.idRecord as idRegistro,
                    EvICU.idMedition as idEventoAEvaluar,
                    EvICU.value as valorARegistrarDeMonitoria,
                    EventMSQ.idScale as idEscala,
                    EventMSQ.idQuestion as idPregunta,
                    Enc.dateRegister as fechaRegistroEvento,
                    EV.actionRecordedDate as fechaRealizacionEventoAlPaciente,
                    EncHc.dateStart as fechaInicioPlan, -- Campo Delta.
                    Enc.identifier as ingreso,
                    Ucom.documentNumber as nitIPS,
                    Office.legalCode as codigoHabilitacion,
                    RIGHT(Office.legalCode, 1) as codigoSucursal
                FROM Encounters Enc
                INNER JOIN users AS Pat WITH(NOLOCK) ON Enc.idUserPatient = Pat.idUser
                INNER JOIN userConfTypeDocuments AS Doc WITH(NOLOCK) ON Pat.idDocumentType = Doc.idTypeDocument
                INNER JOIN companyOffices AS Office WITH(NOLOCK) ON Enc.idOffice = Office.idOffice
                INNER JOIN userPeople AS PatU WITH(NOLOCK) ON Pat.idUser = PatU.idUser
                INNER JOIN users AS Ucom WITH(NOLOCK) ON Office.idUserCompany = Ucom.idUser
                INNER JOIN generalPoliticalDivisions AS City WITH(NOLOCK) ON PatU.idHomePlacePoliticalDivision = City.idPoliticalDivision
                INNER JOIN generalPoliticalDivisions AS CityD WITH(NOLOCK) ON City.idParent = CityD.idPoliticalDivision
                INNER JOIN encounterHC AS EncHc WITH(NOLOCK) ON Enc.idEncounter = EncHc.idEncounter
                INNER JOIN ehrconfhcActivity AS EHRconfAct WITH(NOLOCK) ON EncHc.idHCActivity = EHRconfAct.idHCActivity AND EHRconfAct.idCompany = @idUserCompany
                INNER JOIN encounterRecords AS EncR WITH(NOLOCK) ON Enc.idEncounter = EncR.idEncounter
                INNER JOIN EHREvents AS EV WITH(NOLOCK) ON Enc.idEncounter = EV.idEncounter
                INNER JOIN EHREventMedicalDiagnostics AS EHREvMDiag WITH(NOLOCK) ON EHREvMDiag.idEHREvent = EV.idEHREvent
                INNER JOIN diagnostics AS Diag WITH(NOLOCK) ON EHREvMDiag.idDiagnostic = Diag.idDiagnostic

                -- DIM Esquemas Configurables
                INNER JOIN EHREventCustomActivities AS EHREvCust WITH(NOLOCK) ON EV.idEHREvent = EHREvCust.idEvent
                
                -- DIM Mediciones Signos Vitales
                INNER JOIN EHRPatientMeasurements AS EHRPaMe WITH(NOLOCK) ON Enc.idEncounter = EHRPaMe.idEncounter AND EV.idEHREvent = EHRPaMe.idEHREvent AND Enc.idUserPatient = EHRPaMe.idUserPatient
                
                -- DIM Mediciones de Monitoria
                INNER JOIN EHREventICUMonitoringMeditions EvICU ON EV.idEHREvent = EvICU.idEHREvent
                
                -- DIM Consultas Medicas
                INNER JOIN EHREventMedicalScaleQuestions AS EventMSQ ON EV.idEHREvent = EventMSQ.idEHREvent

                WHERE Enc.idUserCompany = @idUserCompany
                    -- AND EncHC.dateStart BETWEEN @dateStart AND (@dateEnd + '23:59:59')
                    AND EncHC.dateStart >='{last_week}' AND recordedDate<='{now}'
                    AND Enc.idOffice IN (SELECT Value FROM dbo.FnSplit (@OfficeFilter))
                    AND EncR.idPrincipalContractee IN (SELECT Value FROM dbo.FnSplit (@idIns))
                    -- AND EncR.idPrincipalContract IN (SELECT Value FROM dbo.FnSplit (@idCont))
	        ) AS Todo
        """
    
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    # conversión de campos
    df['fechaRegistroEvento'] = df['fechaRegistroEvento'].astype(str)
    df['fechaRealizacionEventoAlPaciente'] = df['fechaRealizacionEventoAlPaciente'].astype(str)
    df['fechaInicioPlan'] = df['fechaInicioPlan'].astype(str)

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
    get_factMasiva = PythonOperator(task_id = "get_factMasiva",
                                                                python_callable = func_get_factMasiva,
                                                                #email_on_failure=False, 
                                                                # email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )
    """
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_factMasiva = MsSqlOperator(task_id='Load_factMasiva',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDEsquemasConfigurables",
                                        # email_on_failure=True, 
                                        # email='BI@clinicos.com.co',
                                        dag=dag
                                       )
    """


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_factMasiva >> task_end