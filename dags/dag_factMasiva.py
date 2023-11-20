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

now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')

#fecha_texto = '2023-07-31 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-07-01 00:00:00', '%Y-%m-%d %H:%M:%S')

#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

# fechas secundarias
#fecha_texto_secundario = '2023-09-01 00:00:00'
#now_secundario = datetime.strptime(fecha_texto_secundario, '%Y-%m-%d %H:%M:%S')
#
#last_week_secundario = datetime.strptime('2023-08-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#now_secundario = now_secundario.strftime('%Y-%m-%d %H:%M:%S')
#last_week_secundario = last_week_secundario.strftime('%Y-%m-%d %H:%M:%S')

def func_get_factMasiva():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)

    # LECTURA DE DATOS
    query = f"""    
            DECLARE 
                @idUserCompany INT= 1,
                @OfficeFilter VARCHAR(MAX) = '1,17,352666',--(SELECT STRING_AGG(idOffice,',') FROM companyOffices WHERE idUserCompany = 352666),
                @idIns VARCHAR(MAX) = '16,33,285991,20,266465,422816,289134,17,150579,358811,39,88813,4,24,22,25,150571,302708,26,289154,365849,266467,7,28,23,420,32,421',
                @idCont VARCHAR(MAX) = '83,81,79,76,84,77,88,82,78,80,92'

            -- QUERY PARA EL DAG
                SELECT
                    DISTINCT
                    Pat.idUser                                           as idUsuario
                    ,Enc.idEncounter                                     as idIngreso
                    ,EV.idEHREvent                                       as idEventoEHR
                    ,Enc.idOffice                                        as idOficina
                    ,EncR.idPrincipalContractee                          as idNombreAseguradora
                    ,EncR.idPrincipalContract                            as idContratoPrincipal
                    
                    ,EHREvMDiag.idDiagnostic                             as idDiagnostico
                    -- ,EHREvMDiag.isPrincipal                              as esDiagnosticoPrincipal

                    ,Enc.dateRegister                                    as fechaRegistroEvento
                    ,EV.actionRecordedDate                               as fechaRealizacionEventoAlPaciente
                    ,EncHc.dateStart                                     as fechaInicioPlan

                    ,EncHc.idHCActivity                                  as idActividadesHC
                    ,EV.idAction                                         as idTipoEvento
                    
                    ,Doc.code + ' | ' + Doc.name                         as tipoDeIdentificacion
                    ,Pat.documentNumber                                  as numeroIdentificacion
                    ,Enc.identifier                                      as ingreso
                    ,Office.legalCode                                    as codigoHabilitacion
                    ,Ucom.documentNumber                                 as nitIPS
                    ,CAST(RIGHT(Office.legalCode, 1) AS INT)             as codigoSucursal
                    

                    ,CityD.codeConcatenate                               as municipioDeResidencia
                    ,PatU.telecom                                        as numeroTelefonicoNo1DelPaciente
                    ,PatU.phoneHome                                      as numeroTelefonicoNo2DelPaciente
                    ,PatU.homeAddress                                    as direccionDeResidenciaDelPaciente
                    ,EHRconfAct.codeActivity                             as codigoServicioAtencionRequeridaPorUsuario

                FROM Encounters AS Enc
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

                WHERE Enc.idUserCompany = @idUserCompany
                    AND (EncHC.dateStart >='{last_week}' AND EncHC.dateStart<'{now}')
                    AND Enc.idOffice IN (SELECT Value FROM dbo.FnSplit (@OfficeFilter))
                    AND EncR.idPrincipalContractee IN (SELECT Value FROM dbo.FnSplit (@idIns))
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
    schedule_interval= '25 19 * * SAT', # cron expression
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_factMasiva = PythonOperator(task_id = "get_factMasiva",
                                                                python_callable = func_get_factMasiva,
                                                                email_on_failure=True, 
                                                                email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_factMasiva = MsSqlOperator(task_id='Load_factMasiva',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblHMasiva",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )
    


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_factMasiva >> load_factMasiva >> task_end