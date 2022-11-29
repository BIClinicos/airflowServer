import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators import email_operator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from variables import sql_connid,sql_connid_gomedisys #FMG Nombre de las conexiones a DB BI y Gomedisys
from utils import sql_2_df #FMG Importa libreria que permite consultaR sql Y cargarlo en un df


#  Se nombran las variables a utilizar en el dag -FMG Nombre del DAG
dag_name = 'dag_' + 'TEC_PYR_DOMIConsultasReporte'
dirname = '/opt/airflow/dags/generated_files/' #FMG Ruta donde se almacenarán los archivos generados 

#FMGSe calcula el primer dia del mes y tres meses atras 
today = date.today()
last_day = today - timedelta(today.day)
last_date = today - timedelta(today.day -1)
last_month = last_day - timedelta(last_day.day)
middle_date = last_month - timedelta(last_month.day -1)
middle_date_op = last_month - timedelta(last_month.day)
first_date = middle_date_op - timedelta(middle_date_op.day -1)

# Parametros para ejecucion de la consulta SQL - Mes y anio del mes a reportar
year = last_day.year
month = last_day.month
month_1 = middle_date.month
month_2 = first_date.month

# Nombre estandar de reportes generados (.csv,.xlsx)
filename = f'Reporte_Domi_Consultas_{year}_{month}_{month_1}_{month_2}.csv'
filename2 = f'Reporte_Domi_Consultas_{year}_{month}_{month_1}_{month_2}.xlsx'

# Función de generacion de reporte y envío via email.
def func_get_TEC_PYR_DOMIConsultas ():

    print('Fecha inicio ', first_date)
    print('Fecha fin ', last_date)
    
    domiConsultas_query = f"""
        (SELECT DISTINCT 
        ENC.identifier 									AS Ingreso, 
        FORMAT(ENC.dateStart,'dd/MM/yyyy HH:mm') 		AS FechaIngreso,
        FORMAT(EV.actionRecordedDate,'dd/MM/yyyy HH:mm') AS FechaActividad,
        USRC.code 										AS TipoDocumentoProfesional,
        USR.documentNumber 								AS DocumentoProfesional,
        CONCAT(USR.givenName,' ',USR.familyName) 		AS NombreProfesional,
        GS.name 										AS Especialidad,
        USRC2.code 										AS TipoDocumentoPaciente,
        USR2.documentNumber 							AS DocumentoPaciente,
        CONCAT(USR2.givenName,' ',USR2.familyName) 		AS NombrePaciente,
        EVMD.presentIllness 							AS EnfermedadActual,
        EVMCP.carePlan 									AS PlanTratamiento,
        ISNULL(ISNULL(EVCA.valueText,EVMC.medicalConcept),EVSW.socialDiagnosis) AS Analisis, --Cuando el análisis no está en la tabla EHREventCustomActivities, se busca en la tabla EHREventMedicalConcept y luego en EHREventSocialWork
        EVCA2.valueText 								AS TipoConsulta,
        ENCC.name 										AS TipoIngreso,
        CONT.name 										AS Contrato
    FROM     
        dbo.EHREvents AS EV
        INNER JOIN dbo.encounters AS ENC ON EV.idEncounter = ENC.idEncounter						--Ingreso
        INNER JOIN dbo.users AS USR ON EV.idPractitioner = USR.idUser								--DocumentoProfesional
        INNER JOIN dbo.userConfTypeDocuments AS USRC ON USRC.idTypeDocument = USR.idDocumentType	--TipoDocumentoProfesional
        INNER JOIN dbo.generalSpecialties AS GS ON EV.idSpeciality = GS.idSpecialty					--Especialidad 
        INNER JOIN dbo.users AS USR2 ON EV.idPatient = USR2.idUser									--DocumentoPaciente
        INNER JOIN dbo.userConfTypeDocuments AS USRC2 ON USRC2.idTypeDocument = USR2.idDocumentType	--TipoDocumentoPaciente
        INNER JOIN dbo.encounterConfClass AS ENCC ON ENC.idEncounterClass = ENCC.idEncounterClass	--TipoIngreso
        INNER JOIN dbo.encounterRecords AS ENCR ON ENC.idEncounter = ENCR.idEncounter				--Contrato
        INNER JOIN dbo.contracts AS CONT ON ENCR.idPrincipalContract = CONT.idContract				--Contrato
        INNER JOIN dbo.EHREventMedicalDescription AS EVMD ON EV.idEHREvent = EVMD.idEHREvent		--EnfermedadActual
        LEFT OUTER JOIN dbo.EHREventMedicalCarePlan AS EVMCP ON EV.idEHREvent = EVMCP.idEHREvent	--PlanTratamiento
        LEFT OUTER JOIN dbo.EHREventMedicalConcept AS EVMC ON EV.idEHREvent = EVMC.idEHREvent		--Analisis
        LEFT OUTER JOIN dbo.EHREventCustomActivities AS EVCA ON EV.idEHREvent = EVCA.idEvent		--Analisis
            AND EVCA.idConfigActivity IN(70,53,146,110)												
            AND EVCA.idElement=1
        LEFT OUTER JOIN dbo.EHREventSocialWork AS EVSW ON EV.idEHREvent = EVSW.idEHREvent		--Analisis
        LEFT OUTER JOIN dbo.EHREventCustomActivities AS EVCA2 ON EV.idEHREvent = EVCA2.idEvent		--TipoConsulta
            AND EVCA2.idConfigActivity IN (52,54)
            AND EVCA2.idElement=1
    WHERE 
        (GS.name like '%Medicina General%' OR GS.name like '%Psiquiatr_a%' 
        OR GS.name like '%Geriatr_a%'OR GS.name like '%Ortopedia%' OR GS.name like '%Cuidados%Pal_ativos%' OR GS.name like '%Nutrici_n%'
        OR GS.name like '%Neurolog_a%'OR GS.name like '%Neumolog_a%' OR GS.name like '%Fisiatr_a%'
        OR GS.name like '%Cardiolog_a%' OR GS.name like '%Medicina Interna%' OR GS.name like '%Medicina Familiar%' OR GS.name like '%Pediatr_a%' 
        OR GS.name like '%Trabajo%Social%') --Especialidades manejadas en el contrato
        AND ENCR.idPrincipalContract=57 --Código del contrato de Compensar-Domiciliaria
        --AND YEAR(ENC.dateStart) = '{year}' AND MONTH(ENC.dateStart) = '{month}' --Año y mes del ingreso
        AND ENC.dateStart >= '{first_date}' AND ENC.dateStart < '{last_date}'
    )   
    UNION ALL
    (   
    SELECT DISTINCT
        ENC.identifier 									AS Ingreso,
        FORMAT(ENC.dateStart,'dd/MM/yyyy HH:mm') 		AS FechaIngreso,
        FORMAT(EV.actionRecordedDate,'dd/MM/yyyy HH:mm') AS FechaActividad,
        USRC.code 										AS TipoDocumentoProfesional,
        USR.documentNumber 								AS DocumentoProfesional,
        CONCAT(USR.givenName,' ',USR.familyName) 		AS NombreProfesional,
        GS.name 										AS Especialidad,
        USRC2.code 										AS TipoDocumentoPaciente,
        USR2.documentNumber 							AS DocumentoPaciente,
        CONCAT(USR2.givenName,' ',USR2.familyName) 		AS NombrePaciente,
        EVMD.presentIllness 							AS EnfermedadActual,
        EVMCP.carePlan 									AS PlanTratamiento,
        EVCA.valueText 									AS Analisis, --Cuando el análisis no está en la tabla EHREventCustomActivities, se busca en la tabla EHREventMedicalConcept y luego en EHREventSocialWork
        EVCA2.valueText 								AS TipoConsulta,
        ENCC.name 										AS TipoIngreso,
        CONT.name 										AS Contrato
    FROM     
        dbo.EHREvents AS EV
        INNER JOIN dbo.encounters AS ENC ON EV.idEncounter = ENC.idEncounter						--Ingreso
        INNER JOIN dbo.users AS USR ON EV.idPractitioner = USR.idUser								--DocumentoProfesional
        INNER JOIN dbo.userConfTypeDocuments AS USRC ON USRC.idTypeDocument = USR.idDocumentType	--TipoDocumentoProfesional
        INNER JOIN dbo.generalSpecialties AS GS ON EV.idSpeciality = GS.idSpecialty					--Especialidad 
        INNER JOIN dbo.users AS USR2 ON EV.idPatient = USR2.idUser									--DocumentoPaciente
        INNER JOIN dbo.userConfTypeDocuments AS USRC2 ON USRC2.idTypeDocument = USR2.idDocumentType	--TipoDocumentoPaciente
        INNER JOIN dbo.encounterConfClass AS ENCC ON ENC.idEncounterClass = ENCC.idEncounterClass	--TipoIngreso
        INNER JOIN dbo.encounterRecords AS ENCR ON ENC.idEncounter = ENCR.idEncounter				--Contrato
        INNER JOIN dbo.contracts AS CONT ON ENCR.idPrincipalContract = CONT.idContract				--Contrato
        INNER JOIN dbo.EHREventMedicalDescription AS EVMD ON EV.idEHREvent = EVMD.idEHREvent		--EnfermedadActual
        LEFT OUTER JOIN dbo.EHREventMedicalCarePlan AS EVMCP ON EV.idEHREvent = EVMCP.idEHREvent	--PlanTratamiento
        LEFT OUTER JOIN dbo.EHREventCustomActivities AS EVCA ON EV.idEHREvent = EVCA.idEvent		--Analisis
            AND EVCA.idConfigActivity = 147															--Se agrega 147 (20220422)
            AND EVCA.idElement=1
        LEFT OUTER JOIN dbo.EHREventCustomActivities AS EVCA2 ON EV.idEHREvent = EVCA2.idEvent		--TipoConsulta
            AND EVCA2.idConfigActivity IN (52,54)
            AND EVCA2.idElement=1
    
    WHERE 
        (GS.name like '%Psicolog_a%') --Especialidades manejadas en el contrato
        AND ENCR.idPrincipalContract=57 --Código del contrato de Compensar-Domiciliaria
        --AND YEAR(ENC.dateStart) = '{year}' AND MONTH(ENC.dateStart) = '{month}') --Año y mes del ingreso)   
        AND ENC.dateStart >= '{first_date}' AND ENC.dateStart < '{last_date}')
    ORDER BY DocumentoPaciente,FORMAT(EV.actionRecordedDate,'dd/MM/yyyy HH:mm')
    """
    # Dispensacion y formulacion debe ser maximo hasta el último día del mes
    df = sql_2_df(domiConsultas_query, sql_conn_id=sql_connid_gomedisys)
    
    print(df.columns)
    print(df.dtypes)
    print(df)

    df.to_csv(dirname+filename, index=False)
    #df.to_excel(dirname+filename2, index=False)

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
    # Se establece el cargue de los datos el día 3 de cada mes.
    #schedule_interval= '0 0 1 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    # Se declara y se llama la función encargada de generar el reporte
    get_TEC_PYR_DOMIConsultas_python_task = PythonOperator(task_id = "get_TEC_PYR_DOMIConsultas",
        python_callable = func_get_TEC_PYR_DOMIConsultas)
    
    # Se declara la función encargada de enviar por correo los reportes generados
    email_summary = email_operator.EmailOperator(
        task_id='email_summary',
        to=['dcardenas@clinicos.com.co','fmgutierrez@clinicos.com.co','nrosas@clinicos.com.co'],
        subject=f'PRUEBA - Reporte automático Domi Consultas {year}-{month}-{month_1}-{month_2}',
        html_content="""<p>Saludos, envio reporte de gomedisys de DOMI Consultas
        (mail creado automaticamente).</p>
        <br/>
        """,
        files=[f'{dirname}{filename}']#,f'{dirname}{filename2}']
        )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_TEC_PYR_DOMIConsultas_python_task >> email_summary >> task_end