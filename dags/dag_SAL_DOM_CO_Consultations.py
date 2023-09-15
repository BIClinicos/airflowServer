from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime, timedelta
from variables import sql_connid,sql_connid_gomedisys 
from utils import sql_2_df, load_df_to_sql 


#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'tmp_SAL_DOM_CO_Consultations'
db_table = "SAL_DOM_CO_Consultations"
dag_name = 'dag_' + db_table

#Se halla las fechas de cargue de la data 
now = datetime.now()
last_week = now - timedelta(weeks=1)
# last_week = datetime(2023,6,1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')
###### Mod problema mipres
#now=datetime.strptime('2022-08-31 23:59:00', '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2022-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')


#year = last_week.year
#month = last_week.month

def func_get_TEC_PYR_DOMIConsultas ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)
    
    domiConsultas_query = f"""
    DECLARE

    @idContract VARCHAR(MAX) = '57,76,77,92,101'
        (SELECT DISTINCT 
        ENC.identifier 									AS Ingreso, 
        --FORMAT(ENC.dateStart,'dd/MM/yyyy HH:mm') 		AS FechaIngreso,
        --FORMAT(EV.actionRecordedDate,'dd/MM/yyyy HH:mm') AS FechaActividad,
        ENC.dateStart                                   AS FechaIngreso,
        EV.actionRecordedDate                           AS FechaActividad,
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
        AND ENCR.idPrincipalContract IN (SELECT * FROM STRING_SPLIT(@idContract,',')) --Código del contrato de Compensar-Domiciliaria y Nueva EPS
        
        AND EV.actionRecordedDate >='{last_week}' AND EV.actionRecordedDate<'{now}'
        --AND ENC.dateStart >= '2023-02-01 00:00:00' AND ENC.dateStart < '2023-03-01 00:00:00'
    )   
    UNION ALL
    (   
    SELECT DISTINCT
        ENC.identifier 									AS Ingreso,
        --FORMAT(ENC.dateStart,'dd/MM/yyyy HH:mm') 		AS FechaIngreso,
        --FORMAT(EV.actionRecordedDate,'dd/MM/yyyy HH:mm') AS FechaActividad,
        ENC.dateStart                                   AS FechaIngreso,
        EV.actionRecordedDate                           AS FechaActividad,
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
        AND ENCR.idPrincipalContract IN (SELECT * FROM STRING_SPLIT(@idContract,',')) --Código del contrato de Compensar-Domiciliaria y Nueva EPS
        
        AND EV.actionRecordedDate >='{last_week}' AND EV.actionRecordedDate<'{now}')
        --AND ENC.dateStart >= '2023-02-01 00:00:00' AND ENC.dateStart < '2023-03-01 00:00:00')
    --ORDER BY DocumentoPaciente,FORMAT(EV.actionRecordedDate,'dd/MM/yyyy HH:mm')
    ORDER BY DocumentoPaciente, EV.actionRecordedDate
    """
    # Ejecutar la consulta capturandola en un dataframe
    df = sql_2_df(domiConsultas_query, sql_conn_id=sql_connid_gomedisys)
    
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['FechaIngreso','FechaActividad']
    for col in cols_dates:
        df[col] = df[col].astype(str)

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

with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '40 5 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_TEC_PYR_GEFDispensacion_python_task = PythonOperator(
                                                            task_id = "get_TEC_PYR_GEFDispensacion",
                                                            python_callable = func_get_TEC_PYR_DOMIConsultas,
                                                            email_on_failure=True, 
                                                            email='BI@clinicos.com.co',
                                                            dag=dag
                                                            )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_TEC_PYR_GEFDispensacion = MsSqlOperator(task_id='Load_TEC_PYR_GEFDispensacion',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE sp_load_SAL_DOM_CO_Consultations",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_TEC_PYR_GEFDispensacion_python_task >> load_TEC_PYR_GEFDispensacion >> task_end
#start_task >> get_TEC_PYR_GEFDispensacion_python_task >> task_end