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
from utils import sql_2_df, remove_accents_cols, remove_special_chars, regular_snake_case, regular_camel_case, load_df_to_sql
# pd.set_option('display.max_rows', None)
#  Se nombran las variables a utilizar en el dag

db_table = "factActivePatients"
db_tmp_table = "tmp_factActivePatients"
dag_name = 'dag_' + db_table

# Fecha de ejecución del dag
today = date.today()
month = today.month
year = today.year

# Dia 1 del mes anterior
"""
#Este cálculo de fecha dio error para la ejecución del 01/01/2023 DD/MM/YYYY 
#por tanto se creó un nuevo cálculo para el parámetro 
past_month_date = datetime(year,month-1,1)
past_month_date = past_month_date.strftime('%Y-%m-%d') """

last_day = today - timedelta(today.day)
past_month_date = last_day - timedelta(last_day.day -1)
past_month_date = past_month_date.strftime('%Y-%m-%d')

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_factActivePatients ():

    # LECTURA DE DATOS  
    query = f"""
    DECLARE 
	@idUserCompany INT = 1,
	@startDate DATE = '{past_month_date}',
	@hcPlanF VARCHAR(MAX) = 2--(SELECT STRING_AGG(idHCActivity,',') FROM dbo.EHRConfHCActivity)

	SELECT 
		
		Enc.dateStart,
		EVDom.idEHREvent,
		EVDom.idPatient,
		FORMAT(@startDate,'MM','es') AS [month],
		ContP.name AS [PLAN DE BENEFICIOS],
		
		(SELECT TOP 1 DATEPART(MONTH,CONVERT(DATE,EHRAct.valueText)) FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
			INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
				WHERE EV.idPatient = Enc.idUserPatient
					AND EHRAct.idConfigActivity = 25
					AND EHRAct.idElement = 1
				ORDER BY EV.idEHREvent DESC) AS [MES INGRESO],

		(SELECT TOP 1 DATEPART(DAY,CONVERT(DATE,EHRAct.valueText)) FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
			INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
				WHERE EV.idPatient = Enc.idUserPatient
					AND EHRAct.idConfigActivity = 25
					AND EHRAct.idElement = 1
				ORDER BY EV.idEHREvent DESC) AS [DÍA INGRESO],

		(SELECT TOP 1 FORMAT(CONVERT(DATE,EHRAct.valueText),'yyyy-MM-dd') FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
			INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
				WHERE EV.idPatient = Enc.idUserPatient
					AND EHRAct.idConfigActivity = 25
					AND EHRAct.idElement = 1
				ORDER BY EV.idEHREvent DESC) AS [FECHA INGRESO],
		
		DATEPART(YEAR,ISNULL(Enc.dateStartDischarge,Enc.dateDischarge)) AS [AÑO EGRESO],
		DATEPART(MONTH,ISNULL(Enc.dateStartDischarge,Enc.dateDischarge)) AS [MES DEL EGRESO],
		DATEPART(DAY,ISNULL(Enc.dateStartDischarge,Enc.dateDischarge)) AS [DÍA EGRESO],
		FORMAT(ISNULL(Enc.dateStartDischarge,Enc.dateDischarge),'yyyy-MM-dd HH:mm:ss tt') AS [FECHA EGRESO],

		(SELECT STUFF((SELECT DISTINCT ',' + Prod.name FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
			INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
			INNER JOIN dbo.Products AS Prod WITH(NOLOCK )ON EVForm.idProduct = Prod.idProduct
			WHERE EVF.idEncounter = Enc.idEncounter
				AND (Prod.name LIKE '%INTERCONSULTA%'
					OR Prod.name LIKE '%CONSULTA%')
				FOR XML PATH (''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')) AS [ESPECIALIDADES],
					
		IIF(Enc.idDischargeUser <> 1, ISNULL(EncDest.description, (SELECT TOP 1 Dest.name FROM dbo.EHREventMedicalCarePlan AS EVCP WITH(NOLOCK)
				INNER JOIN dbo.EHRConfEventDestination AS Dest WITH(NOLOCK) ON EVCP.idEventDestination = Dest.idEventDestination
			WHERE EVCP.idEHREvent = EVDom.idEHREvent)),
				ISNULL((SELECT TOP 1 Dest.name FROM dbo.EHREventMedicalCarePlan AS EVCP WITH(NOLOCK)
				INNER JOIN dbo.EHRConfEventDestination AS Dest WITH(NOLOCK) ON EVCP.idEventDestination = Dest.idEventDestination
			WHERE EVCP.idEHREvent = EVDom.idEHREvent), EncDest.description)) AS [DESTINO DE EGRESO]
	
	FROM dbo.encounters AS Enc WITH(NOLOCK)
		INNER JOIN dbo.users AS Pat WITH(NOLOCK) ON Enc.idUserPatient = Pat.idUser
	
		INNER JOIN dbo.userConfTypeDocuments AS DocT WITH(NOLOCK) ON Pat.idDocumentType = DocT.idTypeDocument
		INNER JOIN dbo.userPeople AS PatP WITH(NOLOCK) ON Pat.idUser = PatP.idUser
		LEFT OUTER JOIN dbo.generalListDetails AS PatPC WITH(NOLOCK) ON PatP.idClassification = PatPC.idListDetail

		INNER JOIN dbo.userConfAdministrativeSex AS Gender WITH(NOLOCK) ON PatP.idAdministrativeSex = Gender.idAdministrativeSex
		LEFT OUTER JOIN dbo.generalPoliticalDivisions AS Neig WITH(NOLOCK) ON PatP.idHomePoliticalDivisionNeighborhood = Neig.idPoliticalDivision
		LEFT OUTER JOIN dbo.generalPoliticalDivisions AS Mun WITH(NOLOCK) ON PatP.idHomePlacePoliticalDivision = Mun.idPoliticalDivision

		INNER JOIN dbo.encounterRecords AS EncR WITH(NOLOCK) ON Enc.idEncounter = EncR.idEncounter
		INNER JOIN dbo.contractPlans AS ContP WITH(NOLOCK) ON EncR.idPrincipalPlan = ContP.idPlan
		INNER JOIN dbo.users AS Comp WITH(NOLOCK) ON Enc.idUserCompany = Comp.idUser
		LEFT OUTER JOIN dbo.encounterRelatedPeople AS EncRP WITH(NOLOCK) ON Enc.idEncounter = EncRP.idEncounter
			AND (EncRP.isResponsible = 1 OR EncRP.isCompanion = 1 OR EncRP.isContact = 1)
		LEFT OUTER JOIN dbo.generalListDetails AS EncDest WITH(NOLOCK) ON Enc.idDischargeDestination = EncDest.idListDetail
	
		OUTER APPLY
		(
			SELECT *
			FROM
			(
				SELECT Loc.valueLocation,
					TLoc.code
				FROM dbo.userLocations AS Loc WITH(NOLOCK)
					INNER JOIN dbo.userConfTypeLocations AS TLoc WITH(NOLOCK) ON Loc.idUserTypeLocation = TLoc.idUserTypeLocation
				WHERE Loc.idUser = Pat.idUser
			) AS PV
			PIVOT(MAX(PV.valueLocation) FOR PV.code IN ([CEL],[TELF],[EMAIL])) AS PV
		) AS Loc

		INNER JOIN dbo.encounterHC AS EncHC WITH(NOLOCK) ON Enc.idEncounter = EncHC.idEncounter
		INNER JOIN dbo.EHRConfHCActivity AS HCAct WITH(NOLOCK) ON EncHC.idHCActivity = HCAct.idHCActivity

		INNER JOIN dbo.EHREvents AS EVDom WITH(NOLOCK) ON Enc.idUserPatient = EVDom.idPatient
			AND EVDom.idAction = 83
			AND EVDom.idEHREvent = (SELECT MAX(V.idEHREvent) -- LAST EVENT
				FROM dbo.EHREvents AS V
					INNER JOIN dbo.EHREventCustomActivities AS EVCAct WITH(NOLOCK) ON V.idEHREvent = EVCAct.idEvent
						AND EVCAct.idConfigActivity IN (52, 54)
						AND EVCAct.idElement = 1
						AND EVCAct.valueNumeric IN (1759,1760,2006,2007,2009)
				WHERE V.idPatient = Enc.idUserPatient --	Por Paciente
					AND V.actionRecordedDate < DATEADD(DAY,1,EOMONTH(@startDate))
					AND idAction = 83)

		LEFT OUTER JOIN dbo.EHREvents AS EVAssig WITH(NOLOCK) ON Enc.idEncounter = EVAssig.idEncounter
			AND EVAssig.idEHREvent = (SELECT MAX(idEHREvent)
				FROM dbo.EHREvents AS V WITH(NOLOCK)
					LEFT OUTER JOIN dbo.EHREventCUstomActivities AS VAct WITH(NOLOCK) ON V.idEHREvent = VAct.idEvent
						AND Vact.idConfigActivity = 54
						AND Vact.valueNumeric IN (2006,2007,2009)
				WHERE V.idEncounter = Enc.idEncounter
					AND V.idAction = 347
					AND (VAct.idEvent IS NOT NULL OR V.actionRecordedDate < '20200605'))
	WHERE Enc.idUserCompany = @idUserCompany
		AND Enc.idEncounterClass = 1 -- Domiciliario
		AND EncHC.isActive = 1
		AND HCAct.idHCActivity IN (SELECT Value FROM STRING_SPLIT(@hcPlanF,','))
		AND CONVERT(DATE,Enc.dateStart) BETWEEN CONVERT(DATE,@startDate) AND CONVERT(DATE,EOMONTH(@startDate))
		AND (ContP.name LIKE '%POS %' or ContP.name LIKE '%PAC%' or ContP.name LIKE '%ACUEDUCTO%')
    """
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)


    # PROCESAMIENTO
    print(df.columns)
	
    # Format columns
    df.columns = ['dateStart', 'idEHREvent', 'idPatient', 'month', 'planDeBeneficios',
       'mesIngreso', 'diaIngreso', 'fechaIngreso', 'anioEgreso', 'mesDelEgreso',
       'diaEgreso', 'fechaEgreso', 'especialidades', 'destinoDeEgreso']
    
    # print('BEFORE',df)
    df = df[(df['destinoDeEgreso'].str.lower().str.match(r'(^domicilio.*)')==True) | (df['destinoDeEgreso'].str.lower().str.match(r'(^ingres*)')==True)]

    df.drop_duplicates(subset=['dateStart', 'idEHREvent', 'idPatient'],inplace=True)

    # convert date types to string
    cols_dates = ['dateStart','fechaIngreso', 'fechaEgreso']
    for col in cols_dates:
        df[col] =  df[col].astype(str)
    
    # convert to int
    cols_int = ['month','mesIngreso', 'diaIngreso','anioEgreso', 'mesDelEgreso','diaEgreso']
    for col in cols_int:
        df[col] = df[col].astype(str).str.replace('.0','')
        # df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
    # print(df['fechaIngreso'])
    # print(df.columns)
    # print(df.dtypes)

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
    # Se establece la ejecución del dag a las 12:00 am (hora servidor) todos los Jueves
    schedule_interval= '0 7 * * 2',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_factActivePatients_python_task = PythonOperator(task_id = "get_factActivePatients",
        												python_callable = func_get_factActivePatients,
														email_on_failure=True, 
														email='BI@clinicos.com.co',
														dag=dag
														)
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_factActivePatients = MsSqlOperator(task_id='Load_factActivePatients',
											mssql_conn_id=sql_connid,
											autocommit=True,
											sql="EXECUTE sp_load_factActivePatients",
											email_on_failure=True, 
											email='BI@clinicos.com.co',
											dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_factActivePatients_python_task >> load_factActivePatients >> task_end