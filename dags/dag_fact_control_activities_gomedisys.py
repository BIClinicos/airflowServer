# Función de transformación de los archivos xlsx
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
import os
import xlrd
import re
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
import numpy as np
from variables import sql_connid,sql_connid_gomedisys 
from utils import sql_2_df, load_df_to_sql, remove_accents_cols, remove_special_chars, regular_snake_case, last_day_of_month

### Se nombran las variables a utilizar en el dag - Cambiarles el nombre

db_table = "fact_control_activities_gomedisys"
db_tmp_table = "tmp_fact_control_activities_gomedisys"
dag_name = 'dag_' + db_table

### Parametros de tiempo
# Eliminado el end date para produccion, 2023-01-02
# Obtencion dinamica
start_date = date.today() - timedelta(days=1)
# Carge historico
# start_date = date(2022, 7, 1) 
# Formato
start_date = start_date.strftime('%Y-%m-%d')

#######
#### Funciones del script
#######

### Metodo para lectura del script

def query_appointments_bogota(start):
	"""Consulta de agendamiento de citas para especializada Bogota

	Parametros: 
	start (date): Fecha inicial del periodo de consulta
	end (date): Fecha final del periodo de consulta

	Retorna:
	data frame con la informacion de agendamiento especializada durante el periodo seleccionado. Nombre de columnas normalizados
	"""
	query = f"""
		DECLARE 
			@idUserCompany INT = 1,
			@idExam VARCHAR(MAX) = (SELECT STRING_AGG(idAppointmentExam,',') FROM appointmentExams),
			@state VARCHAR(MAX) = 'A,C,I,M,N,S,T'

		SELECT
			appointment.idAppointment AS id_appointment, 
			TI.code AS [Tipo Documento],
			ISNULL(Patient.documentNumber, '') AS [Documento],
			practitioner.documentNumber AS [Documento Profesional],
			AppSd.dateAppointment AS [Fecha Cita],
			LOWER(CE.name) AS [Examen],
			CASE
				WHEN ACTN.name like '%Historia Clínica de %' THEN REPLACE(LOWER(ACTN.name), 'historia clínica de ', '')
				WHEN ACTN.name in ('Crónicos especializada', 'Hoja Ingreso Domiciliario', 'Medicina General Especializada') OR 
				CE.name like 'REFORMULA%' THEN 'medicina general'
				WHEN ACTN.name in ('Anticoagulación', 'Historia Clínica Conciliación Farmacoterapéutica') OR ACTN.name like '%Toxicolo%' 
				THEN 'toxicología'
				WHEN (ACTN.name like '%Orden m%' OR ACTN.name like '%solicitudes  a far%' OR ACTN.name like '%nota de enfr%' OR
				ACTN.name like '%incapacidad%' OR ACTN.name like '%Certificado%') THEN LOWER(CE.name)
				WHEN ACTN.name like 'Nota servicio farma%' OR (CE.name like 'Asesor_a farma%') THEN 'químico farmacéutico'
				ELSE LOWER(ACTN.name) 
			END AS [Esquema clinico], --- Este campo es agregado
			(SELECT STRING_AGG(Prod.legalCode,',') FROM appointmentProducts AS AppProd WITH(NOLOCK)
					INNER JOIN products AS Prod WITH(NOLOCK) ON AppProd.idProduct = Prod.idProduct
				WHERE AppProd.idAppointment = appointment.idAppointment) AS [Código CUPS],
			CASE 
				WHEN (EncRecord.idActualMedicalEvent IS NULL) AND (appstate.itemName = 'Asistida') THEN 'No asistida'
				ELSE appstate.itemName
			END AS [Estado Cita],
			CompanyOff.name AS [Sede],
			usCOn.businessName AS [Entidad],
			Con.name AS [Contrato],
			ContP.name AS [Plan]
		FROM appointmentSchedulers AS AppS WITH (NOLOCK)
			INNER JOIN appointmentSchedulerSlots AS AppSd WITH (NOLOCK) ON AppS.idAppointmenScheduler = AppSd.idAppointmentScheduler
			INNER JOIN appointments AS appointment ON appointment.idAppointmentSchedulerSlots = AppSd.idAppointmentSchedulerSlots
			INNER JOIN companyOffices AS CompanyOff WITH (NOLOCK) ON CompanyOff.idOffice = AppS.idOffice
			INNER JOIN users AS practitioner WITH (NOLOCK) ON practitioner.idUser = AppS.idUserProfessional
			INNER JOIN generalInternalLists AS appState WITH(NOLOCK) ON appointment.state = appState.itemValue
				AND appState.groupCode = 'appState'
			INNER JOIN users AS Us WITH (NOLOCK) ON Us.idUser = AppS.idUserRecord
			INNER JOIN users AS Company WITH (NOLOCK) ON Company.idUser = AppS.idUserCompany
			INNER JOIN generalInternalLists AS AppT WITH (NOLOCK) ON appointment.idAppointmentExamType = AppT.idGeneralInternalList
			INNER JOIN appointmentExams CE WITH (NOLOCK) ON appointment.idAppointmentExam = CE.idAppointmentExam
			INNER JOIN users AS Patient WITH (NOLOCK) ON Patient.idUser = appointment.idUserPerson
			INNER JOIN userPeople UP WITH (NOLOCK) ON UP.idUser = Patient.idUser
			INNER JOIN userConfAdministrativeSex AS Gender WITH(NOLOCK) ON UP.idAdministrativeSex = Gender.idAdministrativeSex
			INNER JOIN userConfTypeDocuments AS TI WITH (NOLOCK) ON TI.idTypeDocument = Patient.idDocumentType
			INNER JOIN contracts AS Con WITH (NOLOCK) ON Con.idContract = appointment.idContract
			INNER JOIN contractPlans AS ContP WITH(NOLOCK) ON appointment.idPlan = ContP.idPlan
			INNER JOIN users AS usCOn WITH (NOLOCK) ON usCOn.idUser = Con.idUserContractee
			INNER JOIN healthRegimes AS HealthR WITH(NOLOCK) ON appointment.idHealthRegime = HealthR.idHealthRegime
			LEFT OUTER JOIN encounters AS enc WITH(NOLOCK) ON appointment.idAdmission = enc.idEncounter
			LEFT OUTER JOIN encounterRecords AS EncRecord WITH(NOLOCK) ON enc.idEncounter = EncRecord.idEncounter
			INNER JOIN users AS userRegister WITH (NOLOCK) ON userRegister.idUser = appointment.userRecord
			LEFT OUTER JOIN EHREvents AS evn WITH(NOLOCK) ON evn.idEHREvent = appointment.idEvent
			LEFT OUTER JOIN generalActions AS ACTN WITH (NOLOCK) ON evn.idAction = ACTN.idAction --- Agregado
		WHERE AppS.idUserCompany = @idUserCompany
			AND CE.idAppointmentExam IN (SELECT Value FROM STRING_SPLIT(@idExam,','))
			AND appState.itemValue IN (SELECT Value FROM STRING_SPLIT(@state,','))
			AND CONVERT(DATE,AppSd.dateAppointment) >= '{start}'
			AND (CompanyOff.name LIKE '%Américas%' OR CompanyOff.name LIKE '%Calle 98' OR CompanyOff.name LIKE '%Calle 49%')
		ORDER BY AppSd.dateAppointment
	"""
	# Lectura de data frame
	df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
	print("Como está leyendo el dataframe inicialmente",df)
	print("Nombres y tipos de columnas leídos del dataframe sin transformar",df.dtypes)
	# Normalizacion de nombres
	df.columns = remove_accents_cols(df.columns)
	df.columns = remove_special_chars(df.columns)
	df.columns = regular_snake_case(df.columns)
	print("Nombres y tipos de columnas leídos del dataframe transformados",df.dtypes)
	# Seleccion de columnas
	df = df[['id_appointment','tipo_documento', 'documento', 'documento_profesional', 'fecha_cita', 'examen', 'esquema_clinico', 
			'codigo_cups', 'estado_cita', 'sede', 'entidad', 'contrato', 'plan']]
	# Renombrar columnas
	col_dict = {'id_appointment':'id_appointment', 'tipo_documento':'document_type', 'documento':'document_number', 
				'documento_profesional':'professional_document_number', 'fecha_cita':'appointment_date', 
				'examen':'exam', 'esquema_clinico':'exam_alter', 'codigo_cups':'cups', 
				'estado_cita':'appointment_status', 'sede':'headquarter', 'entidad':'entity', 
				'contrato':'contract', 'plan':'plan'}
	df.rename(col_dict, axis='columns', inplace=True)
	# Retorno del data frame 
	return df

### Metodo de transformacion

def transform_activities(df):
	"""Transformacion de las actividades al requerimiento de la unidad

	Parametros: 
	df: Data frame a transformar

	Retorna:
	data frame con los datos tratados para el consumo del tablero
	"""
	# Formato de fechas - Usando hora con minutos
	date_columns = ['appointment_date']
	for i in date_columns:
		df[i] = df[i].astype(str)
		#df[i] = df[i].str.strip()
		#df[i] = pd.to_datetime(df[i], format = '%Y-%m-%d %H:%M:%S', errors = 'coerce')
	print(df['appointment_date'])
	# Fusionar examen y esquema_clinico en uno solo
	df['activity'] = df['exam_alter'].fillna(df['exam'])
	# Sustitucion condicional de actividades mal detectadas por exam_alter - 2023-01-03
	df['activity'] = np.where(df['activity'].str.contains(r'^.*(informe final|epicrisis|autorizaciones|regis|histor|nota|rehabilita|cargue|terapia|reporte|asignac|segui| domi).*$', regex = True), df['exam'], df['activity']) 
	# Remocion de acentos
	df['activity'] = df['activity'].str.replace('ñ','ni').str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
	# Sustituciones
	df['activity'] = df['activity'].str.replace(r'^(procedimiento de|procedimientos)','procedimiento', case = False, regex=True)
	df['activity'] = df['activity'].str.replace(r'^(programa de|medicina general programa)','programa', case = False, regex=True)
	df['activity'] = df['activity'].str.replace(r'(telemedicina|asistente|pac|\.+|adulto|presencial|\(domiciliaria\)|especializada|programa|compensar|evento|nueva eps|capital|famisanar|primera vez|control|epilepsia|presenc)','', case = False, regex=True)
	df['activity'] = df['activity'].replace(r'\s+', ' ', regex=True)	
	df['activity'] = df['activity'].replace(r'^.*(?:sahos)$', 'clinica sahos', regex=True)
	df['activity'] = df['activity'].replace(r'cronicos', 'medicina general', regex=True)      
	df['activity'] = df['activity'].str.replace(r'(anticoagulacion)','toxicologia', case = False, regex=True)
	df['activity'] = df['activity'].str.replace(r'^neurologia(?:(?!pediatrica).)*$','neurologia', case = False, regex=True)
	df['activity'] = df['activity'].str.replace(r'^neurologia pediatrica.*$','neurologia pediatrica', case = False, regex=True)
	df['activity'] = df['activity'].str.replace(r'^asesoria far.*','quimico farmaceutico', case = False, regex=True)
	df['activity'] = df['activity'].str.replace(r'^(apli |administracion|aplicacion).*$','aplicacion medicamentos', case = False, regex=True)	
	df['activity'] = df['activity'].str.replace(r'^asignacion.*$','asginacion de actividades', case = False, regex=True)	
	df['activity'] = df['activity'].str.replace(r'^(?:(?!procedimiento).*reumatologia.*)*$','reumatologia', case = False, regex=True)
	df['activity'] = df['activity'].str.strip()
	# Eliminar columnas en desuso
	df.drop(['exam', 'exam_alter'], inplace=True, axis=1)
	# Retorno del data frame
	return df

### Metodo de filtro para servicios excluidos - No usar en version de produccion 2023-01-02
def filter_activities(df):
	"""Filtro de actividades

	Parametros: 
	df: Data frame de control de actividades a filtrar

	Retorna:
	data frame con los datos filtrados
	"""
	# Filtros usando activity
	# Quitar juntas, aplicaciones/administracion medicamentos, asignacion, autorizaciones, caminata, enfermeria
	# capilaroscopia, clinica saho, autotitulacion, espirometria, informe, reporte
	df = df[~df['activity'].str.contains(r'(remision|capilaroscopia|reporte|junta|aplicacion|asignacion|autorizaciones|caminata|pdf|saho|autotitulacion|enfermeria|espirometria|informe)', regex = True)]
	# Retorno del data frame
	return df

### Metodo de ETL 1 del DAG

def func_get_fact_control_activities_gomedisys():
	"""Metodo ETL para el DAG #1

	Parametros: 
	Ninguno

	Retorna:
	Retorno vacio
	"""
	df = query_appointments_bogota(start= start_date)
	df = transform_activities(df)
	# df = filter_activities(df)
	# pd.set_option('display.max_rows', None)
	# pd.set_option('display.max_columns', None)
	# pd.set_option('display.width', None)
	# pd.set_option('display.max_colwidth', -1)
	print(df.dtypes)
	print(df.columns)
	if ~df.empty and len(df.columns) >0:
		load_df_to_sql(df, db_tmp_table, sql_connid)

#######
#### Instansacion del DAG
#######

# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}
# Se declara el DAG con sus respectivos parametros
with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecucion diaria xxxx am/pm(Hora servidor)
    schedule_interval= '0 6 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la funcion que sirve para denotar el inicio del DAG a traves de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #check_connection_task = PythonOperator(task_id='check_connection', python_callable=check_connection)

    #Se declara y se llama la funcion encargada de traer y subir los datos a la base de datos a traves del "PythonOperator"
    get_fact_control_activities_gomedisys = PythonOperator(task_id = "get_fact_control_activities_gomedisys",
        python_callable = func_get_fact_control_activities_gomedisys,
        email_on_failure = True, 
        email ='BI@clinicos.com.co',
        dag = dag
        )
    
    # Se declara la funcion encargada de ejecutar el "Stored Procedure"
    load_fact_control_activities_gomedisys = MsSqlOperator(task_id = 'load_fact_control_activities_gomedisys',
                                        mssql_conn_id = sql_connid,
                                        autocommit = True,
                                        sql = "EXECUTE sp_load_fact_control_activities_gomedisys",
                                        email_on_failure = True, 
                                        email = 'BI@clinicos.com.co',
                                        dag = dag
                                       )

    # Se declara la funcion que sirva para denotar la Terminacion del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

#start_task >>check_connection_task >> get_fact_control_activities_gomedisys >> load_fact_control_activities_gomedisys >> task_end
start_task >> get_fact_control_activities_gomedisys >> load_fact_control_activities_gomedisys >> task_end
