import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from utils import remove_accents_cols, regular_snake_case, remove_special_chars, normalize_str_categorical
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df, remove_accents_cols, remove_special_chars, regular_snake_case, regular_camel_case, load_df_to_sql

#  Se nombran las variables a utilizar en el dag

db_table = "fact_appointments_gomedisys"
db_tmp_table = "tmp_fact_appointments_gomedisys"
dag_name = 'dag_' + db_table

# Fecha de ejecución del dag
today = date.today()
month = today.month
year = today.year

# Dia 1 del mes anterior
#past_month_date = datetime(year,month-1,1)
#past_month_date = past_month_date.strftime('%Y-%m-%d')




def func_get_fact_appointments_gomedisys ():

    now = datetime.now()
    last_week = now - timedelta(days=30)
    last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
    print(now)
    print(last_week)

    # LECTURA DE DATOS  
    query = f""" DECLARE 
	@idUserCompany INT = 1,
	@idUserProfessional VARCHAR(MAX) = '10,100,101,102,103,104,105,106,107,108,10823,109,11086,11476,11494,11529,11621,11637,11653,12196,12222,12299,12309,12408,12478,12709,12719,12729,12787,12820,129,12931,13232,13283,13288,140,141,15353,15619,15621,15675,17818,17861,18981,19007,2,20065,20066,20067,21113,21118,21119,24425,24427,24537,24645,24656,24793,24929,24946,25203,26416,26504,26824,26896,26903,26933,27949,28,29,29008,29081,29126,29156,29157,29210,29340,29502,29509,29781,29823,29875,29881,30060,31094,31128,31304,31533,33,34,35,37,38,39,41,42,45,48,49,50,51,52,53,54,56,57,58,67,68,72,73,74,78,79,80,81,87,88,95,96,9629,9630,9632,9634,9637,9638,9693,9695,9697,9703,99',
	@officeFilter VARCHAR(MAX) = '1,11,12,13,14,15,4,5,8,9',
	@idExam VARCHAR(MAX) = (SELECT STRING_AGG(idAppointmentExam,',') FROM appointmentExams),
	@state VARCHAR(MAX) = 'A,C,I,M,N,S,T',
	@patientName VARCHAR(MAX) = ''


	SELECT 
		TI.code AS [Tipo Documento],
		Patient.idUser AS [id_patient],
		ISNULL(Patient.documentNumber, '') AS [Documento],
		Patient.firstGivenName AS [Primer nombre],
		Patient.secondGiveName AS [Segundo nombre],
		Patient.firstFamilyName AS [Primer apellido],
		Patient.secondFamilyName AS [Segundo apellido],
		CONCAT_WS(' ',Patient.firstGivenName,Patient.secondGiveName,Patient.firstFamilyName,Patient.secondFamilyName ) AS [Nombres y apellidos completos],
		evn.actionRecordedDate AS [Atendido],
		practitioner.idUser AS [id_professional],
		CONCAT_WS(' ',practitioner.firstFamilyName,practitioner.secondFamilyName,practitioner.firstGivenName,practitioner.secondGiveName) AS [Profesional],
		practitioner.documentNumber AS [Documento Profesional],
		Spec.name AS [Especialidad],
		AppSd.dateAppointment AS [Fecha Cita],
		AppT.itemName AS [Tipo Cita],
		(AppS.durationTimeSlots * (SELECT COUNT(*) FROM appointmentSchedulerSlots AS Slot WHERE appointment.idAppointment = Slot.idAppointment)) AS [Tiempo de cita],
		CE.name AS [Examen],
		(SELECT STRING_AGG(Prod.legalCode,',') FROM appointmentProducts AS AppProd WITH(NOLOCK)
				INNER JOIN products AS Prod WITH(NOLOCK) ON AppProd.idProduct = Prod.idProduct
			WHERE AppProd.idAppointment = appointment.idAppointment) AS [Código CUPS],
		ISNULL(Rooms.nameRoom, '') AS [Consultorio],
		appointment.idAppointment AS [ID],
		IIF(appointment.isExtra = 1,'SI','') AS [Es adicional],
		appstate.itemName AS [Estado Cita],
		CompanyOff.name AS [Sede],
		usCOn.businessName AS [Entidad],
		Con.name AS [Contrato],
		ContP.name AS [Plan],
		HealthR.name AS [Régimen],
		DATEDIFF(HOUR,UP.birthDate,AppSd.dateAppointment)/8766 AS [Edad],
		Gender.name AS [Género],
		UP.telecom AS [Celular],
		UP.phoneHome AS [Teléfono],
		UP.Email AS [Email],
		appointment.dateRecord AS [Fecha Asignación Cita],
		CONCAT_WS(' ',userRegister.firstFamilyName, userRegister.secondFamilyName, userRegister.firstGivenName,	userRegister.secondGiveName) AS [Usuario Asigna Cita],
		userRegister.documentNumber AS [Documento Usuario Asigna Cita],
		appointment.expectedDate AS [Fecha deseada],
		CONCAT_WS(' ',Us.firstFamilyName, Us.secondFamilyName, Us.firstGivenName, Us.secondGiveName)AS [Usuario Creación Agenda],
		ISNULL(enc.identifier, '') AS [Ingreso],
		AdmitS.name AS [Vía de ingreso], 
		appointment.note AS [Observación]
	FROM appointmentSchedulers AS AppS WITH (NOLOCK)
		INNER JOIN appointmentSchedulerSlots AS AppSd WITH (NOLOCK) ON AppS.idAppointmenScheduler = AppSd.idAppointmentScheduler
		INNER JOIN appointments AS appointment ON appointment.idAppointmentSchedulerSlots = AppSd.idAppointmentSchedulerSlots
		INNER JOIN companyOffices AS CompanyOff WITH (NOLOCK) ON CompanyOff.idOffice = AppS.idOffice
		INNER JOIN users AS practitioner WITH (NOLOCK) ON practitioner.idUser = AppS.idUserProfessional
		LEFT OUTER JOIN userSystemSpecialities AS USpec WITH(NOLOCK) ON practitioner.idUser = USpec.idUser
			AND USpec.isPrincipal = 1
			AND USpec.isActive = 1
		LEFT OUTER JOIN generalSpecialties AS Spec WITH(NOLOCK) ON USpec.idSpeciality = Spec.idSpecialty

		INNER JOIN generalInternalLists AS appState WITH(NOLOCK) ON appointment.state = appState.itemValue
			AND appState.groupCode = 'appState'
		INNER JOIN users AS Us WITH (NOLOCK) ON Us.idUser = AppS.idUserRecord
		INNER JOIN users AS Company WITH (NOLOCK) ON Company.idUser = AppS.idUserCompany
		LEFT OUTER JOIN physicalLocationRooms AS Rooms WITH (NOLOCK) ON Rooms.idRoom = AppS.idRoom
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
		LEFT OUTER JOIN encounterConfAdmitSource AS AdmitS WITH(NOLOCK) ON enc.idAdmitSource = AdmitS.idAdmitSource
		LEFT OUTER JOIN encounterRecords AS EncRecord WITH(NOLOCK) ON enc.idEncounter = EncRecord.idEncounter

		INNER JOIN users AS userRegister WITH (NOLOCK) ON userRegister.idUser = appointment.userRecord
		LEFT OUTER JOIN EHREvents AS evn WITH(NOLOCK) ON evn.idEHREvent = appointment.idEvent

	WHERE AppS.idUserCompany = @idUserCompany
		--AND AppS.idOffice IN (SELECT Value FROM [dbo].[FnSplit](@officeFilter))
		--AND AppS.idUserProfessional IN (SELECT Value FROM STRING_SPLIT(@idUserProfessional,','))
		AND CE.idAppointmentExam IN (SELECT Value FROM STRING_SPLIT(@idExam,','))
		AND appState.itemValue IN (SELECT Value FROM STRING_SPLIT(@state,','))
		AND CONVERT(DATE,AppSd.dateAppointment) > '{last_week}'
		--AND Patient.firstFamilyName + ISNULL(' ' + Patient.secondFamilyName,'') + ' ' + Patient.firstGivenName + ISNULL(' ' + Patient.secondGiveName,'') LIKE '%' + ISNULL(@patientName,'') + '%'
	ORDER BY AppSd.dateAppointment"""

    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    print("Como está leyendo el dataframe inicialmente",df)
    print("Nombres y tipos de columnas leídos del dataframe sin transformar",df.dtypes)

    df.columns = remove_accents_cols(df.columns)

    df.columns = remove_special_chars(df.columns)

    df.columns = regular_snake_case(df.columns)

    print("Como está leyendo el dataframe inicialmente",df)
    print("Nombres y tipos de columnas leídos del dataframe transformados",df.dtypes)

    #esta funcion se aplica a 4 columnas para llevar los string a mayusculas.
    
    print('columnas al momento',df.columns)
    
    str_columns = [
        'profesional', 
        'documento_profesional', 
        'especialidad', 
        'sede'
    ]

    for i in str_columns:
        df[i] = normalize_str_categorical(df[i].astype(str))


    #se calcula la columna de oportunidad real
    df['oportunidad'] = (df['fecha_cita'] -df['fecha_asignacion_cita']).dt.days

    #se calcula la columna de oportunidad deseada
    df['oportunidad_deseada'] = (df['fecha_deseada']-df['fecha_asignacion_cita']).dt.days

    df['codigo_cups'] = normalize_str_categorical(df['codigo_cups'])
    df['codigo_cups'] = df['codigo_cups'].str.extract(r'(^\d+\w)')


    date_columns = ['atendido','fecha_cita','fecha_asignacion_cita','fecha_deseada']

    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        df[i] = pd.to_datetime(df[i], format="%Y-%m-%d", errors = 'coerce')
        
    missing_columns = ['id_cups']
    for i in missing_columns:
        df[i] = 'nan'
            

    df = df[[
        'id',
        'id_patient',
        'atendido', 
        'id_professional', 
        'profesional',
        'documento_profesional',
        'especialidad', 
        'fecha_cita',
        'tipo_cita',
        'examen',
        'id_cups',
        'codigo_cups',
        'estado_cita',
        'sede',
        'entidad',
        'contrato', 
        'fecha_asignacion_cita',
        'fecha_deseada',
        'ingreso',
        'oportunidad',
        'oportunidad_deseada'
        ]
    ]


    dict = {
        'id':'id',
        'id_patient':'id_patient',
        'atendido':'attended', 
        'id_professional':'id_professional', 
        'profesional':'professional',
        'documento_profesional':'professional_document',
        'especialidad':'speciality',
        'fecha_cita':'appointment_date',
        'tipo_cita':'appointment_type',
        'examen':'exam',
        'id_cups':'id_cups',
        'codigo_cups' : 'cups_code', 
        'estado_cita':'appointment_status', 
        'sede':'headquarter',
        'entidad':'entity', 
        'contrato':'contract',
        'fecha_asignacion_cita':'appointment_assignment_date', 
        'fecha_deseada':'desired_date',
        'ingreso':'encounter_identifier', 
        'oportunidad':'oportunity', 
        'oportunidad_deseada':'desired_oportunity'
    }
    
 
    # call rename () method
    df.rename(columns=dict,
          inplace=True)

    date_columns = [
        'attended',
        'appointment_date',
        'appointment_assignment_date',
        'desired_date'
    ]

    for i in date_columns:
        df[i] = df[i].astype(str)

    print(df.dtypes)
    print(df.columns)
    print(df)

    # # CARGA A BASE DE DATOS
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
    # Se establece la ejecución del dag todos los viernes a la 6:00 am(Hora servidor)
    schedule_interval = ' 50 5 * * 2',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_fact_appointments_gomedisys_python_task = PythonOperator(task_id = "get_fact_appointments_gomedisys",
                                                  python_callable = func_get_fact_appointments_gomedisys,
                                                  email_on_failure=True, 
                                                  email='BI@clinicos.com.co',
                                                  dag=dag,
                                                  )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_appointments_gomedisys = MsSqlOperator(task_id='Load_fact_appointments_gomedisys',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_fact_appointments_gomedisys",
                                       email_on_failure=True, 
                                       email='BI@clinicos.com.co',
                                       dag=dag,
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_fact_appointments_gomedisys_python_task >> load_fact_appointments_gomedisys >> task_end

