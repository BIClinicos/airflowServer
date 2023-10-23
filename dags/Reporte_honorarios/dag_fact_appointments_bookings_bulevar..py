from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime
import pandas as pd
from variables import sql_connid
from utils import load_df_to_sql,file_get,normalize_str_categorical, replace_accents_cols
#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'oportunidad'
dirname = '/opt/airflow/dags/files_oportunidad/'
filename = 'TEC_PYR_BookingsBulevar.xlsx'
db_table_dim = "dim_patients"
db_tmp_table_dim = "tmp_dim_patients"
db_table_fact = "fact_appointments_bookings"
db_tmp_table_fact = "tmp_appointments_bookings"
dag_name = 'dag_' + 'fact_appointments_bookings_Bulevar'

# Función de transformación de los archivos xlsx
def transform_tables (path):

    # Lectura del archivo de excel
    df = pd.read_excel(path)


    # se añaden las columnas "sede" y "entidad"
    df['sede'] = 'BULEVAR'
    df['entidad'] = 'ECOPETROL S.A.'
    df['Cita Asignada en E-Salud'] = '1'

    # cambio de nombres de columnas
    
    df = df.rename(
        columns = {
            'ID' : 'id',
            'Servicio' : 'service',
            'Fecha Solicitud Cita' : 'appointment_request_date',
            'Fecha deseada' : 'desired_date',
            'Oportunidad' : 'pertinence',
            'FechaInicioCita' : 'appointment_start_date',
            'FechaFinCita' : 'appointment_end_date',
            'Profesional' : 'professional',
            'TipoConsulta' : 'consultation_type',
            'TipoIdentificacion' : 'document_type',
            'NumeroIdentificacion' : 'document_number',
            'Title' : 'full_name',
            'Correo' : 'mail',
            'Direccion' : 'home_address',
            'Estado' : 'status',
            'Tipo de AFILIACIÓN' : 'membership_type',
            'MEGA-MEPA' : 'mega',
            'SEXO' : 'gender',
            'EDAD' : 'age',
            'Unidad de Medida' : 'unit_measure',
            'Telefono' : 'phone_number',
            'Nombre Persona Contacto' : 'contact_name',
            'Vinculo' : 'relationship',
            'Teléfono Contacto' : 'phone_contact',
            'ModalidadConsulta' : 'consultation_modality',
            'Tipo de Telesalud' : 'telehealth_type',
            'Estado de la Cita' : 'appointment_status',
            'Causa Externa' : 'external_cause',
            'Finalidad de la Consulta' : 'consultation_purpose',
            'CUPS' : 'cups',
            'Diagnóstico' : 'dx',
            'CIE10' : 'cie10',
            'Tipo de Diagnóstico' : 'dx_type',
            'Diagnóstico Secundario' : 'secondary_dx',
            'CIE10 Secundario' : 'secondary_cie10',
            'Tipo de Diagnóstico Sec' : 'secondary_dx_type',
            'II Diagnóstico Secundario' : 'tertiary_dx',
            'CIE10 II Secundario' : 'tertiary_cie10',
            'Tipo de Diagnóstico II Sec' : 'tertiary_dx_type',
            'III Diagnóstico Secundario' : 'quaternary_dx',
            'CIE10 III Secundario' : 'quaternary_cie10',
            'Tipo de Diagnóstico III Sec' : 'quaternary_dx_type',
            'Seguimiento' : 'tracing',
            'Resultado Seguimiento' : 'tracing_result',
            'SintomasAsociadosCovid' : 'covid_associate_symptoms',
            'Persona que Asigna' : 'assigning_person',
            'Procedimientos' : 'procedures',
            'Notas' : 'notes',
            'Remitente' : 'sender',
            'Modificado' : 'modified',
            'Cita Asignada en E-Salud' : 'esalud_assigned_appoinment',
            'IDEvento' : 'eventID',
            'Tipo de elemento' : 'element_type',
            'Ruta de acceso' : 'path',
            'sede' : 'headquarter',
            'entidad' : 'entity'
        }
    )

    # df seleccionados
    df = df[['id',
            'service',
            'appointment_request_date',
            'desired_date',
            'pertinence',
            'appointment_start_date',
            'appointment_end_date',
            'professional',
            'consultation_type',
            'document_type',
            'document_number',
            'full_name',
            'mail',
            'home_address',
            'status',
            'membership_type',
            'mega',
            'gender',
            'age',
            'unit_measure',
            'phone_number',
            'contact_name',
            'relationship',
            'phone_contact',
            'consultation_modality',
            'telehealth_type',
            'appointment_status',
            'external_cause',
            'consultation_purpose',
            'cups',
            'dx',
            'cie10',
            'dx_type',
            'secondary_dx',
            'secondary_cie10',
            'secondary_dx_type',
            'tertiary_dx',
            'tertiary_cie10',
            'tertiary_dx_type',
            'quaternary_dx',
            'quaternary_cie10',
            'quaternary_dx_type',
            'tracing',
            'tracing_result',
            'covid_associate_symptoms',
            'assigning_person',
            'procedures',
            'notes',
            'sender',
            'modified',
            'esalud_assigned_appoinment',
            'eventID',
            'element_type',
            'path',
            'headquarter',
            'entity'
    ]]

    # normalización de columnas categóricas

    str_col = [
        'service',
        'professional',
        'consultation_type',
        'document_type',
        'full_name',
        'home_address',
        'status',
        'membership_type',
        'mega',
        'gender',
        'unit_measure',
        'contact_name',
        'relationship',
        'consultation_modality',
        'telehealth_type',
        'appointment_status',
        'external_cause',
        'consultation_purpose',
        'dx',
        'dx_type',
        'secondary_dx',
        'secondary_dx_type',
        'tertiary_dx',
        'tertiary_dx_type',
        'quaternary_dx',
        'quaternary_dx_type',
        'tracing',
        'tracing_result',
        'covid_associate_symptoms',
        'assigning_person',
        'procedures',
        'notes',
        'sender'
        ]

    for i in str_col:
        df[i] = normalize_str_categorical(df[i])


    # Columna service

    df_2 = df['service'].str.split(':', n=1, expand = True)
    df_2

    df_2 = pd.DataFrame(data = df_2)
    df_2 = df_2.rename(
        columns = {
            0 : 'cero',
            1 : 'uno'
        }
    )

    df_2['cero'] = df_2['cero'].fillna('')

    stand_val = [
        'CONSULTA PRIORITARIA MEDICINA GENERAL',
        'CONSULTA PREFERENTE MEDICINA GENERAL',
        'QUIMICO FARMACEUTICO', 
        'MEDICO OCUPACIONAL', 
        'CONSULTA PRIORITARIA MEDICINA GENERAL',
        'CONSULTA DE ENFERMERÍA RIESGO CARDIOVASCULAR'
        'CRECIMIENTO Y DESARROLLO',
        'CRECIMIENTO Y DESARROLLO',
        'TERAPIA FÍSICA',
        'FISIOTERAPEUTAS',
        'VEJEZ',
        'ADOLESCENCIA',
        'CITOLOGÍA',
        'ADOLECENTE'
        'ADULTEZ',
        'CITOLOGIA',
        'MEDICINA INTERNA' 
        'FISIATRÍA', 
        'MEDICO GENERAL PARA CONSULTA PRIORITARIA',
        'FISIOTERAPIA'
    ]
    for i in stand_val:
        df_2.loc[df_2['cero'].str.contains(i) == True, 'cero'] = i


    df['service'] = df_2['cero']
    print(df['service'].value_counts())

    # Columna 'desired_date'

    def normalize(col_val):
        replacements = (
            
            (' DE ENERO DE ' , '/01/'),
            (' DE FEBRERO DE ' , '/02/'),
            (' DE MARZO DE ' , '/03/'),
            (' DE ABRIL DE ' , '/04/'),
            (' DE MAYO DE ' , '/05/'),
            (' DE JUNIO DE ' , '/06/'),
            (' DE JULIO DE ' , '/07/'),
            (' DE AGOSTO DE ' , '/08/'),
            (' DE SEPTIEMBRE DE ' , '/09/'),
            (' DE OCTUBRE DE ' , '/10/'),
            (' DE NOVIEMBRE DE ' , '/11/'),
            (' DE DICIEMBRE DE ' , '/12/'),
            (' DE ENERO ' , '/01/'),
            (' DE FEBRERO ' , '/02/'),
            (' DE MARZO ' , '/03/'),
            (' DE ABRIL ' , '/04/'),
            (' DE MAYO ' , '/05/'),
            (' DE JUNIO ' , '/06/'),
            (' DE JULIO ' , '/07/'),
            (' DE AGOSTO ' , '/08/'),
            (' DE SEPTIEMBRE ' , '/09/'),
            (' DE OCTUBRE ' , '/10/'),
            (' DE NOVIEMBRE ' , '/11/'),
            (' DE DICIEMBRE ' , '/12/'),
            (' ENERO ' , '/01/'),
            (' FEBRERO ' , '/02/'),
            (' MARZO ' , '/03/'),
            (' ABRIL ' , '/04/'),
            (' MAYO ' , '/05/'),
            (' JUNIO ' , '/06/'),
            (' JULIO ' , '/07/'),
            (' AGOSTO ' , '/08/'),
            (' SEPTIEMBRE ' , '/09/'),
            (' OCTUBRE ' , '/10/'),
            (' NOVIEMBRE ' , '/11/'),
            (' DICIEMBRE ' , '/12/'),
            ('20222' , '2022'),
            ('222' , '2022')
        )
        for a, b in replacements:
            col_val = col_val.replace(a, b).replace(a.upper(), b.upper())
        return col_val

    df['desired_date'] = df['desired_date'].fillna('')
    df['desired_date'] = df['desired_date'].apply(lambda x: normalize(x))
    df['desired_date'] = pd.to_datetime(df['desired_date'], errors='coerce')

    print(df['desired_date'].isna().sum())

    # Columna 'consultation_type'

    df['consultation_type'] = df['consultation_type'].fillna('')    
    df['consultation_type'] = df['consultation_type'].apply(lambda x: 'PRIMERA VEZ' if (x == 'PRIMERA VEZ') else 'CONTROL' if (x == 'CONTROL') else '')

    print(df['consultation_type'].unique())

    # Columna 'document_type'

    df['document_type'] = df['document_type'].str.replace(r'[$-@&/.:()]','', regex=True)
    df['document_type'] = df['document_type'].fillna('')
    df.loc[df['document_type'].str.contains('CÉD'), 'document_type'] = 'CC'

    print(df['document_type'].unique())

    # Columna 'document_number'

    df['document_number'] = df['document_number'].astype(str)
    df['document_number'] = normalize_str_categorical(df['document_number'])
    cond1 = df['document_type'].isin(['CC', 'TI', 'RC', 'NV', 'CE'])
    cond2 = pd.to_numeric(df['document_number'], errors='coerce').isna()
    cond3 = cond1 & cond2
    df.loc[cond3,['document_number']] = df.loc[cond3]['document_number'].str.replace('.','').str.extract(r'(-?\d+\.?)').values
    df['document_number'] = df['document_number'].fillna('NO DATA')

    print(df[['document_type','document_number']].head(46))

    # Columna 'status'

    tmp1 = df['status'].str.startswith('ACT', na = False)
    tmp2 = df['status'].str.startswith('INAC', na = False)
    df.loc[tmp1, 'status'] = 'ACTIVO/A'
    df.loc[tmp2, 'status'] = 'INACTIVO/A'

    print(df['status'].unique())

    # Columna 'membership_type'

    df['membership_type'].fillna('NO DATA')
    w1 = df['membership_type'].str.startswith('CALLE', na = False)
    w2 = df['membership_type'].str.startswith('OKOK', na = False)
    df.loc[w1, 'membership_type'] = ''
    df.loc[w2, 'membership_type'] = ''

    print(df['membership_type'].unique())

    # Columna 'gender'

    df['gender'] = df['gender'].fillna('')
    df.loc[df['gender'].str.contains('MMJ'), 'gender'] = ''  
    df['gender'] = df['gender'].replace('M','MASCULINO').replace('F','FEMENINO')

    print(df['gender'].unique())

    # Columna 'telehealth_type'

    df['telehealth_type'] = df['telehealth_type'].fillna('NO DATA')
    df['telehealth_type'] = df['telehealth_type'].str.replace('0','')

    print(df['telehealth_type'].unique())

    # Columna 'appointment_status'

    df['appointment_status'] = df['appointment_status'].fillna('NO DATA')
    df['appointment_status'] = df['appointment_status'].apply(lambda x: replace_accents_cols(x))

    print(df['appointment_status'].unique())

    # Columna 'covid_associate_symptoms'

    df['covid_associate_symptoms'] = df['covid_associate_symptoms'].fillna('NO DATA')
    df['covid_associate_symptoms'] = df['covid_associate_symptoms'].apply(lambda x: 'NO' if (x == 'NO') else 'SI' if (x == 'SI') else '')

    print(df['covid_associate_symptoms'].unique())

    # Columna 'cups'

    df['cups'] = normalize_str_categorical(df['cups'])
    df['cups'] = df['cups'].fillna('NO DATA')
    df['cups'] = df['cups'].replace('0', 'NO DATA')
    df['cups'] = df['cups'].str.extract(r'(^\d+\w)')

    print(df['cups'].unique())

    # Columna 'age'

    #df['age'] = df['age'].str.extract(r'(^\d*\d)')
    df['age'] = pd.to_numeric(df['age'], errors='coerce', downcast="integer")
    df['age'] = df['age'].fillna('0')
    df['age'] = df['age'].astype(int)

    print(df['age'].unique())

    # Columna 'modified'

    df['modified'] = pd.to_datetime(df['modified'], errors='coerce')
    print(df['modified'].unique())

    # Se pasan las columnas datetime a str para cargue a la base de datos

    date_col = [
        'appointment_request_date', 
        'desired_date',
        'appointment_start_date', 
        'appointment_end_date',
        'modified'
    ]

    for i in date_col:
        df[i] = df[i].astype(str)

    # Se añade la columna birth_date vacía

    df['birth_date'] = ''

    # 202307 Truncamiento de mail
    df['mail'] = df['mail'].str.slice(0,55)
    
    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_appointments_bookings ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    
    print(df)
    print(df.dtypes)
    print(df.columns)

    df_patients = df[
        [
        'document_type', 
        'document_number',
        'full_name', 
        'mail', 
        'home_address',
        'birth_date',
        'gender', 
        'phone_number', 
        'contact_name',
        'relationship', 
        'phone_contact'
       ]
    ]


    print(df_patients.info())
    print(df_patients.columns)

    df_fact_bookings = df[
        [
        'id', 
        'service', 
        'appointment_request_date', 
        'desired_date',
        'appointment_start_date', 
        'appointment_end_date',
        'professional', 
        'consultation_type',
        'document_type',
        'document_number', # llave conexión con la dimensión de pacientes
        'membership_type', 
        'mega',
        'age',
        'unit_measure', 
        'consultation_modality',
        'telehealth_type', 
        'appointment_status', 
        'external_cause',
        'consultation_purpose', 
        'cups', 
        'dx', 
        'cie10', 
        'dx_type',
        'secondary_dx', 
        'secondary_cie10', 
        'secondary_dx_type', 
        'tertiary_dx',
        'tertiary_cie10', 
        'tertiary_dx_type', 
        'quaternary_dx',
        'quaternary_cie10', 
        'quaternary_dx_type', 
        'tracing',
        'tracing_result',
        'covid_associate_symptoms', 
        'assigning_person', 
        'procedures', 
        'notes',
        'sender', 
        'modified', 
        'esalud_assigned_appoinment', 
        'eventID',
        'element_type', 
        'path', 
        'headquarter', 
        'entity'
       ]
    ]
    print(df_fact_bookings.info())
    print(df_fact_bookings.columns)

    df_patients = df_patients.drop_duplicates(subset=[
        'document_type', 
        'document_number'
       ]
    )

    df_fact_bookings = df_fact_bookings.drop_duplicates(subset=[
        'id', 
        'appointment_request_date',
        'headquarter', 
        'entity'
       ]
    )


    if ~df.empty and len(df_patients.columns) >0:
        load_df_to_sql(df_patients, db_tmp_table_dim, sql_connid)

    if ~df.empty and len(df_fact_bookings.columns) >0:
        load_df_to_sql(df_fact_bookings, db_tmp_table_fact, sql_connid)


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
    # se establece la ejecución a las 12:20 PM(Hora servidor) todos los sabados
    schedule_interval= '40 6 * * 2',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')


    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_appointments_bookings_python_task = PythonOperator( task_id = "get_appointments_bookings",
                                                        python_callable = func_get_appointments_bookings,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dim_patient = MsSqlOperator( task_id='Load_dim_patient',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql="EXECUTE sp_load_dim_patients",
                                            email_on_failure=True,
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                       )

    load_fact_appointments_bookings = MsSqlOperator( task_id='Load_fact_appointments_bookings',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql="EXECUTE sp_load_fact_appointments_bookings",
                                            email_on_failure=True,
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_appointments_bookings_python_task >> load_dim_patient >> load_fact_appointments_bookings >> task_end