import os
import xlrd
import re
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
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'oportunidad'
dirname = '/opt/airflow/dags/files_oportunidad/'
filename = 'TEC_PYR_OportunidadDuitama.xlsx'
db_table = 'fact_appointments_bw_compensar'
db_table2 = 'dim_patients'
db_tmp_table = 'tmp_fact_appointments_bw_compensar'
db_tmp_table2 = 'tmp_dim_patients'
dag_name = 'dag_' + 'TEC_PYR_BaseDuitama'
container_to = 'historicos'

# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def transform_tables(path):
    
    df = pd.read_excel(path)

    print("Como está leyendo el dataframe inicialmente",df)
    print("Nombres y tipos de columnas leídos del dataframe sin transformar",df.dtypes)
    # Estandarización de los nombres de columnas del dataframe
    
    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)

    print("columns formated",df.columns)


    #Esta función utiliza replace una función de pandas para reemplazar valores
    df['episodio'] = df['episodio'].replace(['ASISTIDOS','NO ASISTIDOS'],['ASISTIDAS','NO ASISTIDAS'])

    #Se reemplaza el valor H y M por Masculino y Femenino para dejar sólo dos categorías en la columna sexo.
    df['sexo'] = df['sexo'].replace(['H','M'],['MASCULINO','FEMENINO'])

    # Estandarización de las columnas de fechas a tipo fecha

    date_columns = ['fecha_de_entrada','fecha_citacion','fecha_deseada','fecha_nacimiento']

    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        df[i] = pd.to_datetime(df[i], format="%Y-%m-%d", errors = 'coerce')

    #Se crea la columnan nombre_completo uniendo las 4 columnas de number.
    df['nombre_completo'] =  df['primer_nombre'] + ' ' + df['segundo_nombre'] + ' '+ df['ic_apellido'] + ' '+ df['ic_apellido_soltero']

    #se elimina las columnas de nombre parcial
    df.drop(columns=['ic_apellido','ic_apellido_soltero','primer_nombre', 'segundo_nombre'])

    #se calcula la columna de oportunidad real
    df['oportunidad'] = (df['fecha_citacion'] -df['fecha_de_entrada']).dt.days

    #se calcula la columna de oportunidad deseada
    df['oportunidad_deseada'] = (df['fecha_deseada']-df['fecha_de_entrada']).dt.days

    #se añade columna con valor constante. 
    df['entidad'] ='UNION TEMPORAL CLINICOS ATENCION MEDICA BOYACA - UT'
    
    df['cups'] = normalize_str_categorical(df['prestacion'].astype(str))
    df['cups'] = df['cups'].str.extract(r'(^\d+\w)')
    df['cups'] = df['cups'].fillna('NO DATA')

    # Columna que tiene nombre vacio
    df['nombre_completo_de_la_prestacion'] = df['unnamed_16']

    df = df[['sede_de_u_o', 'unidad_organizativa', 'episodio', 'paciente','tip_doc_identifica', 
       'num_identificacion','fecha_nacimiento', 'sexo', 'nombre_completo','fecha_de_entrada','fecha_citacion','fecha_deseada',
       'cups','nombre_completo_de_la_prestacion','cantidad_citas','cantidad_pacientes','citas_cumplidas',
       'creado_por','nombre_usuario_gener','codigo_usuario_que_g','oportunidad','oportunidad_deseada','entidad']]

    dict = {'sede_de_u_o':'headquarter','unidad_organizativa': 'organizational_unit', 'episodio':'episode', 'paciente':'id_patient',
    'tip_doc_identifica':'document_type','num_identificacion':'document_number', 'fecha_nacimiento':'birth_date',
    'sexo':'gender','nombre_completo':'full_name','fecha_de_entrada':'appointment_assignment_date','fecha_citacion':'appointment_date','fecha_deseada':'appointment_desired_date',
    'cups':'cups','nombre_completo_de_la_prestacion':'service','cantidad_citas':'appointment_quantity','cantidad_pacientes':'patient_quantity',
    'citas_cumplidas':'accomplished_appointments','creado_por':'created_by','nombre_usuario_gener':'user_name','codigo_usuario_que_g':'user_code',
    'oportunidad':'oportunity','oportunidad_deseada':'desired_oportunity','entidad':'entity'}
    
    df.rename(dict,axis='columns', inplace=True)

    # Normalize keys
    keys = ['headquarter','document_type','document_number','appointment_assignment_date','appointment_date','cups','created_by']
    for col in keys:
        df[col] = normalize_str_categorical(df[col].astype(str))
    
    print('info tabla')
    df.info()
 
    return df

def func_get_TEC_PYR_BaseDuitama():    

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    print('columnas',df.columns)
    print(df.dtypes)
    print(df.head(20))
    print(df)
    
  
    int_cols = ['oportunity','desired_oportunity']
    for col in int_cols:
        df[col] = df[col].astype(str).str.replace('.0','',regex=False)

    columnas_ausentes = ['mail','home_adress','phone_number','contact_name','contact_name','relationship','phone_contact']
    for i in columnas_ausentes:
        df[i] = 'nan'
    

    df_patients = df[
        ['document_type',
        'document_number',
        'full_name',
        'mail',
        'home_adress',
        'birth_date',
        'gender',
        'phone_number',
        'contact_name',
        'relationship',
        'phone_contact'
        ]
    ]

    df_fact_appointments_bw_compensar = df[
        ['headquarter',
        'organizational_unit',
        'episode',
        'document_type',
        'document_number',
        'appointment_assignment_date',
        'appointment_date',
        'appointment_desired_date',
        'cups',
        'service',
        'appointment_quantity',
        'patient_quantity',
        'accomplished_appointments',
        'created_by',
        'user_name',
        'user_code',
        'oportunity',
        'desired_oportunity',
        'entity'
    ]
]   

 
    print('columnas de las tablas',df_patients.columns) 
    print('columnas de las tablas2',df_fact_appointments_bw_compensar.columns) 

    print(df_fact_appointments_bw_compensar)
    
    df_patients = df_patients.drop_duplicates(subset=['document_type', 'document_number'])
    
    df_fact_appointments_bw_compensar = df_fact_appointments_bw_compensar.drop_duplicates(subset=['headquarter','document_type','document_number','appointment_assignment_date','appointment_date','cups','created_by'])

    if ~df.empty and len(df_patients.columns) >0:
        load_df_to_sql(df_patients, db_tmp_table2, sql_connid)

    if ~df.empty and len(df_fact_appointments_bw_compensar.columns) >0:
        load_df_to_sql(df_fact_appointments_bw_compensar, db_tmp_table, sql_connid)

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
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '0 6 * * 2',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    get_TEC_PYR_BaseDuitama_python_task = PythonOperator(task_id = "get_TEC_PYR_BaseDuitama",
                                                python_callable = func_get_TEC_PYR_BaseDuitama,
                                                email_on_failure=True, 
                                                email='BI@clinicos.com.co',
                                                dag=dag,
                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_TEC_PYR_BaseDuitama_dim = MsSqlOperator(task_id='Load_TEC_PYR_BaseDuitama_dim',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_dim_patients",
                                       email_on_failure=True, 
                                       email='BI@clinicos.com.co'
                                       )
    
 #   # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_TEC_PYR_BaseDuitama_fact = MsSqlOperator(task_id='Load_TEC_PYR_BaseDuitama_fact',
                                      mssql_conn_id=sql_connid,
                                      autocommit=True,
                                      sql="EXECUTE sp_load_fact_appointments_bw_compensar",
                                      email_on_failure=True, 
                                      email='BI@clinicos.com.co',
                                      )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_TEC_PYR_BaseDuitama_python_task >> load_TEC_PYR_BaseDuitama_dim >> load_TEC_PYR_BaseDuitama_fact >>task_end