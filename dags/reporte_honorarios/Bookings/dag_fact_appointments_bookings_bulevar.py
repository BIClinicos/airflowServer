from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime
import pandas as pd

from variables import sql_connid
from utils import load_df_to_sql,file_get,normalize_str_categorical, replace_accents_cols 
from reporte_honorarios.utils.CONSTANS_BULEVAR import *
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
    
    df = df.rename(columns = COLUMNS_BULEVAR)

    # df seleccionados
    df = df[COLUMNS_SELECTED]

    # normalización de columnas categóricas

    str_col = STR_COL

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

    stand_val = STAND_VAL
    
    for i in stand_val:
        df_2.loc[df_2['cero'].str.contains(i) == True, 'cero'] = i


    df['service'] = df_2['cero']
    print(df['service'].value_counts())

    # Columna 'desired_date'

    def normalize(col_val):
        replacements = REPLACEMENTS
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
        COLUMNS_PATIENTS
    ]


    print(df_patients.info())
    print(df_patients.columns)

    df_fact_bookings = df[
        FACT_BOOKINGS
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