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
from variables import sql_connid
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get, normalize_str_categorical, remove_accents_cols, remove_special_chars, regular_camel_case, regular_snake_case

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'rcv'
dirname = '/opt/airflow/dags/files_rcv/'
filename = 'SAL_PRI_EC_RCV.xlsx'
db_table_dim = "dim_patients"
db_tmp_table_dim = "tmp_dim_patients"
db_table_fact = "fact_rcv"
db_tmp_table_fact = "tmp_fact_rcv"
dag_name = 'dag_' + db_table_fact


# Función de transformación de los archivos xlsx
def transform_data (path):

    # lectura del archivo de excel
    df = pd.read_excel(path, header = [0], converters={'# CC': str})



    # estandarización de los nombres de las columnas
    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)


    # cambio de nombres de columnas a ingles

    df = df.rename(
        columns = {
            '#_cc' : 'document_number', 
            'nomres_y_apellidos_del_usuario' : 'full_name', 
            'celular_1' : 'phone_number', 
            'celular_2' : 'phone_number_two',
            'fecha_de_nacimiento' : 'birth_date', 
            'edad' : 'age', 
            'genero' : 'gender', 
            'hta' : 'hta', 
            'dm' : 'dm', 
            'talla' : 'height',
            'fecha_talla' : 'height_date', 
            'peso' : 'weight', 
            'imc' : 'bmi', 
            'fecha_peso' : 'weight_date',
            'ultima_fecha_ultima_atencion' : 'last_atention_date', 
            'resultado_ultima_tas' : 'last_tas',
            'resultado_ultima_tad' : 'last_tad', 
            'fecha_ultima_ta' : 'date_last_ta', 
            'result_tension' : 'category_ta',
            'hta_controlada?' : 'hta_controlled', 
            'peso_estado' : 'category_weight', 
            'ultima_creatinina' : 'last_creatine',
            'fecha_ultima_creatinina' : 'date_last_creatine', 
            'ultima_hba1c' : 'last_hba1c', 
            'fecha_ultima_hba1c' : 'date_last_hba1c',
            'rango_hba1c_%_ultima' : 'last_hba1c_range', 
            'metas_hba1c' : 'hba1c_controlled', 
            'anterior_hba1c' : 'previous_hba1c',
            'rango_hba1c_%_anterior' : 'previous_hba1c_range', 
            'diferencia_hba1c_(ultima_anterior)' : 'difference_hba1c',
            'seguimiento_hba1c' : 'tracking_hba1c', 
            'ultima_microalbuminuria' : 'last_microalbuminuria',
            'ultima_relacion_albuminuria_creatinuria' : 'last_ratio_albuminuria_creatinuria', 
            'ultimo_ct' : 'last_ct', 
            'ultimo_ldl' : 'last_ldl',
            'metas_ldl' : 'ldl_controlled', 
            'ultimo_tg' : 'last_tg', 
            'fecha_ultimo_tg' : 'date_last_tg',
            'tasa_de_filtracion_glomerular_cockroft_gault' : 'gfr_cockroft_gault',
            'estadio_erc_cockroft_gault' : 'stage_cockroft_gault', 
            'enfermera' : 'nursing_professional',  
            'mega' : 'mega', 
            'periodo' : 'period',
            'insulinizado' : 'insulinized',
            'medicamentos_antihipertensivos' : 'antihyp_med',
            'medicamentos_orales_dm' : 'oral_med_dm',
            'medicamentos_insulinizados_dm' : 'insulinized_med_dm',
            'insulinizado' : 'insulinized',
            'glucometro' : 'glucometer',
            'bomba' : 'insulin_pump',
            'anterior_creatinina' : 'previous_creatine',
            'fecha_anterior_creatitinina' : 'date_previous_creatine',
            'diferencia_creatinina' : 'diff_creatine',
            'diferencia_fechas_creatinina' : 'diff_date_creatine',
            'seguimiento_creatininas' : 'creatine_monitoring',
            'diferencia_fechas_hba1c' : 'diff_date_hba1c',
            'tipo_de_beneficiario' : 'beneficiary_type',
            'curso_de_vida' : 'lifetime',
            'clasificacion_de_riesgo' : 'risk_stratification',
            'valoracion_pie_diabetico' : 'diabetic_foot_exam',
            'clasificacion_de_riesgo_pie_diabetico' : 'diabetic_foot_grade',
            #'tasa_de_filtracion_glomerular_ckd_epi' : 'gfr_ckd_epi',
            #'estadio_erc_ckd_epi' : 'stage_ckd_epi', 
            #'rcv' : 'rcv',
            #'erc' : 'erc', 
            'prestadores' : 'provider'
        }
    )
    print(df['last_creatine'].head(10))
    # Llenar vacios en tracking_hba1c
    df['tracking_hba1c'] = df['tracking_hba1c'].fillna('N/D')
    # Recortar insulinized
    df['insulinized'] = df['insulinized'].astype(str)
    df['insulinized'] = df['insulinized'].str[:5]
    # Llenar vacios en nursing_professional
    df['nursing_professional'] = df['nursing_professional'].fillna('JEFE YENITH PUENTES')

    # Se agrega la columna document_type = cc

    df['document_type'] = 'CC'


    # Se eliminan los registros sin cédulas

    df = df.dropna(subset=['document_number'])


    # Estandarización columna "gender"

    df['gender'] = df['gender'].replace('M','MASCULINO').replace('F','FEMENINO')
    

    # Estandarización columnas numéricas

    float_col = [
        'age',
        'height',
        'weight', 
        'bmi', 
        'last_tas', 
        'last_tad', 
        'last_creatine', 
        'last_hba1c',
        'previous_hba1c',
        'last_microalbuminuria',
        'last_ratio_albuminuria_creatinuria', 
        'last_ct', 
        'last_ldl',
        'last_tg', 
        'gfr_cockroft_gault',
        'previous_creatine',
        'diff_creatine',
        'diff_date_creatine',
        #'gfr_ckd_epi',        
        'diff_date_hba1c'
    ]

    for i in float_col:
        df[i] = df[i].astype(str)
        df[i] = df[i].replace(' ','')
        df[i] = pd.to_numeric(df[i], errors='coerce')


    # Estandarización columnas tipo texto

    cat_col = [
        #'document_number',
        'full_name',
        'phone_number', 
        'phone_number_two',
        'hta', 
        'dm', 
        'category_ta', 
        'hta_controlled',
        'category_weight', 
        'last_hba1c_range', 
        'hba1c_controlled',
        'previous_hba1c_range', 
        'tracking_hba1c',
        'ldl_controlled', 
        'stage_cockroft_gault', 
        'nursing_professional', 
        'mega', 
        'insulinized',
        'antihyp_med',
        'oral_med_dm',
        'insulinized_med_dm',
        'glucometer',
        'insulin_pump',
        'creatine_monitoring',
        'beneficiary_type',
        'lifetime',
        'risk_stratification',
        'diabetic_foot_exam',
        'diabetic_foot_grade',
        #'stage_ckd_epi',  
        #'rcv', 
        #'erc', 
        'provider'
    ]

    for i in cat_col:
        df[i] = df[i].astype(str)
        df[i] = normalize_str_categorical(df[i].astype(str).str.replace('.0',''))
        df[i] = df[i].str.strip()
    
    # Estandarización columnas fecha
    date_col = [
        'birth_date',
        'height_date', 
        'weight_date', 
        'last_atention_date',
        'date_last_ta', 
        'date_last_creatine', 
        'date_last_hba1c', 
        'date_last_tg',
        'period',
        'date_previous_creatine'
    ]
    for i in date_col:
        df[i] = df[i].astype(str)
        df[i] = pd.to_datetime(df[i], errors='coerce')


    # Se agregan columnas faltantes del dataframe "dim_patients" del datawarehouse

    missing_columns = ['mail','home_address','contact_name','contact_name','relationship','phone_contact']

    for i in missing_columns:
        df[i] = ''
        print('lo hizo perfecto missing_columns: ', i)
    
    print("df total impreso: ", df.dtypes)
    print("df columnas totales: ", df.columns)

    
    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_fact_rcv ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_data(path)


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

    df_patients = df_patients.drop_duplicates(subset=['document_type', 'document_number'])

    print(df_patients)
    print(df_patients.dtypes)
    print(df_patients.columns)

    for column in df_patients:
        print(column,"->", df_patients[column].astype(str).str.len().max())

    df_rcv = df[
        [
        'document_type', 
        'document_number',
        'hta', 
        'dm', 
        'height',
        'height_date', 
        'weight', 
        'bmi', 
        'weight_date', 
        'last_atention_date',
        'last_tas', 
        'last_tad', 
        'date_last_ta', 
        'category_ta', 
        'hta_controlled',
        'category_weight', 
        'last_creatine', 
        'date_last_creatine', 
        'last_hba1c',
        'date_last_hba1c', 
        'last_hba1c_range',
        'hba1c_controlled',
        'previous_hba1c', 
        'previous_hba1c_range', 
        'difference_hba1c',
        'tracking_hba1c', 
        'last_microalbuminuria',
        'last_ratio_albuminuria_creatinuria', 
        'last_ct', 
        'last_ldl',
        'ldl_controlled', 
        'last_tg', 
        'date_last_tg', 
        'gfr_cockroft_gault',
        'stage_cockroft_gault', 
        'nursing_professional', 
        'mega', 
        'period',
        'insulinized',
        'antihyp_med',
        'oral_med_dm',
        'insulinized_med_dm',
        'glucometer',
        'insulin_pump',
        'previous_creatine',
        'date_previous_creatine',
        'diff_creatine',
        'diff_date_creatine',
        'creatine_monitoring',
        'diff_date_hba1c',
        'beneficiary_type',
        'lifetime',
        'risk_stratification',
        'diabetic_foot_exam',
        'diabetic_foot_grade',
        #'gfr_ckd_epi', 
        #'stage_ckd_epi',
        #'rcv',
        #'erc',
        'provider'
        ]
    ]

    for column in df_rcv:
        print(column,"->", df_rcv[column].astype(str).str.len().max())

    # eliminación de los registros repetidos condicionados a las llaves del store procedure
    df_rcv = df_rcv.drop_duplicates(subset=['document_type' ,'document_number', 'nursing_professional' ,'mega' ,'period'])

    print(df_rcv)
    print(df_rcv.dtypes)
    print(df_rcv.columns)

    # cargue de datos a las tablas temporales
    if ~df.empty and len(df_patients.columns) >0:
        load_df_to_sql(df_patients, db_tmp_table_dim, sql_connid)

    if ~df.empty and len(df_rcv.columns) >0:
        load_df_to_sql(df_rcv, db_tmp_table_fact, sql_connid)



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
    # Se establece el cargue de los datos el primer día de cada mes.
    schedule_interval= '40 4 6 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_fact_rcv_python_task = PythonOperator(
        task_id = "get_fact_rcv",
        python_callable = func_get_fact_rcv,
        email_on_failure=True,
        email='BI@clinicos.com.co',
        dag=dag,
    )
    # Se declara la función encargada de ejecutar el "Stored Procedure" de dim_patients
    load_dim_patient = MsSqlOperator(
        task_id='Load_dim_patient',
        mssql_conn_id=sql_connid,
        autocommit=True,
        sql="EXECUTE sp_load_dim_patients",
        email_on_failure=True,
        email='BI@clinicos.com.co',
        dag=dag
    )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure" de 
    load_fact_rcv = MsSqlOperator(
        task_id='Load_fact_rcv',
        mssql_conn_id=sql_connid,
        autocommit=True,
        sql="EXECUTE sp_load_fact_rcv",
        email_on_failure=True,
        email='BI@clinicos.com.co',
        dag=dag,
    )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_fact_rcv_python_task >> load_dim_patient >> load_fact_rcv >> task_end