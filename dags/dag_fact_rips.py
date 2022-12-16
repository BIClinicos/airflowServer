import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime, timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from pandas import read_excel
from variables import sql_connid
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get, normalize_str_categorical, remove_accents_cols, remove_special_chars, regular_camel_case, regular_snake_case

# Parámetros de nombre de archivo leido en la carpeta para mes y año

# Fecha de ejecución del dag
today = date.today()
nextmonthdate = today - relativedelta(months=1)
nextmonth = nextmonthdate.month
month = today.month
year = today.year
x = len(str(nextmonth))

if x < 2:
    nextmonth = "0" + str(nextmonth)

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'rcv'
dirname = '/opt/airflow/dags/files_rcv/'
filename = f'AC{nextmonth}{year}.txt'
db_table_fact = "fact_rips"
db_tmp_table_fact = "tmp_fact_rips"
dag_name = 'dag_' + db_table_fact


# Función de transformación de los archivos xlsx
def transform_data (path):

    # lectura del archivo de excel
    #df = pd.read_csv(path,sep='\t',header=None)
    df = pd.read_csv(path, sep=",", delimiter=",", header=None)
    print(df)

    columns = [
        'invoice_no',
        'habilitation_code', 
        'document_type',
        'document_number',
        'appointment_date',
        'numero_autorizacion',
        'cups',
        'appointment_purpose',
        'causa_externa',
        'diagnostic_code',
        'cod_dx_rel_1',
        'cod_dx_rel_2',
        'cod_dx_rel_3',
        'dx_type',
        'valor_consulta',
        'valor_cuota_moderadora',
        'valor_neto_pagar'
    ]

    df.columns = columns

    df = df[
        [
            'invoice_no',
            'habilitation_code', 
            'document_type',
            'document_number',
            'appointment_date',
            'cups',
            'appointment_purpose',
            'diagnostic_code',
            'dx_type'
        ]
    ]

    # columna habilitation_code
    if np.issubdtype(df.habilitation_code,np.number):
      df['habilitation_code'] = df['habilitation_code'].astype(str)
      
    df['habilitation_code'] = df['habilitation_code'].astype(str)
    filter_exp = df['habilitation_code'].astype(str).str.contains('E+11',regex=False)
    df.loc[filter_exp,'habilitation_code'] = df['habilitation_code'].str.replace('E+11','',regex=False)
    df.loc[filter_exp,'habilitation_code'] = df['habilitation_code'].str.replace(',','.',regex=False)
    df.loc[filter_exp,'habilitation_code'] = (pd.to_numeric(df['habilitation_code'])*(10**11)).astype(int)
    df['habilitation_code'] = df['habilitation_code'].astype(str)
    df['habilitation_code'] = df['habilitation_code'].str[:-2]


    # columna document_number

    df['document_number'] = df['document_number'].astype(str)

    # columna appointment_date

    df['appointment_date'] = pd.to_datetime(df['appointment_date'], errors='coerce')

    # columna appointment_purpose

    df['appointment_purpose'] = df['appointment_purpose'].astype(str)

    # columna dx_type

    df['dx_type'] = df['dx_type'].astype(str)


    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_fact_rips ():

    path = dirname + filename
    file_get(path,container,filename, wb = wb)
    df = transform_data(path)

    df = df.drop_duplicates(
        subset=[
            'invoice_no',
            'habilitation_code', 
            'document_type',
            'document_number',
            'appointment_date',
            'cups',
            'appointment_purpose',
            'diagnostic_code',
            'dx_type'
        ]
    )


    print(df)
    print(df.dtypes)

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table_fact, sql_connid)


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
    schedule_interval= '30 4 6 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_fact_rips_python_task = PythonOperator(
        task_id = "get_fact_rips",
        python_callable = func_get_fact_rips,
        email_on_failure=True,
        email='BI@clinicos.com.co',
        dag=dag,
    )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure" de 
    load_fact_rips = MsSqlOperator(
        task_id='Load_fact_rips',
        mssql_conn_id=sql_connid,
        autocommit=True,
        sql="EXECUTE sp_load_fact_rips",
        email_on_failure=True,
        email='BI@clinicos.com.co',
        dag=dag,
    )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_fact_rips_python_task >>  load_fact_rips >> task_end