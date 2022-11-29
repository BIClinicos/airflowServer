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
from pandas import read_excel, read_html
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,normalize_str_categorical
# from bs4 import BeautifulSoup

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pipeline'
dirname = '/opt/airflow/dags/files_pipeline/'
filename = 'Nuevos_Contratos.xlsx'
filename2= 'Cuadro_mando.xlsx'
db_table = 'fact_new_contracts'
db_table2 = 'fact_new_contracts_details'
db_tmp_table = 'tmp_fact_new_contracts'
db_tmp_table2 = 'tmp_fact_new_contracts_details'
dag_name = 'dag_' + db_table
container_to = 'historicos'

# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def transform_tables1(path):
    
    df = pd.read_excel(path, sheet_name=0) #se abre el excel

    print("df",df)
    print("df.dtypes",df.dtypes)
    # Estandarización de los nombres de columnas del dataframe

    print(df.columns)

    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)
    df.columns
    #Esta celda ejecuta las funciones de normalización de columnas
    
    print('columnas del dataframe',df.columns)


    # df = df[['nit', 'programa', 'poblacion_promedio', 'zona','servicios', 'departamento', 'ciudad',
    #         'inicio_facturacion(dd_mm_yyyy)', 'unidad', 'ingreso_percapita','ingreso_mes', 'ingreso_base', 'clasificacion', 
    #         'tipo_negocio']]

    df = df[['nit', 'cliente', 'programa', 'poblacion_promedio', 'zona', 'departamento', 'ciudad',
            'inicio_facturacion(dd_mm_yyyy)', 'unidad', 'ingreso_percapita','ingreso_base','ingreso_mes', 'clasificacion', 
            'tipo_negocio']]

    # dict = {'nit':'nit', 'programa':'name', 'poblacion_promedio':'average_population','zona':'zone',
    # 'departamento':'department','ciudad':'city','inicio_facturacion(dd_mm_yyyy)':'billing_date',
    # 'unidad':'unit','ingreso_percapita':'percapita_income','ingreso_mes':'percapita_income',
    # 'ingreso_mes':'month_income','ingreso_base':'base_income','clasificacion':'classification',
    # 'tipo_negocio':'bussiness_type'}

    # df.rename(dict,axis='columns', inplace=True)

    dict = {'nit':'nit',
    'cliente':'client_name',
    'programa':'program_name',
    'poblacion_promedio':'average_population',
    'zona':'zone',
    'departamento':'department',
    'ciudad':'city',
    'inicio_facturacion(dd_mm_yyyy)':'billing_date',
    'unidad':'unit',
    'ingreso_percapita':'percapita_income',
    'ingreso_base':'base_income',
    'ingreso_mes':'month_income',
    'clasificacion':'classification',
    'tipo_negocio':'bussiness_type'}

    df = df.rename(dict,axis='columns')
    df['classification'] = normalize_str_categorical(df['classification'])
    df['program_name'] = normalize_str_categorical(df['program_name'])
    df['program_name'] = remove_accents_cols(df['program_name'])

    return df

def transform_tables2(path):

    df2 = pd.read_excel(path, sheet_name=1)

    df2.columns = remove_accents_cols(df2.columns)
    df2.columns = remove_special_chars(df2.columns)
    df2.columns = regular_snake_case(df2.columns)

    print('columns 2',df2.columns)
    #Esta celda ejecuta las funciones de normalización de columnas

    df2 = df2[['nit', 'cliente', 'programa', 'servicios', 'cantidades']]


#     #Este segundo diccionario nos permite hacer el rename de las columnas en el segundo dataframe
    dict2 = {'nit':'nit',
    'cliente':'client_name',
    'programa':'program_name',
    'servicios':'service',
    'cantidades':'quantity'
    }


    df2.rename(dict2,axis='columns', inplace=True)
    df2['program_name'] = normalize_str_categorical(df2['program_name'])
    df2['program_name'] = remove_accents_cols(df2['program_name'])
    
    return df2


def func_get_fact_new_contracts():

    path = dirname + filename
    # print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables1(path)
    print('columns',df.columns)
    # print(df.dtypes)
    # print(df.head(20))
    # print(df)
    
    # query = f"""
    #     Select *
    #     from
    #         dbo.fact_billing
    # """
    # df3 = sql_2_df(query, sql_conn_id=sql_connid)
    # print('DF3 huy zonas',df3.columns)
    # print(df3.dtypes)
    # print(df3)


    df2 = transform_tables2(path)
    # #Agregamos las columnas ininico facturación y clasificación al dataframe2 (details) por medio de un merge con Pandas
    print('columns df2',df2.columns)
    df2 = pd.merge(df2,df[['nit','billing_date','classification']],on='nit', how='left')
    print('columns df2 merge',df2.columns)

    date_columns = ['billing_date']

    for i in date_columns:
        df2[i] = df2[i].astype(str)
        df2[i] = df2[i].str.strip()
        df2[i] = pd.to_datetime(df2[i], format="%Y-%m-%d", errors = 'coerce')
    # print(df2.dtypes)
    # print(df2.head(20))
    # print(df2)



    df2 = df2[
        [
        'nit',
        'client_name',
        'program_name',
        'service',
        'quantity',
        'billing_date',
        'classification'
        ]
    ]
 
    # print('columnas de las tablas',df.columns, df2.columns) 


    df = df.drop_duplicates(subset=['nit','program_name','billing_date','month_income'])

    df2 = df2.drop_duplicates(subset=['nit','program_name', 'service'])
    print(df2.columns)
    print('df2',df2)

    # LOAD TO DB
    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)

    if ~df2.empty and len(df2.columns) >0:
        load_df_to_sql(df2, db_tmp_table2, sql_connid)


# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}
# Se declara el DAG con sus respectivos parámetros
with DAG(
    dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= None,
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    func_get_fact_new_contracts_python_task = PythonOperator(task_id = "get_fact_new_contracts",
                                                python_callable = func_get_fact_new_contracts,
                                                #email_on_failure=True, 
                                                #email='BI@clinicos.com.co',
                                                #dag=dag,
                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_new_contracts = MsSqlOperator(task_id='Load_fact_new_contracts',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_fact_new_contracts",
                                       #email_on_failure=True, 
                                       #email='BI@clinicos.com.co',
                                       #dag=dag,
                                       )

    load_fact_new_contracts_details = MsSqlOperator(task_id='Load_fact_new_contracts_details',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_fact_new_contracts_details",
                                       #email_on_failure=True, 
                                       #email='BI@clinicos.com.co',
                                       #dag=dag,
                                       )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

# start_task >> func_get_fact_new_contracts_python_task >> load_fact_new_contracts >> task_end
start_task >> func_get_fact_new_contracts_python_task >> load_fact_new_contracts >> load_fact_new_contracts_details >> task_end