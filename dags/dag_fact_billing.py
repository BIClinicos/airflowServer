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
filename = 'Cuadro_mando.xlsx'
db_table = 'fact_billing'
db_tmp_table = 'tmp_fact_billing'
dag_name = 'dag_' + db_table
container_to = 'historicos'

# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def transform_tables(path):
    
    df = pd.read_excel(path, sheet_name=0, header=4) #se abre el excel, la primera página, utilizando cómo columnas la fila numero 4
    df = df.rename({'Unnamed: 3' : 'CLASIFICACIÓN'}, axis=1)
    df = df.drop(index=(0)) # Se modifica el dataframe eliminando la fila con el indice 0 qué no aporta ningún tipo de información valiosa y posee valores nulos 

    print("df",df)
    print("df.dtypes",df.dtypes)
    # Estandarización de los nombres de columnas del dataframe


    #Fill NAN with last valid value
    #El objetivo de esta función es hacer un fill de valores nulos en la columna de EPS, llenando con un forward fill con el último valor válido del dataframe
    #Esto permite llenar las filas vacías en la columna de EPS con el valor correspondiente 
    df['EPS'].fillna(method='ffill', inplace=True)
    
    # FORMAT COLUMNS
    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)

    print(df.columns)


    #Aquí se establecen las columnas desde la 5 en adelante, qué corresponden a las columnas de fecha, estas van a ir al argumento value_vars de la función pd.melt
    df_m = df.columns[5:]

    #Aquí citamos la función melt qué posee id_vars (qué son las variables a preservar), value_vars qué son las variables a realizarles Unpivot, el nombre de la columna
    # a crear qué en este caso sería 'Fecha' y los valores qué serán ubicados en la columna 'Monto'
    
    print(df.columns)

    
    df = pd.melt(df.reset_index(), id_vars=["nit","eps","servicio","clasificacion","tipo_contrato"], value_vars=df_m, var_name='fecha', value_name='monto')

    # FORMAT AND REPAIR DATES

    #Primero eliminamos la porción string qué dice FACTURACION de la columna de fecha, para poder procesarla
    df['fecha'] = df['fecha'].str.lstrip('facturacion')

    # Extract years and month from 'fecha'
    years = df["fecha"].str[-4:]
    month_name = df["fecha"].str[0:-4]

    years = normalize_str_categorical(years)
    month_name = normalize_str_categorical(month_name).str.replace('_','',regex=False)

    # la siguiente pieza del código realiza un reemplazo por medio de la función map, identifica las palabras de los meses y las reemplaza por el primero de cada més
    months = month_name.map(
        {
            "ENERO" : "01/01",
            "FEBRERO" : "01/02",
            "MARZO" : "01/03",
            "ABRIL" : "01/04",
            "MAYO" : "01/05",
            "JUNIO" : "01/06",
            "JULIO" : "01/07",
            "AGOSTO" : "01/08",
            "SEPTIEMBRE" : "01/09",
            "OCTUBRE" : "01/10",
            "NOVIEMBRE" : "01/11",
            "DICIEMBRE" : "01/12",
        }
    )

    df_date = months + '/' + years
    df["fecha"] = df_date
    df["fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y", errors = 'coerce')

    date_columns = ['fecha']

    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        df[i] = pd.to_datetime(df[i], format="%Y-%m-%d", errors = 'coerce')


    print('df.columns 1', df.columns)

    #Esta celda ejecuta las funciones de normalización de columnas

    df = df[['nit','eps','fecha','servicio','clasificacion','tipo_contrato','monto']]

    dict = {'nit':'nit','eps':'client_name','fecha':'date','servicio':'service','clasificacion':'classification','tipo_contrato':'type_contract','monto':'billing_value'}

    df.rename(dict,axis='columns', inplace=True)
    
    df['classification'] = normalize_str_categorical(df['classification'])

    print('df after rename',df)
    print('df after rename',df.columns)
    
    
    df['classification'] = df['classification'].map({ 'NUEVO PROGRAMA':'NUEVOS PROGRAMAS','NUEVA REGION':'NUEVAS REGIONES','NUEVO CONTRATO':'NUEVOS CONTRATOS'})

    # CLEANING
    df = df.dropna(subset=['date'])
    df = df[df['client_name']!='TOTALES']

    print('df after cleaning',df)
    print('df after cleaning',df.columns)

    # NIT FULL REPAIR (MAY BE NAN VALUES IN SORUCE FILE, SO USE FILLNA WITH FFILL METHOD IS INCORRECT)
    nits = df['nit'].dropna().astype(str).str.replace('.0','',regex=False)
    df['nit'] =  df['nit'].astype(str).str.replace('.0','',regex=False)

    nits_array = nits.unique()

    filter_nit = df['nit'].isin(nits_array)

    df_unique_clients_nits = df[filter_nit].groupby(['nit','client_name']).count().reset_index()[['nit','client_name']]
    df_unique_clients_nits = df_unique_clients_nits.rename(columns={'nit':'nit_unique'})

    df_final = df.merge(df_unique_clients_nits, on='client_name', how='left')
    df_final['nit'] = df_final['nit_unique']

    normalize_cols = ['nit','client_name']
    for col in normalize_cols:
        df_final[col] = normalize_str_categorical(df_final[col])

    df_final[['nit', 'client_name', 'date', 'service', 'classification',
       'type_contract', 'billing_value']]

    print('df final',df_final)
    print('df final',df_final.columns)
       

    return df_final

def func_get_fact_billing():


    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    
    df = transform_tables(path)
    print('columnas',df.columns)
    print(df.dtypes)
    print(df.head(20))
    print(df)


    df = df[
        [
        'nit',
        'client_name',
        'date',
        'service',
        'classification',
        'type_contract',
        'billing_value',
        ]
    ]
 
    print('columnas de las tablas',df.columns) 

    df = df.drop_duplicates(subset=['client_name','date','service','type_contract'])

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)


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
    # Se establece la ejecución del dag mensual
    schedule_interval= '0 4 8 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    func_get_fact_billing_python_task = PythonOperator(task_id = "get_fact_billing",
                                                python_callable = func_get_fact_billing,
                                                #email_on_failure=True, 
                                                #email='BI@clinicos.com.co',
                                                #dag=dag,
                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_billing = MsSqlOperator(task_id='Load_fact_billing',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_fact_billing",
                                       #email_on_failure=True, 
                                       #email='BI@clinicos.com.co',
                                       #dag=dag,
                                       )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> func_get_fact_billing_python_task >> load_fact_billing >>task_end