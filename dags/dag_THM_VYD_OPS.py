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
import numpy as np
from pandas import read_excel
from variables import sql_connid
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_contains_name,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'rotacionpersonas'
dirname = '/opt/airflow/dags/files_rotacion_personas/'
filename = 'THM_VYD_OPS.xlsx'
db_table = "THM_VYD_OPS"
db_tmp_table = "tmp_THM_VYD_OPS"
dag_name = 'dag_' + db_table
container_to = 'historicos'


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_table(path):


    df = pd.read_excel(path, header = [2], sheet_name = "OPS")
    

    df_col_adi = df.columns[24:]
    df = df.drop(df_col_adi ,axis=1)

    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ','_')
    df.columns = df.columns.str.replace('-','_')
    df.columns = df.columns.str.replace('\n','_')
    df.columns = df.columns.str.replace('á','a')
    df.columns = df.columns.str.replace('é','e')
    df.columns = df.columns.str.replace('í','i')
    df.columns = df.columns.str.replace('ó','o')
    df.columns = df.columns.str.replace('ú','u')
    df.columns = df.columns.str.replace('ñ','ni')


    df['especialidad'] = df['especialidad'].str.upper()


    df['inicio_contrato'] = df['inicio_contrato'].astype(str)
    df['inicio_contrato'] = df['inicio_contrato'].str.strip()
    # df['inicio_contrato'] = df['inicio_contrato'].replace(' ', '')
    df['inicio_contrato'] = df['inicio_contrato'].apply(lambda x: x.replace("/","-"))
    df['inicio_contrato'] = df['inicio_contrato'].apply(lambda x: x.replace(" 00:00:00",""))
    df['inicio_contrato'] = df['inicio_contrato'].apply(lambda x: x.replace("19-06-2019","2019-06-19"))
    df['inicio_contrato'] = pd.to_datetime(df['inicio_contrato'], format="%Y-%m-%d")

    print(df['inicio_contrato'].head(487))


    # df['fecha_verificacion_titulo'] = df['fecha_verificacion_titulo'].str.strip()
    df['fecha_verificacion_titulo'] = df['fecha_verificacion_titulo'].replace('...', '')
    df['fecha_verificacion_titulo'] = df['fecha_verificacion_titulo'].replace('.', '')
    # df['fecha_verificacion_titulo'] = pd.to_datetime(df['fecha_verificacion_titulo'], format="%d-%m-%Y", errors='coerce')

    date_columns = ['inicio_contrato','fecha_nacimiento','fecha_verificacion_titulo','fecha_retiro']

    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        df[i] = pd.to_datetime(df[i], format="%Y-%m-%d", errors = 'coerce')

    print(df['fecha_verificacion_titulo'].head(50))


    df['sede'] = df['sede'].str.lower()
    df['sede'] = df['sede'].fillna('')
    df.loc[df['sede'].str.contains('américas'), 'sede'] = 'americas'
    df.loc[df['sede'].str.contains('domiciliaria'), 'sede'] = 'americas'
    df.loc[df['sede'].str.contains('teleorientación'), 'sede'] = 'teleorientación'
    df.loc[df['sede'].str.contains('remoto'), 'sede'] = 'trabajo en casa'
    df.loc[df['sede'].str.contains('remota'), 'sede'] = 'trabajo en casa'
    df.loc[df['sede'].str.contains(','), 'sede'] = 'varios'
    df.loc[df['sede'].str.contains(';'), 'sede'] = 'varios'
    df.loc[df['sede'].str.contains(' y '), 'sede'] = 'varios'
    df['sede'] = df['sede'].str.capitalize()

    print(df['sede'].unique())

    if 'unidad_para_informe_mensual' not in df.columns:
        df['unidad_para_informe_mensual'] = df['unidad_de_negocio']

    df['unidad_para_informe_mensual'] = df['unidad_para_informe_mensual'].fillna('')
    df.loc[df['unidad_para_informe_mensual'].str.contains('Administrativa'), 'unidad_para_informe_mensual'] = 'Unidad Administrativa'
    df.loc[df['unidad_para_informe_mensual'].str.contains('Gerencia'), 'unidad_para_informe_mensual'] = 'Unidad Administrativa'
    df.loc[df['unidad_para_informe_mensual'].str.contains('Proyecto Merk'), 'unidad_para_informe_mensual'] = 'Unidad Administrativa'
    df.loc[df['unidad_para_informe_mensual'].str.contains('Domiciliaria'), 'unidad_para_informe_mensual'] = 'Unidad Domiciliaria'
    df.loc[df['unidad_para_informe_mensual'].str.contains('Primaria'), 'unidad_para_informe_mensual'] = 'Unidad Primaria'
    df.loc[df['unidad_para_informe_mensual'].str.contains('Especializada'), 'unidad_para_informe_mensual'] = 'Unidad Especializada'
    df['unidad_para_informe_mensual'] = df.apply(lambda x: x['unidad_de_negocio'] if (x['unidad_para_informe_mensual'] == '') else x['unidad_para_informe_mensual'], axis = 1)

    print(df['unidad_para_informe_mensual'])
    print(df['unidad_para_informe_mensual'].unique())


    df['valor_contrato'] = df['valor_contrato'].fillna('')
    df['valor_contrato'] = df['valor_contrato'].astype(str)
    df_valor = df['valor_contrato'].str.split(' /', n=1, expand = True)
    df['valor_contrato'] = df_valor[0]
    df['valor_contrato'] = df['valor_contrato'].apply(lambda x: x.replace("$ ",""))
    df['valor_contrato'] = pd.to_numeric(df['valor_contrato'])


    print(df_valor.head(20))
    print(df['valor_contrato'].head(20))
    print(df['valor_contrato'].dtypes)


    df['tipo_cuenta'] = df['tipo_cuenta'].fillna('')
    df.loc[df['tipo_cuenta'].str.contains('Compensar'), 'tipo_cuenta'] = ''

    print(df['tipo_cuenta'].head(20))
    print(df['tipo_cuenta'].unique())

    df = df[['tipo_id','id','nombre_completo','especialidad','sede','inicio_contrato',
        'valor_contrato','unidad_de_negocio','unidad_para_informe_mensual',
        'fecha_nacimiento','lugar_expedicion_id','rh','correo','telefono','direccion',
        'banco','numero_cuenta','tipo_cuenta','eps','pension','arl',
        'fecha_verificacion_titulo','estado_activo_inactivo_retirado','fecha_retiro',
        'observaciones']]   

    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_THM_VYD_OPS ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_table(path)
    print(df.columns)
    print(df.dtypes)
    print(df.head(30))

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_table, sql_connid)


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
    # Se establece la ejecución del dag todos los viernes a las 12:40 am(Hora servidor)
    schedule_interval= '40 10 * * fri',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_THM_VYD_OPS_python_task = PythonOperator(task_id = "get_THM_VYD_OPS",
                                                python_callable = func_get_THM_VYD_OPS,
                                                email_on_failure=True, 
                                                email='BI@clinicos.com.co',
                                                dag=dag
                                                )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_THM_VYD_OPS_python_task >> task_end