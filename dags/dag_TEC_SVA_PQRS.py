import os
import xlrd
import re
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from numpy import nan
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pqrs'
dirname = '/opt/airflow/dags/files_pqrs/'
filename = 'TEC_SVA_PQRS.xlsx'
db_table = "TEC_SVA_PQRS"
db_tmp_table = "tmp_TEC_SVA_PQRS"
dag_name = 'dag_' + db_table
container_to = 'historicos'


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Edicion 2023-01-13. Ajuste de reglas de negocio, aplicar previa devolucion del df
# por la funcion transform_tables
def data_correction_pqrs(df):
    # Parametros hardcode para indicar las columnas requeridas
    age = 'edad' # Columnas de edad
    client = 'eps_paciente' # Columna de EPS
    unit = 'unidad' # Columna de unidad
    # Adicion de PRIMARIA a COMPENSAR o ALIANSALUD
    cond_1 = df[unit].str.contains('PRIMARIA') & df[client].str.contains(r'^(COMPENSAR$|ALIANSALUD$).*', regex = True)
    df.loc[cond_1, client] += (' PRIMARIA')    
    # Correccion de edad
    cond_2 = (df[age] == 0)
    df.loc[cond_2, age] = nan
    # Correccion gestion farmaceutica
    #cond_3 = df[unit].str.contains('GESTIÓN FARMACEUTICA', na=False)
    #cond_4 = df[client].str.contains(r'.*DOMICILIARIO$', regex = True, na=False)
    #df.loc[cond_3 & cond_4, unit] = 'DOMICILIARIA'
    #df.loc[cond_3 & ~cond_4, unit] = 'ESPECIALIZADA'
    # Correccion otros
    cond_5 = df[client].str.contains('OTROS', na=False)
    cond_6 = df[unit].str.contains('DOMICILIARIA', na=False)
    df.loc[cond_5 & cond_6, client] = 'OTROS DOMICILIARIA'
    df.loc[cond_5 & ~cond_6, unit] = 'OTROS ESPECIALIZADA'
    # Retorno de df
    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def transform_tables (path):

    # lectura del dataframe desde el archivo csv
    # df = pd.read_csv(path, sep = ";")
    # 20221123 lectura de la segunda hoja
    try:
        df = pd.read_excel(path, sheet_name=1)
    except ValueError:
        df = pd.read_excel(path)

    print("Como está leyendo el dataframe inicialmente",df)
    print("Nombres y tipos de columnas leídos del dataframe sin transformar",df.dtypes)


    # Estandarización de los nombres de columnas del dataframe
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('.',' ')
    df.columns = df.columns.str.replace('/',' ')
    df.columns = df.columns.str.replace('   ',' ')
    df.columns = df.columns.str.replace('  ',' ')
    df.columns = df.columns.str.replace(' ','_')
    df.columns = df.columns.str.replace('á','a')
    df.columns = df.columns.str.replace('é','e')
    df.columns = df.columns.str.replace('í','i')
    df.columns = df.columns.str.replace('ó','o')
    df.columns = df.columns.str.replace('ú','u')
    df.columns = df.columns.str.replace('ñ','ni')
    df.columns = df.columns.str.replace('\n.*','')
    df.columns = df.columns.str.replace('#','no')
    df.columns = df.columns.str.replace('_\([^)]*\)','')

    # 20221020 Campo canal_de_recepcion nunca tiene nulos, se usa para eliminar entradas vacias
    df = df.dropna(subset=['canal_de_recepcion'])

    # df = df.drop(['idpqrs'], axis=1)
    # Eliminar todos los registros que no sean sí o no en la columna de "oportuna"

    df['oportuna'] = df['oportuna'].astype('str')
    df['oportuna'] = df['oportuna'].str.upper()
    df['oportuna'] = df['oportuna'].str.replace('SI','OPORTUNA')
    df_3 = df[df.oportuna == 'OPORTUNA']
    df_4 = df[df.oportuna == 'NO OPORTUNA']
    df = df_3.append(df_4, ignore_index=True)
    # Eliminar todos los registros que no sean sí o no en la columna de "atribuible"

    df['atribuible'] = df['atribuible'].astype(str)
    df['atribuible'] = df['atribuible'].str.upper()
    df_1 = df[df.atribuible == 'SI']
    df_2 = df[df.atribuible == 'NO']
    df = df_1.append(df_2, ignore_index=True)

    # reemplazando los valores nulos por strings vacíos

    if 'cc' in df.columns:
        df['cc'] = df['cc'].astype(str)
        df['cc'] = df['cc'].str.upper()
    else:
        df['tipo_de_documento'] = df['tipo_de_documento'].astype(str)
        df['cc'] = df['tipo_de_documento'].str.upper()

    df['no_documento'] = df['no_documento'].fillna('')

    if 'observaciones' in df.columns:
        df['observacion'] = df['observaciones']

    # Estandarización de las columnas de fechas a tipo fecha

    date_columns = ['fecha_recepcion','fecha_maxima_de_respuesta','fecha_de_respuesta_preliminar','fecha_respuesta']

    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        df[i] = pd.to_datetime(df[i], format="%Y-%m-%d", errors = 'coerce')

    # df['fecha_recepcion'] = pd.to_datetime(df['fecha_recepcion'], format="%Y-%m-%d", errors = 'coerce')
    
    # df['fecha_maxima_de_respuesta'] = pd.to_datetime(df['fecha_maxima_de_respuesta'], format="%Y-%m-%d", errors = 'coerce')

    # df['fecha_de_respuesta_preliminar'] = df['fecha_de_respuesta_preliminar'].astype(str)
    # df['fecha_de_respuesta_preliminar'] = df['fecha_de_respuesta_preliminar'].str.strip()
    # df['fecha_de_respuesta_preliminar'] = pd.to_datetime(df['fecha_de_respuesta_preliminar'], format="%Y-%m-%d", errors = 'coerce')
    
    # df['fecha_respuesta'] = df['fecha_respuesta'].astype(str)
    # df['fecha_respuesta'] = df['fecha_respuesta'].str.strip()
    # df['fecha_respuesta'] = pd.to_datetime(df['fecha_respuesta'], format="%Y-%m-%d", errors = 'coerce')

    # Estandarización de las columna de edad
    df['edad'] = df['edad'].fillna('')
    df['edad'] = df['edad'].astype(str)
    df['edad'] = df['edad'].str.strip()
    df.loc[df['edad'].str.contains('mes'), 'edad'] = '0'   
    df.loc[df['edad'].str.contains('MES'), 'edad'] = '0'    
    df['edad'] = df['edad'].str.replace(r'[a-zA-Z%]', '', regex=True)
    df['edad'] = df['edad'].str.replace('Ñ', '')
    df['edad'] = df['edad'].str.replace('ñ', '')
    df['edad'] = df['edad'].str.strip()

    df['edad'] = pd.to_numeric(df['edad'], downcast='integer', errors = 'coerce')

    # Estandarización de las columna de telefono

    df['telefonos'] = df['telefonos'].fillna('')
    df['telefonos'] = df['telefonos'].astype(str)
    df['telefonos'] = df['telefonos'].str.strip()
    df['telefonos'] = df['telefonos'].str.replace(r'[a-zA-Z%]', '', regex=True)

    df['tiempo_maximo_respuesta'] = pd.to_numeric(df['tiempo_maximo_respuesta'], downcast='integer')

    if 'alerta' in df.columns:
        df['alerta'] = df['alerta'].str.upper()
    else:
        df['alerta'] = df['alerta_de_vencimiento'].str.upper()

    # Reemplazo de INCAPACIDAD en INCAPACIDADES y TRATO INADECUADO DEL APCIENTE por TRATO INADECUADO DEL PACIENTE en la columna especialidad_area_proceso

    df['especialidad_area_proceso'] = df['especialidad_area_proceso'].astype(str)
    df['especialidad_area_proceso'] = df['especialidad_area_proceso'].str.upper()
    df['especialidad_area_proceso'] = df['especialidad_area_proceso'].replace({'INCAPACIDAD':'INCAPACIDADES', 'TRATO INADECUADO DE APCIENTE ': 'TRATO INADECUADO DEL PACIENTE'})

    df['fecha_operacion'] = pd.to_datetime(date.today())
    df['contrato'] = ''
    df['cruce'] = ''

    # df.loc[df['edad'].str.lower().str.contains('mes') == True, 'edad'] = 0

    # Remplazo de columnas 2023-04
    dict_replace = {'eps':'peticionario'}
    df.rename(columns = dict_replace, inplace = True)
    print(df.columns)
    df = df[['canal_de_recepcion', 'peticionario', 'oys',
       'nombres_y_apellidos_paciente', 'cc', 'no_documento',
       'edad', 'telefonos', 'correo', 'eps_paciente', 'unidad', 'ciudad',
       'clasificacion_pqrs', 'motivo', 'especialidad_area_proceso',
       'descripcion', 'atributos_de_calidad', 'fecha_recepcion',
       'tiempo_maximo_respuesta', 'fecha_maxima_de_respuesta',
       'responsable_asignado', 'fecha_de_respuesta_preliminar',
       'alerta', 'atribuible', 'responsable_respuesta', 'respuesta', 
       'fecha_respuesta', 'oportuna', 'contrato', 'observacion', 'cruce',
       'fecha_operacion']]

    df = data_correction_pqrs(df)

    return df
    
    
def func_get_TEC_SVA_PQRS ():    

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    print(df.columns)
    print(df.dtypes)
    print(df.head(20))
    print(df)

    df = df.drop_duplicates(subset=['canal_de_recepcion', 'peticionario', 'oys',
       'nombres_y_apellidos_paciente', 'cc', 'no_documento', 'edad',
       'telefonos', 'correo', 'eps_paciente', 'unidad', 'ciudad',
       'clasificacion_pqrs', 'motivo', 'especialidad_area_proceso',
       'descripcion', 'atributos_de_calidad', 'fecha_recepcion',
       'tiempo_maximo_respuesta', 'fecha_maxima_de_respuesta',
       'responsable_asignado', 'fecha_de_respuesta_preliminar', 'alerta',
       'atribuible', 'responsable_respuesta', 'respuesta', 'fecha_respuesta',
       'oportuna', 'observacion'])
    
    print(df)


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
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '20 10 * * fri',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_TEC_SVA_PQRS_python_task = PythonOperator(task_id = "get_TEC_SVA_PQRS",
                                                  python_callable = func_get_TEC_SVA_PQRS,
                                                  email_on_failure=True, 
                                                  email='BI@clinicos.com.co',
                                                  dag=dag,
                                                  )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_TEC_SVA_PQRS = MsSqlOperator(task_id='Load_TEC_SVA_PQRS',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_TEC_SVA_PQRS",
                                       email_on_failure=True, 
                                       email='BI@clinicos.com.co',
                                       dag=dag,
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_TEC_SVA_PQRS_python_task >> load_TEC_SVA_PQRS >> task_end