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
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_contains_name,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pami-ecopetrol'
dirname = '/opt/airflow/dags/files_pami/'
filenames = ['SabanaBulevar.xlsx', 'SabanaSanMartin.xlsx', 'SabanaCartagena.xlsx']
db_tables = ['SAL_PRI_sabanas']
db_tmp_table = 'TmpTblHSabanasPAMI'
dag_name = 'dag_TblHSabanasPAMI'
container_to = 'historicos'


# Se realiza un chequeo de la conexión al blob storage
def check_connection(container, filename):
    print('Conexión OK')
    return wb.check_for_blob(container,filename)

# Función de transformación de los archivos xlsx
def transform_table(dir, filenames):
    # Nombre columnas
    col_names = [
        'sede'
        ,'servicio'
		,'fecha_solicitud'
		,'fecha_inicio_cita'
		,'oportunidad'
		,'profesional'
		,'tipo_cita'
		,'tipo_id'
		,'numero_id'
		,'modalidad'
		,'estado_cita'
		,'diagnostico'
		,'CIE10'
		,'remitente'
		,'procedimientos'
    ]
    # Lectura y transformacion de df
    df_bulevar = pd.read_excel(dir + filenames[0], header = [0], sheet_name = 0)
    if 'Procedimientos' not in df_bulevar:
        df_cartagena['Procedimientos'] = None
    df_bulevar.columns = col_names
    #
    df_sm = pd.read_excel(dir + filenames[1], header = [0], sheet_name = 0)
    if 'Procedimientos' not in df_sm:
        df_sm['Procedimientos'] = None    
    df_sm.columns = col_names
    #  
    df_cartagena = pd.read_excel(dir + filenames[2], header = [0], sheet_name = 0)
    if 'Procedimientos' not in df_cartagena:
        df_cartagena['Procedimientos'] = None   
    df_cartagena.columns = col_names
    ### Concatenar
    df = pd.concat([df_bulevar, df_sm, df_cartagena])
    ### Cambiar de formato
    date_columns = ['fecha_solicitud','fecha_inicio_cita']
    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        #df[i] = pd.to_datetime(df[i], format="%Y-%m-%d", errors = 'coerce')
    ### Remplazo de servicios
    df['servicio'] = df['servicio'].str.upper()
    df['servicio'] = df['servicio'].str.replace('(CONSULTA PREFERENTE)+','')
    df['servicio'] = df['servicio'].str.replace('(Y OBSTETRICIA)+','')
    df['servicio'] = df['servicio'].str.replace('(CONSULTA PRIORITARIA)+','')
    df['servicio'] = df['servicio'].str.replace('TERAPIA DEL LENGUAJE|FONOAUDIOLOGIA','TERAPIA DE LENGUAJE')
    df['servicio'] = df['servicio'].str.replace('FISIOTERAPIA','TERAPIA FÍSICA')
    # Modificacion solicitada 2023-05-23
    cond_1 = df['servicio'].str.contains('TERAPIA FÍSICA') & df['tipo_cita'].str.contains('Primera vez')
    df.loc[cond_1, 'servicio'] = 'TERAPIA FÍSICA 1RA VEZ'
    df['servicio'] = df['servicio'].str.strip()
    print(df['servicio'].unique())
    ## df['servicio'] = df['servicio'].str.replace(r'[^\x00-\x7F]+','')
    ### Limpieza de registros sin id
    nreg = df['numero_id'].count()
    print(f'La directris de no NA en numero id dejo {nreg} registros fuera')
    df = df[df['numero_id'].notna()]   
    ### Limpieza tipo id
    df['tipo_id'] = df['tipo_id'].str.strip()
    df['tipo_id'] = df['tipo_id'].fillna('CC')
    df['tipo_id'] = df['tipo_id'].str.replace(r'[^\x00-\x7F]+','')
    df['tipo_id'] = df['tipo_id'].str[:2]
    #
    df['fecha_solicitud'] = df['fecha_solicitud'].fillna(df['fecha_inicio_cita'])
    ### Cambio estado cita
    dict_replace ={
        'Asistió':'ASISTIDO',
        'No asistió':'NO ASISTIDO',
        'Cancelada':'CANCELADO',
        'Reprogramada':'REPROGRAMADA'
    }
    df = df[df['profesional'].notna()]
    df = df.replace({'estado_cita':dict_replace})
    df['estado_cita'] = df['estado_cita'].fillna('ASISTIDO')
    ### Cambiar sede
    #dict_replace ={
    #    'Bulevar':'Clínicos IPS Sede Bulevar',
    #    'San Martin':'Clínicos IPS Sede San Martín',
    #    'Cartagnera':'Clínicos IPS Sede Cartagena'
    #}
    #df = df.replace({'sede':dict_replace})
    #
    print(df.info())
    print(df.head(5))
    return df


# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_TblHSabanasPAMI():
    #
    for filename in filenames:
        path = dirname + filename
        print(path)
        check_connection(container, filename)
        file_get(path, container,filename, wb = wb)
    #
    df = transform_table(dirname, filenames)
    print(df.columns)
    print(df.dtypes)
    print(df.tail(50))
    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)

    #for filename in filenames:
    #    move_to_history_contains_name(dirname, container, filename, container_to)


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
    schedule_interval= '0 0 11 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_tmp_TblHSabanasPAMI = PythonOperator(task_id = "Get_tmp_TblHSabanasPAMI",
                                                  python_callable = func_get_TblHSabanasPAMI,
                                                  email_on_failure=True, 
                                                  email='BI@clinicos.com.co',
                                                  dag=dag,
                                                  )
    
    # Carge de tabla de hechos
    load_TblHSabanasPAMI = MsSqlOperator(task_id ='Load_TblHSabanasPAMI',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblHSabanasPAMI",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )
    
    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_tmp_TblHSabanasPAMI >> load_TblHSabanasPAMI >> task_end