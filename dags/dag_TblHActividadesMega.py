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
filename = 'MEGA.xlsx'
db_tables = ['TblDMegas', 'TblDTipoMega', 'TblHActividadesMega']
db_tmp_table = 'TmpTblHActividadesMega'
dag_name = 'dag_TblHActividadesMega'
container_to = 'historicos'


# Se realiza un chequeo de la conexión al blob storage
def check_connection(container, filename):
    print('Conexión OK')
    return wb.check_for_blob(container,filename)

# Función de transformación de los archivos xlsx
def transform_table(dir, filename):
    # Lectura y transformacion de df
    df_mega1 = pd.read_excel(dir + filename, header = [0], sheet_name = 0)
    df_mega1 = pd.melt(frame = df_mega1, id_vars = ['Sede', 'Mes', 'MEGA'], var_name = 'detalle', value_name = 'cantidad')
    print(df_mega1.info())
    print(df_mega1.head(5))
    #df_mega1['cantidad'] = df_mega1['cantidad'].str.replace('NA', None)
    #df_mega1['cantidad'] = [x.str.rstrip("%").astype(float)/100 if '%' in x else x.astype(float) for x in df_mega1['cantidad']]
    df_mega1['concepto'] = 'Mega 1'
    df_mega1['tipo'] = 'No aplica'
    df_mega1.columns = ['sede', 'fecha', 'mega', 'detalle', 'cantidad', 'concepto', 'tipo']
    df_mega1 = df_mega1[['fecha', 'mega', 'tipo', 'concepto', 'detalle', 'sede', 'cantidad']]
    #
    df_mega2 = pd.read_excel(dir + filename, header = [0], sheet_name = 1)
    df_mega2 = pd.melt(frame = df_mega2, id_vars = ['MEGAS', 'MES', 'Tipo MEGA'], var_name = 'detalle', value_name = 'cantidad')
    df_mega2['concepto'] = 'Mega 2'
    df_mega2['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_mega2.columns = ['mega', 'fecha', 'tipo', 'detalle', 'cantidad', 'concepto', 'sede']
    df_mega2 = df_mega2[['fecha', 'mega', 'tipo', 'concepto', 'detalle', 'sede', 'cantidad']]
    #
    ### Concatenar
    df = pd.concat([df_mega1, df_mega2])
    ### Cambiar de formato
    df['fecha'] = pd.to_datetime(df['fecha'], format='%Y/%m/%d')
    ### Cambiar sede
    dict_replace ={
        'Bulevar':'Clínicos IPS Sede Bulevar',
        'San Martin':'Clínicos IPS Sede San Martín',
        'Cartagena':'Clínicos IPS Sede Cartagena'
    }
    df = df.replace({'sede':dict_replace})
    #
    print(df.info())
    print(df.head(5))
    return df


# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_TblHActividadesMega():
    #
    path = dirname + filename
    print(path)
    check_connection(container, filename)
    file_get(path, container,filename, wb = wb)
    #
    df = transform_table(dirname, filename)
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
    get_tmp_TblHActividadesMega = PythonOperator(task_id = "Get_tmp_TblHActividadesMega",
                                                  python_callable = func_get_TblHActividadesMega,
                                                  email_on_failure=True, 
                                                  email='BI@clinicos.com.co',
                                                  dag=dag,
                                                  )
    
    # Carge del dimensional conceptos
    load_TblDConceptosPAMI = MsSqlOperator(task_id ='Load_TblDConceptosPAMI',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblDConceptosPAMI",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )
    
    # Carge del dimensional detalle
    load_TblDConceptoDetallePAMI = MsSqlOperator(task_id ='Load_TblDConceptoDetallePAMI',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblDConceptoDetallePAMI",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )
    
    # Carge del dimensional MEGA
    load_TblDMegas = MsSqlOperator(task_id ='Load_TblDMegas',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblDMegas",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )
    
    # Carge del dimensional tipo de MEGA
    load_TblDTipoMega = MsSqlOperator(task_id ='Load_TblDTipoMega',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblDTipoMega",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )

    # Carge de hechos, actividades MEGA
    load_TblHActividadesMega = MsSqlOperator(task_id ='Load_TblHActividadesMega',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblHActividadesMega",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_tmp_TblHActividadesMega >> load_TblDConceptosPAMI >> load_TblDConceptoDetallePAMI >> load_TblDMegas >> load_TblDTipoMega >> load_TblHActividadesMega >> task_end