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
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'mallaterapias'
dirname = '/opt/airflow/dags/files_malla_terapias/'
filename = 'SAL_DOM_CO_MallaTer.xlsx'
db_table = "SAL_DOM_CO_MallaTer"
db_tmp_table = "tmp_SAL_DOM_CO_MallaTer"
dag_name = 'dag_' + db_table


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_tables (path):
    i = 0 # Hoja inicial
    while True:
        df = pd.read_excel(path, sheet_name = i)
        if df.shape[1] >= 18: # Verificar si la hoja contiene la cantidad de campos necesarios
            break
        i =+ 1
    # Cierre de ciclo con lectura    
    df_col_adi = df.columns[17:]
    df_col_mes_ant = df.columns[8:15:2]
    df = df.drop(df_col_adi ,axis=1)
    df = df.drop(df_col_mes_ant, axis=1)
    df = df.rename(columns = {"REGISTRO.1" : "MES", "PROFESIONAL QUE REALIZÓ LA VALORACION" : "PROFESIONAL QUE REALIZO LA VALORACION", "NUMERO DOCUMENTO DE IDENTIFICACIÓN" : "NUMERO DOCUMENTO DE IDENTIFICACION"})
    df_m = df.columns[8:12]
    df = pd.melt(df.reset_index(), id_vars = ["REGISTRO","MES","IPS DOMICILIO","NOMBRES COMPLETOS","TIPO DOCUMENTO","NUMERO DOCUMENTO DE IDENTIFICACION","VISITA MEDICA","PROFESIONAL QUE REALIZO LA VALORACION","DESTINO DE EGRESO"], value_vars = df_m, var_name='ESPECIALIDAD', value_name='ORDENES')
    df_especialidad = df["ESPECIALIDAD"].str.split(" ", expand=True)
    df_especialidad = df_especialidad.drop([1],axis=1)
    df["ESPECIALIDAD"] = df_especialidad 
    df["NUMERO DOCUMENTO DE IDENTIFICACION"] = df["NUMERO DOCUMENTO DE IDENTIFICACION"].astype(str)
    df = df.dropna(subset=["NOMBRES COMPLETOS", "TIPO DOCUMENTO"])
    df["VISITA MEDICA"] = pd.to_datetime(df["VISITA MEDICA"], format='%Y/%m/%d', errors='coerce')
    df["ORDENES"] = df["ORDENES"].replace({0.0 : 0})
    df["ORDENES"] = pd.to_numeric(df["ORDENES"], errors='coerce')
    df["ORDENES"] = df["ORDENES"].fillna(0)

    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_SAL_DOM_CO_MallaTer ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    print('before drop',df)
    # df_duplicates =df[df.duplicated(subset = ['REGISTRO', 'MES', 'NUMERO DOCUMENTO DE IDENTIFICACION', 'VISITA MEDICA', 'PROFESIONAL QUE REALIZO LA VALORACION', 'ESPECIALIDAD'])]
    df = df.drop_duplicates(subset= ['REGISTRO', 'MES', 'NUMERO DOCUMENTO DE IDENTIFICACION', 'VISITA MEDICA', 'PROFESIONAL QUE REALIZO LA VALORACION', 'ESPECIALIDAD'])
    print('after drop',df)
    # print(df_duplicates)
    # print(df.dtypes)
    # print(df['ORDENES'].tail(50))

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
    # Se establece la ejecución del dag todos los días a las 12:00 am(Hora servidor)
    schedule_interval= '30 5 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_SAL_DOM_CO_MallaTer_python_task = PythonOperator(task_id = "get_SAL_DOM_CO_MallaTer",
        python_callable = func_get_SAL_DOM_CO_MallaTer,
        email_on_failure=True,
        email='BI@clinicos.com.co',
        dag=dag,
        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_SAL_DOM_CO_MallaTer = MsSqlOperator(task_id='Load_SAL_DOM_CO_MallaTer',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_SAL_DOM_CO_MallaTer",
                                       email_on_failure=True,
                                       email='BI@clinicos.com.co',
                                       dag=dag,
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_SAL_DOM_CO_MallaTer_python_task >> load_SAL_DOM_CO_MallaTer >> task_end