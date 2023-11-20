import pandas as pd
import os
import re
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta
from variables import sql_connid,sql_connid_gomedisys
from utils import load_df_to_sql_pandas, WasbHook, remove_accents_cols, remove_special_chars, regular_snake_case, filelist_get
#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'otros-domi'
dirname = '/opt/airflow/dags/Masiva/in/'
filename_prefix = 'BASE MAESTRA NUEVA EPS'
db_table = "TblHBaseMaestraNEPS"
db_tmp_table = "tmpTblHBaseMaestraNEPS"
dag_name = 'dag_' + db_table

def transform_data(files_path):
    # Lectura de todos los archivos que coninciden con el patron
    df_list = [pd.read_excel(path, header = [0]) for path in files_path]
    for indx in range(len(df_list)):
        if len(df_list[indx].columns) == 19:
            df_list[indx].iloc[:,1] = df_list[indx].iloc[:,1].astype(str) + " " + df_list[indx].iloc[:,2].astype(str)
            df_list[indx] = df_list[indx].drop(df_list[indx].columns[2], axis = 1)
        df_list[indx].columns = [''] * len(df_list[indx].columns)

    df = pd.concat(df_list)
    print(df.columns)
    # Nombres personalizados a columnas
    df_colnames = ['fecha_emision','id','nombre','ips','tipo','clase','numero','estado','usuario_autorizador'
      ,'codigo','descripcion','cantidad','cubrimiento','pago_afiliado','origen','anexo','no_prescripcion',
      'consecutivo']
    df.columns = df_colnames
    # Tratamiento de fechas por index
    for index, column in enumerate(df.columns):
        if index in [0]:
            df[column] = df[column].astype(str)
            df[column] = pd.to_datetime(df[column], errors='coerce')
    return df


def func_get_tblhBaseMaestra():
    # LECTURA DE DATOS
    filelist_get(dirname, container, filename_prefix, blob_delimiter='.xlsx' , wb = wb)
    files_path = [dirname+file for file in os.listdir(dirname) if os.path.isfile(dirname+file) and re.match(filename_prefix, file)]
    print(files_path)
    df = transform_data(files_path)
    # CARGA A BASE DE DATOS
    if ~df.empty and len(df.columns) >0:
        load_df_to_sql_pandas(df,db_tmp_table, sql_connid, truncate=True)


# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 1),
}

# Se declara el DAG con sus respectivos parámetros
with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag a las 1:00 am (hora servidor) todos los Domingos
    schedule_interval= '30 16 * * FRI',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    func_get_tblhBaseMaestra_python_task = PythonOperator(task_id = "func_get_tblhBaseMaestra",
                                                        python_callable = func_get_tblhBaseMaestra,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_tblhBaseMaestra = MsSqlOperator(task_id='Load_tblhBaseMaestra',
                                             mssql_conn_id=sql_connid,
                                             autocommit=True,
                                             sql=f"EXECUTE uspCarga_tblhBaseMaestraNEPS",
                                             email_on_failure=True, 
                                             email='BI@clinicos.com.co',
                                             dag=dag
                                        )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> func_get_tblhBaseMaestra_python_task  >> load_tblhBaseMaestra >> task_end