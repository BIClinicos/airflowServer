import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators import email_operator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
import numpy as np
from pandas import read_excel
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df, load_df_to_sql, get_engine

########
#### Ficha tecnica
#######
### Elabora: David Cardenas Pienda
### Fecha: 2023-01-19
### Documentacion: Si
### Proceso: Reporte de malla final domiciliaria. Compensar
#######

### Parametros de tablas (def y temp) y nombre del dag

db_table = "SAL_DOM_CO_MallaFinal"
db_tmp_table = "tmp_SAL_DOM_CO_MallaFinal"
dag_name = 'dag_' + db_table

### Parametros de tiempo
# Obtencion dinamica
end_date = date.today() - timedelta(days=1)
start_date = end_date.replace(day=1)
# Carge historico
#start_date = date(2023, 6, 1) 
#end_date = date(2023, 6, 30)
# Formato
start_date = start_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d')

#######
#### Funciones del script
#######

### Metodo para lectura del script

def query_SAL_DOM_CO_MallaFinal (start_date, end_date):
    """Listado de internacion domiciliaria

    Parametros: 
    start (date): Fecha inicial del periodo de consulta
    end (date): Fecha final del periodo de consulta

    Retorna:
    data frame con la informacion del listado de internacion domiciliaria compensar
    """
    ## Verficacion de fechas
    print('Fecha inicio ', start_date)
    print('Fecha fin ', end_date)
    # LECTURA DE DATOS
    with open("dags/MasivaCompensar/queries/MasivaCompensar.sql") as fp:
        query = fp.read().replace("{end_date}", f"{end_date!r}").\
                replace("{start_date}",f"{start_date!r}")
    df:pd.DataFrame = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    print(df.info())
    # Convertir a str los campos de tipo fecha 
    cols_dates = [i for i in df.columns if 'Fecha' in i]
    for col in cols_dates:
        df[col] = df[col].astype(str)
    # Convertir en int los campos float
    cols_float = df.select_dtypes(include=[np.float]).columns
    for col in cols_float:
        df[col] = df[col].astype('Int64')

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', -1)
    print(df.columns)
    print(df.dtypes)
    print(df)
    ### Retorno
    return df
    
### Metodo de cargue de malla final

def func_get_SAL_DOM_CO_MallaFinal():
	"""Metodo ETL para el DAG #1

	Parametros: 
	Ninguno

	Retorna:
	Retorno vacio
	"""
	df = query_SAL_DOM_CO_MallaFinal(start_date = start_date, end_date = end_date)
	print(df.dtypes)
	print(df.columns)
	if ~df.empty and len(df.columns) >0:
		load_df_to_sql(df, db_tmp_table, sql_connid)

# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}

with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los dias a las 6 15 am (hora servidor)
    schedule_interval= '15 6 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_SAL_DOM_CO_MallaFinal_python_task = PythonOperator(
                                                    task_id = "get_SAL_DOM_CO_MallaFinal",
                                                    python_callable = func_get_SAL_DOM_CO_MallaFinal,
                                                    email_on_failure = True, 
                                                    email='BI@clinicos.com.co',
                                                    dag=dag
                                                    )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_SAL_DOM_CO_MallaFinal_Task = MsSqlOperator(task_id='Load_SAL_DOM_CO_MallaFinal',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE sp_load_SAL_DOM_CO_MallaFinal",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_SAL_DOM_CO_MallaFinal_python_task >> load_SAL_DOM_CO_MallaFinal_Task >> task_end