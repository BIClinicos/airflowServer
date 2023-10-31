"""
Proyecto: Masiva NEPS

author dag: Luis Esteban Santamaría. Ingeniero de Datos.
Fecha creación: 29/08/2023

"""

# Librerias
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
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get


#  Creación de variables

db_table = "TblDEsquemasConfigurables"
db_tmp_table = "TmpEsquemasConfigurables"
dag_name = 'dag_' + db_table


# Para correr manualmente las fechas
# fecha_texto = '2023-10-26 00:00:00'
# now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
# last_week=datetime.strptime('2023-07-01 00:00:00', '%Y-%m-%d %H:%M:%S')


# now = now.strftime('%Y-%m-%d %H:%M:%S')
# last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')

def func_get_dimEsquemasConfigurables ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)

    
    query = f"""
        SELECT
            Enc.idUserPatient               as idUsuarioPaciente
            ,Enc.idEncounter                 as idIngreso
            ,EHREvCust.idEvent               as idEvento
            ,Eve.actionRecordedDate          as fechaEvento

            ,EHREvCust.idConfigActivity      as idEsquemaActividad
            ,EHREvCust.idElement             as idElementoAGuardar
            ,EHREvCust.valueText             as valorTextoARegistrar
        FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
        INNER JOIN encounters AS Enc WITH(NOLOCK) ON Eve.idEncounter = Enc.idEncounter
        WHERE Eve.actionRecordedDate >='{last_week}' AND Eve.actionRecordedDate<'{now}'
        AND
        (
            ((EHREvCust.idElement = 1						        AND EHREvCust.idConfigActivity IN (292, 293, 301, 302, 304, 375))
            OR (EHREvCust.idElement IN (3,4)					    AND EHREvCust.idConfigActivity = 293)
            OR (EHREvCust.idElement IN (1,2,3,4,5,6,7,8,9)		    AND EHREvCust.idConfigActivity = 374)
            OR (EHREvCust.idElement IN (1,2)					    AND EHREvCust.idConfigActivity = 303)
            OR (EHREvCust.idElement IN (12,13,14,15,16,17,18,24)    AND EHREvCust.idConfigActivity = 290))
        )
    """

    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    # conversión de campos
    df['fechaEvento'] = df['fechaEvento'].astype(str)

    print(df.columns)
    print(df.dtypes)
    print(df.isna().sum()) # conteo de nulos por campo
    print(df)

    # Si la consulta no es vacía, carga el dataframe a la tabla temporal en BI.
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
    # Se establece la ejecución del dag a las 9:10 am (hora servidor) todos los Jueves
    schedule_interval= '30 18 * * SAT', # cron expression
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dimEsquemasConfigurables = PythonOperator(task_id = "get_dimEsquemasConfigurables",
                                                                python_callable = func_get_dimEsquemasConfigurables,
                                                                email_on_failure=False, 
                                                                email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dimEsquemasConfigurables = MsSqlOperator(task_id='Load_dimEsquemasConfigurables',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDEsquemasConfigurables",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )
    
    load_factEsquemasConfigurables = MsSqlOperator(task_id='Load_factEsquemasConfigurables',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblHEsquemasConfigurables",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')


start_task >> get_dimEsquemasConfigurables >> load_dimEsquemasConfigurables >> load_factEsquemasConfigurables >> task_end
