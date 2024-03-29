"""
Proyecto: Masiva NEPS

author dag: Luis Esteban Santamaría. Ingeniero de Datos.
modified by: David Alejandro López Atehortúa. Ingeniero de Datos Senior
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

db_table = "TblDActividadesHomeCare"
db_tmp_table = "TmpActividadesHomeCare"
dag_name = 'dag_' + db_table


# Para correr manualmente las fechas

now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')

#fecha_texto = '2023-10-31 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-10-26 00:00:00', '%Y-%m-%d %H:%M:%S')

#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')


def func_get_dimActividadesHomeCare ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)

    
    query = f"""
        DECLARE @idProductos VARCHAR(MAX) = '4984,4987,40496,4989,4990,4992,4993,4994'

        SELECT
            DISTINCT
                Enc.idUserPatient           as idUsuarioPaciente
            ,EncHcAct.idEncounter        as idIngreso
            ,Ev.idEHREvent               as idEventoEHR
            ,EncHcAct.dateRegister       as fechaRegistro
            ,EncHcAct.idProduct          as idProducto
            ,EncHcAct.idRol
            ,EHRConf.idHCActivity        as idRegistroActividad
            ,EncHcAct.quantityTODO       as cantidadPorHacer
        FROM encounterHCActivities EncHcAct
        INNER JOIN Encounters as Enc WITH(NOLOCK) ON EncHcAct.idEncounter = Enc.idEncounter
        INNER JOIN EHREvents AS Ev WITH(NOLOCK) ON EncHcAct.idEncounter = Ev.idEncounter
        INNER JOIN encounterHC as EncHC WITH(NOLOCK) ON EncHcAct.idEncounter = EncHC.idEncounter
        LEFT JOIN EHRConfHCActivity AS EHRConf ON EncHC.idHCActivity = EHRConf.idHCActivity
        WHERE EncHcAct.idRol IS NOT NULL AND EncHcAct.isActive = 1
        AND EncHcAct.dateRegister >='{last_week}' AND EncHcAct.dateRegister <'{now}'

        AND  EncHcAct.idProduct IN (SELECT Value FROM dbo.FnSplit(@idProductos))
    """

    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    # conversión de campos
    dateCols = ['fechaInicioPlan','fechaRegistroRevista']
    df['fechaRegistro'] = df['fechaRegistro'].astype(str)

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
    schedule_interval= '45 18 * * SAT', # cron expression
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_dimActividadesHomeCare = PythonOperator(task_id = "get_dimActividadesHomeCare",
                                                                python_callable = func_get_dimActividadesHomeCare,
                                                                email_on_failure=True, 
                                                                email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dimActividadesHomeCare = MsSqlOperator(task_id='Load_dimActividadesHomeCare',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDActividadesHomeCare",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    load_factActividadesHomeCare = MsSqlOperator(task_id='Load_factActividadesHomeCare',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblHActividadesHomeCare",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dimActividadesHomeCare >> load_dimActividadesHomeCare >> load_factActividadesHomeCare >> task_end
