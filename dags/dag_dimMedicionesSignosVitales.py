"""
Proyecto: Masiva NEPS

author dag: Luis Esteban Santamaría. Ingeniero de Datos.
Fecha creación: 28/08/2023

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

db_table = "TblDMedicionesSignosVitales"
db_tmp_table = "TmpMedicionesSignosVitales"
dag_name = 'dag_' + db_table


# Para correr manualmente las fechas (Comentario: por historia clinica, solo registros a partir  de Julio 1)
# fecha_texto = '2023-10-26 00:00:00'
# now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
# last_week=datetime.strptime('2023-07-01 00:00:00', '%Y-%m-%d %H:%M:%S')


# now = now.strftime('%Y-%m-%d %H:%M:%S')
# last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')


now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')


# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_dimMedicionesSignosVitales ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)


    query = f"""
        DECLARE 
		@idActionEvents VARCHAR(MAX) = '1013,1004,1023' -- Historias Clínicas

        SELECT
            DISTINCT
            EHP.idUserPatient as idUsuarioPaciente
            ,EHP.idEncounter as idIngreso
            ,EHP.idEHREvent as idEventoEHR
            ,EHP.recordedDate as fechaEvento

            ,EHP.idMeasurement as idSignoVital
            ,EHP.idRecord as idRegistro
            ,Ev.idAction as idHistoriaClinica
            ,EHC.name as descripcionSignoVital
            ,CAST(EHP.recordedValue as INT) as valorRegistradoSigno
            ,EHC.isForFemale as aplicaParaMujeres
            ,EHC.isForMale as aplicaParaHombres
            ,EHC.minimumAgeMonths as edadMinimaEnMeses
            ,EHC.maximumAgeMonths as edadMaximaEnMeses
        from EHRPatientMeasurements EHP
        INNER JOIN EHRConfMeasurements EHC ON  EHP.idMeasurement=EHC.idMeasurement
        INNER JOIN Encounters Enc ON Enc.idEncounter = EHP.idEncounter
        INNER JOIN EHREvents Ev ON Ev.idEHREvent = EHP.idEHREvent
        WHERE Ev.idAction in (SELECT Value FROM dbo.FnSplit(@idActionEvents)) 
        AND EHP.recordedDate >='{last_week}' AND EHP.recordedDate<='{now}'

        AND (EHC.name LIKE '%Talla%'
            OR EHC.name LIKE '%Peso%'
            OR EHC.name LIKE '%P.A.%Sist_lica%'
            OR EHC.name LIKE '%P.A.%Diast_lica%'
            OR EHC.name LIKE '%Circunferencia%Abdominal%')

    """

    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    # conversión de campos
    df['fechaEvento'] = df['fechaEvento'].astype(str)
    df['valorRegistradoSigno'] = df['valorRegistradoSigno'].astype(int)


    print(df.columns)
    
    print(df.dtypes)

    nan_count_all = df.isna().sum()
    print(nan_count_all)
    
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
    schedule_interval= '0 18 * * SAT',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"  
    get_dimMedicionesSignosVitales = PythonOperator(task_id = "get_dimMedicionesSignosVitales",
                                                                python_callable = func_get_dimMedicionesSignosVitales,
                                                                email_on_failure=True, 
                                                                email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )


    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_dimMedicionesSignosVitales = MsSqlOperator(task_id='Load_dimMedicionesSignosVitales',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDMedicionesSignosVitales",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_factMedicionesSignosVitales = MsSqlOperator(task_id='Load_factMedicionesSignosVitales',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblHMedicionesSignosVitales",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dimMedicionesSignosVitales >> load_dimMedicionesSignosVitales >> load_factMedicionesSignosVitales >> task_end