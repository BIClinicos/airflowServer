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


# Para correr manualmente las fechas
fecha_texto = '2023-01-31 00:00:00'
now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

now = now.strftime('%Y-%m-%d %H:%M:%S')
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')


# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_dimMedicionesSignosVitales ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)

    query_legacy = f"""
        SELECT
            DISTINCT
            -- CONCAT(EHRCM.idMeasurement, '-', EHRPM.idUserPatient ,'-') as id_Medicion_Signo_Vital,
            EHRCM.idMeasurement as id_Medicion_Registrada,
            EHRPM.idUserPatient as id_Usuario_Paciente,
            EHRPM.recordedDate as [Fecha_de_Evento],
            EHRCM.name as [Nombre_Signo_Vital],
            EHRCM.isForMale as [Aplica_para_Hombres],
            EHRCM.isForFemale as [Aplica_para_Mujeres],
            EHRCM.minimumAgeMonths as [Edad_Minima_en_Meses],
            EHRCM.maximumAgeMonths as [Edad_Maxima_en_Meses],
            EHRCM.requireSpeciality as [Requiere_Especialidad_Profesional],
            EHRCM.requiereCDA as [Requiere_Actividad_de_EHR],
            EHRCM.sortPosition as [Posicion_en_Pantalla]
        FROM EHRConfMeasurements EHRCM
        INNER JOIN EHRPatientMeasurements EHRPM ON EHRCM.idMeasurement=EHRPM.idMeasurement
        WHERE recordedDate >='{last_week}' AND recordedDate<='{now}'
    """

    query = f"""
        SELECT
            Todo.idMedicionRegistrada,
            Todo.idUsuarioPaciente,
            Todo.fechaEvento,
            Todo.nombreSignoVital,
            Todo.valorRegistradoSigno,
            Todo.aplicaParaHombres,
            Todo.aplicaParaMujeres,
            Todo.edadMinimaEnMeses,
            Todo.edadMaximaEnMeses,
            Todo.requiereEspecialidadProfesional,
            Todo.requiereActividadDeEHR,
            Todo.posicionEnPantalla
        FROM (
            SELECT DISTINCT
                EHRCM.idMeasurement     as idMedicionRegistrada,
                EHRPM.idUserPatient     as idUsuarioPaciente,
                EHRPM.recordedDate      as fechaEvento,
                EHRCM.name              as nombreSignoVital,
                EHRPM.recordedValue     as valorRegistradoSigno,
                EHRCM.isForMale         as aplicaParaHombres,
                EHRCM.isForFemale       as aplicaParaMujeres,
                EHRCM.minimumAgeMonths  as edadMinimaEnMeses,
                EHRCM.maximumAgeMonths  as edadMaximaEnMeses,
                EHRCM.requireSpeciality as requiereEspecialidadProfesional,
                EHRCM.requiereCDA       as requiereActividadDeEHR,
                EHRCM.sortPosition      as posicionEnPantalla,
                ROW_NUMBER() over( partition by EHRCM.idMeasurement, EHRPM.idUserPatient ORDER BY EHRPM.recordedDate DESC) as Indicador
            FROM EHRConfMeasurements EHRCM
            INNER JOIN EHRPatientMeasurements EHRPM ON EHRCM.idMeasurement=EHRPM.idMeasurement
            WHERE recordedDate >='{last_week}' AND recordedDate<='{now}' ) AS Todo
            WHERE Todo.Indicador=1
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
    schedule_interval= None, # '10 9 * * 04', # cron expression
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"  
    get_dimMedicionesSignosVitales = PythonOperator(task_id = "get_dimMedicionesSignosVitales",
                                                                python_callable = func_get_dimMedicionesSignosVitales,
                                                                #email_on_failure=False, 
                                                                # email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_dimMedicionesSignosVitales >> task_end