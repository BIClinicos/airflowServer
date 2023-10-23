import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta
from variables import sql_connid,sql_connid_gomedisys
from utils import load_df_to_sql_pandas, WasbHook, remove_accents_cols, remove_special_chars, regular_snake_case, file_get
#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'otros-domi'
dirname = '/opt/airflow/dags/Masiva/in/'
filename = f'Reporte de agendas domiciliarias.xlsx'
db_table = "tmpTblHReporteAgendasDomi"
db_tmp_table = "tmpTblHReporteAgendas"
dag_name = 'dag_' + db_table


def transform_data(path):
    # lectura del archivo de excel
    df = pd.read_excel(path, header = [0])
    # estandarización de los nombres de las columnas
    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)
    # area
    df['area'] = [
        'ENFERMERÍA' if 'auxiliar de enfermeria' in x
        else 'RHB' if 'terap' in x
        else 'MÉDICA' for x in df['perfil_profesional']
    ]
    df['mal_creado'] = 'No'
    #  Retorno
    df = df[
        [
        'no_documento', 
        'ingreso',
        'contrato', 
        'plan', 
        'plan_domiciliario',
        'actividad',
        'producto', 
        'estado',
        'fecha_programado', 
        'profesional',
        'documento_profesional', 
        'perfil_profesional',
        'sede',
        'area',
        'mal_creado'
        ]    
    ]
    return df


def func_get_tblhReporteAgendas():
    # LECTURA DE DATOS
    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_data(path)
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
    schedule_interval= '45 2 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    func_get_tblhReporteAgendas_python_task = PythonOperator(task_id = "func_get_tblhReporteAgendas",
                                                        python_callable = func_get_tblhReporteAgendas,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_tblhReporteAgendas = MsSqlOperator(task_id='Load_tblhEspecializadaNEPS',
                                             mssql_conn_id=sql_connid,
                                             autocommit=True,
                                             sql=f"EXECUTE uspCarga_tblhReporteAgendas",
                                             email_on_failure=True, 
                                             email='BI@clinicos.com.co',
                                             dag=dag
                                        )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> func_get_tblhReporteAgendas_python_task  >> load_tblhReporteAgendas >> task_end