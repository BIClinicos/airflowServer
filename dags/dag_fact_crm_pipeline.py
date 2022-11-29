import os
import xlrd
import re
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from utils import remove_accents_cols, regular_snake_case, remove_special_chars, normalize_str_categorical
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel, read_html
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get, normalize_str_categorical
# from bs4 import BeautifulSoup


#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pipeline'
dirname = '/opt/airflow/dags/files_pipeline/'
filename = 'Proyecto Customer Relationship Management V7 2022.xlsm'
db_table_details = 'fact_crm_pipeline_details'
db_tmp_table_details = 'tmp_crm_pipeline_details'
db_table = 'fact_crm_pipeline'
db_tmp_table = 'tmp_crm_pipeline'
dag_name = 'dag_' + 'fact_crm_pipeline'
container_to = 'historicos'

# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql

def transform_tables(path):

    df = pd.read_excel(path, sheet_name=2, header=4, engine='openpyxl')

    # Estandarización de los nombres de columnas del dataframe
    
    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)

    print("nombres estandarizados en español",df.dtypes)

    print("columnas",df.columns)

    # Limpieza de columnas no necesarias para las transformaciones

    df = df[
        [
        'etiqueta', 
        'nit', 
        'empresa', 
        'nombre', 
        'valor_mensual',
        'cantidad_meses', 
        'valor_anual', 
        'margen_calculado', 
        'fase', 
        'cliente',
        'fecha_inicio', 
        'fecha_ultimo_contacto', 
        'observaciones',
        'responsable2', 
        'dias_en_gestion', 
        'probabilidad_de_efectividad',
        'estado', 
        'razon_perdida', 
        'prioridad', 
        'fuente', 
        'asignacion',
        'unidad', 
        'flag_de_avance', 
        '1_identificar_necesidades', 
        '2_definir_alcance', 
        '3_diseniar_solucion',
        '4_caso_de_negocio', 
        '5_propuesta_final', 
        '6_negociacion_cierre',
        '7_implementacion_y_seguimiento', 
        'duracion_acum', 
        'duracion_esp', 
        '%',
        'alerta_atraso_fase_actual', 
        'fecha_aviso_seleccion', 
        'fecha_cierre',
        'observacion_th', 
        'columna1'
        ]
    ]

    print("columnas definitivas",df.columns)

    # cambio de nombres de columnas
    
    df = df.rename(
        columns = {
            'etiqueta' : 'label', 
            'nit' : 'nit', 
            'empresa' : 'company', 
            'nombre' : 'name', 
            'valor_mensual' : 'monthly_value',
            'cantidad_meses' : 'duration_months', 
            'valor_anual' : 'anual_value', 
            'margen_calculado' : 'profit_margin', 
            'fase' : 'phase', 
            'cliente' : 'contact_client',
            'fecha_inicio' : 'date_start', 
            'fecha_ultimo_contacto' : 'last_contact',
            'observaciones' : 'notes',
            'responsable2' : 'current_liable', 
            'dias_en_gestion' : 'management_time', 
            'probabilidad_de_efectividad' : 'conclude_probability',
            'estado' : 'status', 
            'razon_perdida' : 'loss_reason', 
            'prioridad' : 'priority', 
            'fuente' : 'source', 
            'asignacion' : 'assigned_to',
            'unidad' : 'unit', 
            'flag_de_avance' : 'progress_flag', 
            '1_identificar_necesidades' : '1_identificar_necesidades',
            '2_definir_alcance' : '2_definir_alcance',
            '3_diseniar_solucion' : '3_diseniar_solucion',
            '4_caso_de_negocio' : '4_caso_de_negocio', 
            '5_propuesta_final' : '5_propuesta_final', 
            '6_negociacion_cierre' : '6_negociacion_cierre',
            '7_implementacion_y_seguimiento' : '7_implementacion_y_seguimiento', 
            'duracion_acum' : 'total_time', 
            'duracion_esp' : 'estimated_time', 
            '%' : 'percentage',
            'alerta_atraso_fase_actual' : 'delay_alert',
            'fecha_aviso_seleccion' : 'date_selection', 
            'fecha_cierre' : 'date_deadline',
            'observacion_th' : 'notes_th', 
            'columna1' : 'comments'
        }
    )

    print("columnas nombres cambiados",df.dtypes)

    # sacar el numero inicial del nit

    df_1 = df['nit'].str.split('-', expand=True)
    df['nit'] = df_1[0]

    print(df['nit'])

    # Normalización de las columnas tipo texto

    text_col = [
        'company', 
        'name',
        'phase',
        'contact_client',
        'notes',
        'current_liable',
        'status', 
        'loss_reason', 
        'priority', 
        'source', 
        'assigned_to',
        'unit',
        'notes_th',
        'comments'
    ]

    for i in text_col:
        df[i] = df[i].fillna('')
        df[i] = df[i].astype(str)
        df[i] = normalize_str_categorical(df[i])

    # normalización de las columnas tipo "float"

    float_col = [
        'monthly_value',
        'duration_months', 
        'anual_value', 
        'profit_margin',
        'management_time', 
        'conclude_probability',
        'progress_flag', 
        'total_time', 
        'estimated_time', 
        'percentage',
        '1_identificar_necesidades', 
        '2_definir_alcance', 
        '3_diseniar_solucion',
        '4_caso_de_negocio', 
        '5_propuesta_final', 
        '6_negociacion_cierre',
        '7_implementacion_y_seguimiento'
    ]

    for i in float_col:
        df[i] = df[i].astype(str)
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0)
    

    # Pasar a tipo fecha las columnas de fecha
    date_col = [
        'date_start', 
        'last_contact',
        'date_selection', 
        'date_deadline'
    ]

    for i in date_col:
        df[i] = df[i].fillna('')
        df[i] = df[i].astype(str)
        df[i] = pd.to_datetime(df[i], errors='coerce')

    # Eliminación de filas nulas cuando "date_start" es nulo
    df = df.dropna(subset=['nit','date_start'])

    print("columnas finales de lo que vá del tratamiento de datos",df.dtypes)
    print("dataframe final",df)
    print("Columna de unidad", df['unit'])
    # df.to_excel('/opt/airflow/dags/files_pipeline/output.xlsx')

    return df

def func_get_fact_crm_pipeline():


    path = dirname + filename
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)


    # división pipeline details

    df_details = df[
        [
        'label', 
        'nit', 
        'company', 
        'name',
        '1_identificar_necesidades',
        '2_definir_alcance',
        '3_diseniar_solucion',
        '4_caso_de_negocio', 
        '5_propuesta_final', 
        '6_negociacion_cierre',
        '7_implementacion_y_seguimiento',
        'date_start'
       ]
    ]

    df_details['1_identificar_necesidades'] = df_details['1_identificar_necesidades']
    df_details['2_definir_alcance'] = df_details['1_identificar_necesidades'] + df_details['2_definir_alcance']
    df_details['3_diseniar_solucion'] = df_details['2_definir_alcance'] + df_details['3_diseniar_solucion']
    df_details['4_caso_de_negocio'] = df_details['3_diseniar_solucion'] + df_details['4_caso_de_negocio']
    df_details['5_propuesta_final'] = df_details['4_caso_de_negocio'] + df_details['5_propuesta_final']
    df_details['6_negociacion_cierre'] = df_details['5_propuesta_final'] + df_details['6_negociacion_cierre']
    df_details['7_implementacion_y_seguimiento'] = df_details['6_negociacion_cierre'] + df_details['7_implementacion_y_seguimiento']


    # Columnas índice del pivot "melt"
    id_col = ['label', 'nit', 'company', 'name','date_start']
    # Columnas a pivotear en el "melt"
    melt_col = ['1_identificar_necesidades','2_definir_alcance','3_diseniar_solucion','4_caso_de_negocio','5_propuesta_final','6_negociacion_cierre','7_implementacion_y_seguimiento']

    # se pivotea el dataframe de fact_cmr_pipeline_details
    df_details = pd.melt(df_details.reset_index(), id_vars=id_col, value_vars=melt_col, var_name='phase', value_name='value')

    # Creación de columna condicional que asigna el numero de días estimados para cada paso "estimated_value"
    value_dic = {
        '1_identificar_necesidades' : 7,
        '2_definir_alcance' : 14,
        '3_diseniar_solucion' : 28,
        '4_caso_de_negocio' : 42,
        '5_propuesta_final' : 56,
        '6_negociacion_cierre' : 70,
        '7_implementacion_y_seguimiento' : 80
    }

    df_details['estimated_value'] = ''

    for key, val in value_dic.items():
        df_details.loc[df_details['phase'] == key, 'estimated_value'] = val

    # pasar las columnas 'value' y 'estimated_value' a tipo timedelta para poder hacer operaciones con las fechas iniciales
    timedelta_col = ['value', 'estimated_value']

    for i in timedelta_col:
        df_details[i] = pd.to_timedelta(df_details[i], 'd')

    df_details['date_estimated_start'] = df_details['date_start']

    df_details['date_estimated_end'] = df_details['date_estimated_start'] + df_details['estimated_value']

    df_details['date_real_start'] = df_details['date_start']

    df_details['date_real_end'] = df_details['date_real_start'] + df_details['value']


    df_details = df_details[
        [
        'label', 
        'nit', 
        'company', 
        'name',
        'phase',
        'date_estimated_start',
        'date_estimated_end',
        'date_real_start',
        'date_real_end'
       ]
    ]

    print("fact_cmr_pipeline_details tipos y columnas ",df_details.dtypes)
    print("fact_cmr_pipeline_details columnas ",df_details.columns)
    print("fact_cmr_pipeline_details dataframe entero ",df_details)
    df_details.to_excel('/opt/airflow/dags/files_pipeline/output_details.xlsx')

    df_pipeline = df[
        [
        'label', 
        'nit', 
        'company', 
        'name', 
        'monthly_value',
        'duration_months', 
        'anual_value', 
        'profit_margin', 
        'phase', 
        'contact_client',
        'date_start', 
        'last_contact',
        'notes',
        'current_liable', 
        'management_time', 
        'conclude_probability',
        'status', 
        'loss_reason', 
        'priority', 
        'source', 
        'assigned_to',
        'unit', 
        'progress_flag', 
        'total_time', 
        'estimated_time', 
        'percentage',
        'delay_alert',
        'date_selection', 
        'date_deadline',
        'notes_th', 
        'comments'
       ]
    ]
    df_pipeline['product'] = ''

    print("fact_cmr_pipeline tipos y columnas ",df_pipeline.dtypes)
    print("fact_cmr_pipeline columnas ",df_pipeline.columns)
    print("fact_cmr_pipeline dataframe entero ",df_pipeline)
    df_pipeline.to_excel('/opt/airflow/dags/files_pipeline/output_pipeline.xlsx')

    if ~df.empty and len(df_details.columns) >0:
        load_df_to_sql(df_details, db_tmp_table_details, sql_connid)

    if ~df.empty and len(df_pipeline.columns) >0:
        load_df_to_sql(df_pipeline, db_tmp_table, sql_connid)


# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}
# Se declara el DAG con sus respectivos parámetros
with DAG(
    dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '0 5 1 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    get_fact_crm_pipeline = PythonOperator(task_id = "get_fact_crm_pipeline",
                                                python_callable = func_get_fact_crm_pipeline,
                                                email_on_failure=True, 
                                                email='BI@clinicos.com.co',
                                                dag=dag,
                                                )

    # Se declara la función encargada de ejecutar el "Stored Procedure" fact_cmr_pipeline_details
    load_fact_crm_pipeline_details = MsSqlOperator(task_id='Load_fact_crm_pipeline_details',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_fact_crm_pipeline_details",
                                       email_on_failure=True, 
                                       email='BI@clinicos.com.co',
                                       dag=dag,
                                       )

    # Se declara la función encargada de ejecutar el "Stored Procedure" fact_cmr_pipeline
    load_fact_crm_pipeline = MsSqlOperator(task_id='Load_fact_crm_pipeline',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_fact_crm_pipeline",
                                       email_on_failure=True, 
                                       email='BI@clinicos.com.co',
                                       dag=dag,
                                       )


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_fact_crm_pipeline >> load_fact_crm_pipeline_details >> load_fact_crm_pipeline >> task_end