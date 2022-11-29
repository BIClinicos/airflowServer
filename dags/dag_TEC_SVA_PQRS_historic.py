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

#  Se nombran las variables a utilizar en el dag

db_table = "TEC_SVA_PQRS"
db_tmp_table = "tmp_TEC_SVA_PQRS"
dag_name = 'dag_' + db_table + '_historic'

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_TEC_SVA_PQRS ():


    # Script de sql con la consulta que queremos traer al dag
    pqr_query = f"""
    select
        *
    from
        [TEC_SVA_PQRS]
    """

    # Llamado a la función que me trae el query de la base de datos
    df = sql_2_df(pqr_query, sql_conn_id=sql_connid)


    # Estandarización de los nombres de columnas del dataframe
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('   ',' ')
    df.columns = df.columns.str.replace('  ',' ')
    df.columns = df.columns.str.replace(' ','_')
    df.columns = df.columns.str.replace('á','a')
    df.columns = df.columns.str.replace('é','e')
    df.columns = df.columns.str.replace('í','i')
    df.columns = df.columns.str.replace('ó','o')
    df.columns = df.columns.str.replace('ú','u')
    df.columns = df.columns.str.replace('ñ','ni')

    # Estandarización de las columnas de fechas a tipo fecha

    columns_date = [
            'fecha_recepcion',
            'fecha_maxima_de_respuesta',
            'fecha_de_respuesta_preliminar',
            'fecha_respuesta'
        ]
    df['fecha_de_respuesta_preliminar'] = df['fecha_de_respuesta_preliminar'].str.replace('Sáb ','')
    df['fecha_de_respuesta_preliminar'] = df['fecha_de_respuesta_preliminar'].str.replace(' 7:23','')
    df['fecha_de_respuesta_preliminar'] = df['fecha_de_respuesta_preliminar'].fillna('')
    

    df['fecha_respuesta'] = df['fecha_respuesta'].fillna('')

    df.loc[df['fecha_respuesta'].str.contains('DR'), 'fecha_respuesta'] = ''
    df.loc[df['fecha_respuesta'].str.contains('min'), 'fecha_respuesta'] = ''
    df.loc[df['fecha_respuesta'].str.contains('NEU'), 'fecha_respuesta'] = ''
    df.loc[df['fecha_respuesta'].str.contains('REU'), 'fecha_respuesta'] = ''
    df.loc[df['fecha_respuesta'].str.contains('"'), 'fecha_respuesta'] = ''

    for col in columns_date:
        df[col] = pd.to_datetime(df[col], format="%d/%m/%Y", errors='coerce')

    # Estandarización de las columna de edad

    df['edad'] = df['edad'].fillna('')
    df['edad'] = df['edad'].str.strip()
    df['edad'] = df['edad'].str.replace(' AÑOS','')
    df['edad'] = df['edad'].str.replace(' AÑO','')
    df['edad'] = df['edad'].str.replace(' años','')
    df['edad'] = df['edad'].str.replace('N/A','')
    df['edad'] = df['edad'].str.replace('NA','')
    df.loc[df['edad'].str.contains('MES'), 'edad'] = '0'
    df.loc[df['edad'].str.contains('mes'), 'edad'] = '0'
    df['edad'] = pd.to_numeric(df['edad'], downcast='integer')

    # Estandarización de las columna de telefono

    df['telefonos'] = df['telefonos'].str.strip()
    df['telefonos'] = df['telefonos'].str.replace('ANONIMO','')
    df['telefonos'] = df['telefonos'].str.replace('MATEO  CORREA VILLALOBOS','')
    df['telefonos'] = df['telefonos'].str.replace('"','')
    df['telefonos'] = df['telefonos'].str.replace('N/A','')
    df['telefonos'] = df['telefonos'].str.replace('NA','')
    df['telefonos'] = df['telefonos'].str.replace('N/S','')
    df['telefonos'] = df['telefonos'].str.replace('N7A','')

    df['tiempo_maximo_respuesta'] = pd.to_numeric(df['tiempo_maximo_respuesta'], downcast='integer')

    df['alerta'] = df['alerta'].str.upper()



    # Reemplazo de INCAPACIDAD en INCAPACIDADES y TRATO INADECUADO DEL APCIENTE por TRATO INADECUADO DEL PACIENTE en la columna especialidad_area_proceso

    df['especialidad_area_proceso'] = df['especialidad_area_proceso'].str.upper()
    df['especialidad_area_proceso'] = df['especialidad_area_proceso'].replace({'INCAPACIDAD':'INCAPACIDADES', 'TRATO INADECUADO DE APCIENTE ': 'TRATO INADECUADO DEL PACIENTE'})


    # Eliminar todos los registros que no sean sí o no en la columna de "atribuible"

    df['atribuible'] = df['atribuible'].str.upper()
    df_1 = df[df.atribuible == 'SI']
    df_2 = df[df.atribuible == 'NO']
    df = df_1.append(df_2, ignore_index=True)


    # Eliminar todos los registros que no sean sí o no en la columna de "oportuna"

    df['oportuna'] = df['oportuna'].str.upper()
    df['oportuna'] = df['oportuna'].str.replace('SI','OPORTUNA')
    df_3 = df[df.oportuna == 'OPORTUNA']
    df_4 = df[df.oportuna == 'NO OPORTUNA']
    df = df_3.append(df_4, ignore_index=True)

    df = df.fillna('')
    


    print(df['especialidad_area_proceso'].unique())
    print(df['atribuible'].unique())
    print(df['oportuna'].unique())
    print(df_3)
    print(df_4)
    print(df.columns)
    print(df.dtypes)
    print(df.head(20))
    print(df)

    df = df.drop_duplicates(subset=['canal_de_recepcion', 'peticionario', 'oys',
       'nombres_y_apellidos_paciente', 'cc', 'no_documento', 'edad',
       'telefonos', 'correo', 'eps_paciente', 'unidad', 'ciudad',
       'clasificacion_pqrs', 'motivo', 'especialidad_area_proceso',
       'descripcion', 'atributos_de_calidad', 'fecha_recepcion',
       'tiempo_maximo_respuesta', 'fecha_maxima_de_respuesta',
       'responsable_asignado', 'fecha_de_respuesta_preliminar', 'alerta',
       'atribuible', 'responsable_respuesta', 'respuesta', 'fecha_respuesta',
       'oportuna', 'contrato', 'observacion', 'cruce', 'fecha_operacion'])

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
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '0 10 * * fri',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_TEC_SVA_PQRS_python_task = PythonOperator(task_id = "get_TEC_SVA_PQRS",
        python_callable = func_get_TEC_SVA_PQRS)
    
    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_TEC_SVA_PQRS_python_task >> task_end