import os
import xlrd
import re
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators import email_operator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get, load_df_to_sql_2

import numpy as np 
from datetime import date
from datetime import datetime, timedelta

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pqrs'
dirname = '/opt/airflow/dags/files_pqrs/'
filename = 'TEC_SVA_PQRS.xlsx'
db_table = "TEC_SVA_PQRS"
db_tmp_table = "tmp_TEC_SVA_PQRS"
dag_name = 'dag_TEC_SVA_PQRS_AuditUpdate'#'dag_' + db_table
container_to = 'historicos'
data_state = ''

today = date.today()
last_day = today - timedelta(days = 1)
audit_table = "biDataUpdatesAudit"
data_date = ''
#now = date.today()

# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def transform_tables (path):

    # lectura del dataframe desde el archivo csv
    df = pd.read_excel(path, sheet_name=1)

    print("Como está leyendo el dataframe inicialmente",df)
    print("Nombres y tipos de columnas leídos del dataframe sin transformar",df.dtypes)


    # Estandarización de los nombres de columnas del dataframe
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('.',' ')
    df.columns = df.columns.str.replace('/',' ')
    df.columns = df.columns.str.replace('   ',' ')
    df.columns = df.columns.str.replace('  ',' ')
    df.columns = df.columns.str.replace(' ','_')
    df.columns = df.columns.str.replace('á','a')
    df.columns = df.columns.str.replace('é','e')
    df.columns = df.columns.str.replace('í','i')
    df.columns = df.columns.str.replace('ó','o')
    df.columns = df.columns.str.replace('ú','u')
    df.columns = df.columns.str.replace('ñ','ni')
    df.columns = df.columns.str.replace('\n.*','')
    df.columns = df.columns.str.replace('#','no')
    df.columns = df.columns.str.replace('_\([^)]*\)','')

    # 20221020 Campo canal_de_recepcion nunca tiene nulos, se usa para eliminar entradas vacias
    df = df.dropna(subset=['canal_de_recepcion'])

    # Eliminar todos los registros que no sean sí o no en la columna de "oportuna"
    df['oportuna'] = df['oportuna'].str.upper()
    df['oportuna'] = df['oportuna'].str.replace('SI','OPORTUNA')
    df_3 = df[df.oportuna == 'OPORTUNA']
    df_4 = df[df.oportuna == 'NO OPORTUNA']
    df = df_3.append(df_4, ignore_index=True)

    # Eliminar todos los registros que no sean sí o no en la columna de "atribuible"
    df['atribuible'] = df['atribuible'].astype(str)
    df['atribuible'] = df['atribuible'].str.upper()
    df_1 = df[df.atribuible == 'SI']
    df_2 = df[df.atribuible == 'NO']
    df = df_1.append(df_2, ignore_index=True)

    # reemplazando los valores nulos por strings vacíos
    if 'cc' in df.columns:
        df['cc'] = df['cc'].astype(str)
        df['cc'] = df['cc'].str.upper()
    else:
        df['tipo_de_documento'] = df['tipo_de_documento'].astype(str)
        df['cc'] = df['tipo_de_documento'].str.upper()

    df['no_documento'] = df['no_documento'].fillna('')

    if 'observaciones' in df.columns:
        df['observacion'] = df['observaciones']

    # Estandarización de las columnas de fechas a tipo fecha
    date_columns = ['fecha_recepcion','fecha_maxima_de_respuesta','fecha_de_respuesta_preliminar','fecha_respuesta']

    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        df[i] = pd.to_datetime(df[i], format="%Y-%m-%d", errors = 'coerce')

    # Estandarización de las columna de edad
    df['edad'] = df['edad'].fillna('')
    df['edad'] = df['edad'].astype(str)
    df['edad'] = df['edad'].str.strip()
    df.loc[df['edad'].str.contains('mes'), 'edad'] = '0'   
    df.loc[df['edad'].str.contains('MES'), 'edad'] = '0'    
    df['edad'] = df['edad'].str.replace(r'[a-zA-Z%]', '', regex=True)
    df['edad'] = df['edad'].str.replace('Ñ', '')
    df['edad'] = df['edad'].str.replace('ñ', '')
    df['edad'] = df['edad'].str.strip()

    df['edad'] = pd.to_numeric(df['edad'], downcast='integer', errors = 'coerce')

    # Estandarización de las columna de telefono
    df['telefonos'] = df['telefonos'].fillna('')
    df['telefonos'] = df['telefonos'].astype(str)
    df['telefonos'] = df['telefonos'].str.strip()
    df['telefonos'] = df['telefonos'].str.replace(r'[a-zA-Z%]', '', regex=True)

    df['tiempo_maximo_respuesta'] = pd.to_numeric(df['tiempo_maximo_respuesta'], downcast='integer')

    if 'alerta' in df.columns:
        df['alerta'] = df['alerta'].str.upper()
    else:
        df['alerta'] = df['alerta_de_vencimiento'].str.upper()

    # Reemplazo de INCAPACIDAD en INCAPACIDADES y TRATO INADECUADO DEL APCIENTE por TRATO INADECUADO DEL PACIENTE en la columna especialidad_area_proceso
    df['especialidad_area_proceso'] = df['especialidad_area_proceso'].astype(str)
    df['especialidad_area_proceso'] = df['especialidad_area_proceso'].str.upper()
    df['especialidad_area_proceso'] = df['especialidad_area_proceso'].replace({'INCAPACIDAD':'INCAPACIDADES', 'TRATO INADECUADO DE APCIENTE ': 'TRATO INADECUADO DEL PACIENTE'})

    df['fecha_operacion'] = pd.to_datetime(date.today())
    df['contrato'] = ''
    df['cruce'] = ''

    df = df[['canal_de_recepcion', 'peticionario', 'oys',
       'nombres_y_apellidos_paciente', 'cc', 'no_documento',
       'edad', 'telefonos', 'correo', 'eps_paciente', 'unidad', 'ciudad',
       'clasificacion_pqrs', 'motivo', 'especialidad_area_proceso',
       'descripcion', 'atributos_de_calidad', 'fecha_recepcion',
       'tiempo_maximo_respuesta', 'fecha_maxima_de_respuesta',
       'responsable_asignado', 'fecha_de_respuesta_preliminar',
       'alerta', 'atribuible', 'responsable_respuesta', 'respuesta', 
       'fecha_respuesta', 'oportuna', 'contrato', 'observacion', 'cruce',
       'fecha_operacion']]

    return df
    
    
def func_get_TEC_SVA_PQRS ():    

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
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
       'oportuna', 'observacion'])
    
    print(df)

    # if ~df.empty and len(df.columns) >0:
    #     load_df_to_sql(df, db_tmp_table, sql_connid)

    return df

#Función para identificar si la data está actualizada
def identify_updated_data ():

    df = func_get_TEC_SVA_PQRS()

    print('CAPTURAR FECHA DATA')
    #Captura de la máxima fecha del campo fecha_recepcion
    df_1 = df.agg(Maximum_Date=('fecha_recepcion', np.max))
    #print(df_1)
    print(last_day)

    if df_1.fecha_recepcion[0] < last_day:
        
        data_state = 'retrasada'

        data_date = df_1.fecha_recepcion[0]
        df_1.columns = ['data_date']
        event = ['Data retrasada']
        description = ['La informacion del dag_ TEC_SVA_PQRS cargada no está al día']
        dateTime = [today]
        
        df_1['event'] = event
        df_1['description'] = description
        df_1['dateTime'] = dateTime

        print(df_1)
        print('retrasada')
        
        if ~df_1.empty and len(df_1.columns) > 0:
            load_df_to_sql_2(df_1, audit_table, sql_connid)
        
        return data_state

    elif df_1.fecha_recepcion[0] > last_day: 
        data_state = 'al_dia'
        return data_state #'al_dia'

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
    schedule_interval= None,#'20 10 * * fri',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_TEC_SVA_PQRS_python_task = PythonOperator(task_id = "get_TEC_SVA_PQRS",
                                                  python_callable = func_get_TEC_SVA_PQRS,
                                                  #email_on_failure=True, 
                                                  #email='BI@clinicos.com.co',
                                                  dag=dag,
                                                  )
    
    #Se declara y se llama la función que identifica la opción por fecha actualizada 
    choose_option = BranchPythonOperator( 
        task_id='choose_option', 
        python_callable = identify_updated_data) 

    #Función a ejecutar cuando la data está retrasada
    retrasada = email_operator.EmailOperator(task_id='retrasada',
        #to=['BI@clinicos.com.co','quimico.farmaceutico@clinicos.com.co','kcastellanos@clinicos.com.co'],
        to=['fmgutierrez@clinicos.com.co'],
        subject=f'CUIDADO ::: Data retrasada dag PQRS {data_date}',
        html_content=f"""<p>Saludos, la data del dag <b>dag_TEC_SVA_PQRS</b>, cargada el <b>{today}</b> está desactualizada </p>
        <b> (Mail creado automaticamente) </b> </p>
        <br/>
        """
        )

    #Función a ejecutar cuando la data está al día
    al_dia = email_operator.EmailOperator(task_id='al_dia',
        #to=['BI@clinicos.com.co','quimico.farmaceutico@clinicos.com.co','kcastellanos@clinicos.com.co'],
        to=['fmgutierrez@clinicos.com.co'],
        subject=f'Data dag PQRS cargada al día {data_date}',
        html_content=f"""<p>Saludos, la data del dag de PQRS, cargada el {today} está al día
        <br/>
        (mail creado automaticamente).</p>
        <br/>
        """
        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_TEC_SVA_PQRS = MsSqlOperator(task_id='Load_TEC_SVA_PQRS',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_TEC_SVA_PQRS",
                                       #email_on_failure=True, 
                                       #email='BI@clinicos.com.co',
                                       dag=dag,
                                       )

    join = DummyOperator(task_id='join', dag=dag, trigger_rule='none_failed')

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_TEC_SVA_PQRS_python_task >> load_TEC_SVA_PQRS >> choose_option 
choose_option >> retrasada >> join >> task_end
choose_option >> al_dia >> join >> task_end