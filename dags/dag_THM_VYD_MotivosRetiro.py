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
from variables import sql_connid
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix,get_files_xlsx_with_prefix, get_files_xlsx_contains_name, get_files_xlsx_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,remove_accents_cols,remove_special_chars,regular_camel_case,regular_snake_case,move_to_history_for_prefix,normalize_str_categorical
#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'rotacionpersonas'
dirname = '/opt/airflow/dags/files_rotacion_personas/'
filename = 'THM_VYD_MotivosRetiro.xlsx'
db_table = "THM_VYD_MotivosRetiro"
db_tmp_table = "tmp_THM_VYD_MotivosRetiro"
dag_name = 'dag_' + db_table
prefix = db_table


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_tables(path):

    df = pd.read_excel(path, header = [0])
    df.columns = df.columns.str.upper()
    df = df.dropna(subset=['FECHA DE RETIRO'])

    # Separación de las columna 'FUNCIÓN' por cargo y nivel de cargo
    df_cargo = df['FUNCIÓN'].str.split('-', n=1, expand = True)

    # Estandarización de la columna nivel del cargo
    df_1 = df_cargo.apply(lambda x: "NO REPORTA" if (x[1] == None) else x[0], axis = 1)
    df['nivel_cargo'] = df_1
    df['nivel_cargo'] = df['nivel_cargo'].str.strip()

    # print(df['nivel_cargo'].head(30))

    # Estandarización de la columna "FUNCIÓN"
    df_2 = df_cargo.apply(lambda x: x[0] if (x[1] == None) else x[1], axis = 1)
    df['FUNCIÓN'] = df_2
    df['FUNCIÓN'] = df['FUNCIÓN'].str.strip()

    # print(df['FUNCIÓN'].head(30))

    #Eliminación de las columnas "MES" y "TIPO DE SALIDA"
    delete_col = ['MES', 'TIPO DE SALIDA']

    for i in delete_col:
      if i in df.columns:
        df = df.drop([i], axis = 1)
        

    # cambio en la columna SEDE

    admin_rep = ['Admin', 'Alc_zares']

    for i in admin_rep:
        df.loc[df['SEDE'].str.contains(i), 'SEDE'] = 'Administrativa'
    

    df.loc[df['SEDE'].str.contains('Trabajo en'), 'SEDE'] = 'Trabajo en Casa'
    df.loc[df['SEDE'].str.contains('Telet%jo'), 'SEDE'] = 'Teletrabajo'
    df.loc[df['SEDE'].str.contains('%remoto'), 'SEDE'] = 'Teleorientación'
    df.loc[df['SEDE'].str.contains('Bogot_'), 'SEDE'] = 'Varios'
    df.loc[df['SEDE'].str.contains('Fusagasug_'), 'SEDE'] = 'Americas'

    # print(df['SEDE'].unique())


    # cambio en la columna UNIDAD/ÁREA

    df['UNIDAD/ÁREA'] = df['UNIDAD/ÁREA'].str.capitalize()
    df.loc[df['UNIDAD/ÁREA'].str.contains('Domiciliaria'), 'UNIDAD/ÁREA'] = 'Unidad Domiciliaria'
    df.loc[df['UNIDAD/ÁREA'].str.contains('Especializada'), 'UNIDAD/ÁREA'] = 'Unidad Especializada'
    df.loc[df['UNIDAD/ÁREA'].str.contains('Financiera'), 'UNIDAD/ÁREA'] = 'Financiera y Administrativa'
    df.loc[df['UNIDAD/ÁREA'].str.contains('Primaria'), 'UNIDAD/ÁREA'] = 'Unidad Primaria'
    df.loc[df['UNIDAD/ÁREA'].str.contains('Tecnolog'), 'UNIDAD/ÁREA'] = 'Tecnología'

    # print(df['UNIDAD/ÁREA'].unique())


    # cambio en la columna MOTIVO DE RETIRO y CAUSA DE TERMINACIÓN REPORTE PILAR

    termi_corr = ['Terminacion Unilateral de contrato', 'Terminacion Unilateral del contrato']
    # Crear si no existe
    if 'MOTIVO DE RETIRO' not in df.columns:
      df['MOTIVO DE RETIRO'] = df['TIPO DE DESVINCULACIÓN'] 
    # Llenar si esta NA
    df['MOTIVO DE RETIRO'] = df['MOTIVO DE RETIRO'].fillna('No reporta')
    df['CAUSA DE TERMINACIÓN REPORTE PILAR'] = df['CAUSA DE TERMINACIÓN REPORTE PILAR'].fillna('No reporta')
    # Ciclo de remplazo  
    for i in termi_corr:
       df.loc[df['MOTIVO DE RETIRO'].str.contains(i), 'MOTIVO DE RETIRO'] = 'Terminación Unilateral del contrato'
       df.loc[df['CAUSA DE TERMINACIÓN REPORTE PILAR'].str.contains(i), 'CAUSA DE TERMINACIÓN REPORTE PILAR'] = 'Terminación Unilateral del contrato'
    
    # print(df['MOTIVO DE RETIRO'].unique())


    #cambio de nombres de las columnas para subida a base de datos

    df = df.rename(
        columns = {
            'FECHA DE RETIRO' : 'fecha_retiro',
            'TIPO DE CONTRATACIÓN' : 'tipo_contrato',
            'FECHA DE INGRESO' : 'fecha_ingreso',
            'FUNCIÓN' : 'cargo', 
            'SEDE' : 'sede', 
            'UNIDAD/ÁREA' : 'unidad', 
            'MOTIVO DE RETIRO' : 'motivo_retiro',
            'CAUSA DE TERMINACIÓN REPORTE PILAR' : 'causa_terminacion',
            'nivel_cargo' : 'nivel_cargo'
        }
    )

    df = df[['fecha_retiro',
       'tipo_contrato',
       'sede',
       'unidad',
       'motivo_retiro',
       'causa_terminacion',
       'nivel_cargo',
       'cargo',
       'fecha_ingreso']]

    # Columnas de fecha como tipo string para cargue a base de datos
    date_col = ['fecha_retiro', 'fecha_ingreso']

    for i in date_col:
        df[i] = df[i].astype(str)

    nor_col = ['tipo_contrato','sede','unidad','motivo_retiro','causa_terminacion', 'cargo', 'nivel_cargo']

    for i in nor_col:
        df[i] = df[i].fillna('')
        df[i] = normalize_str_categorical(df[i])

    df = df.groupby(['fecha_retiro',
       'tipo_contrato',
       'sede',
       'unidad',
       'motivo_retiro',
       'causa_terminacion',
       'nivel_cargo',
       'cargo',
       'fecha_ingreso']).agg(no_retiros=('fecha_retiro', 'count')).reset_index()
       
    print(pd.__version__)

    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_THM_VYD_MotivosRetiro ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_tables(path)
    
    print(df)
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
# Se declara el DAG con sus respectivos parámetros
with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # se establece la ejecución a las 12:20 PM(Hora servidor) todos los sabados
    schedule_interval= '40 4 8 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_THM_VYD_MotivosRetiro_python_task = PythonOperator( task_id = "get_THM_VYD_MotivosRetiro",
                                                        python_callable = func_get_THM_VYD_MotivosRetiro,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_THM_VYD_MotivosRetiro = MsSqlOperator( task_id='Load_THM_VYD_MotivosRetiro',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql="EXECUTE sp_load_THM_VYD_MotivosRetiro",
                                            email_on_failure=True, 
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_THM_VYD_MotivosRetiro_python_task >> load_THM_VYD_MotivosRetiro >> task_end