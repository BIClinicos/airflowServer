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
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get, remove_accents_cols, remove_special_chars, regular_camel_case, regular_snake_case, normalize_str_categorical

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'famisanar'
dirname = '/opt/airflow/dags/files_famisanar/'
filename = 'SAL_ESP_FA_Reumatologia.xlsx'
db_table = "SAL_ESP_FA_Reumatologia"
db_tmp_table = "tmp_SAL_ESP_FA_Reumatologia"
dag_name = 'dag_' + db_table


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_data (path):
    df = pd.read_excel(path, header = [0])

    print(df.dtypes)
    print(df.columns)


    df = df.rename(
        columns = {
            'FECHA REPORTE' : 'fecha_reporte', 
            'TIPO_DOC' : 'tipo_doc', 
            'IDENTIFICACIÓN' : 'identificacion', 
            'APELLIDO 1' : 'apellido_1',
            'APELLLIDO 2' : 'apellido_2', 
            'NOMBRE 1' : 'nombre_1', 
            'NOMBRE 2' : 'nombre_2', 
            'EDAD' : 'edad', 
            'TELÉFONO' : 'telefono', 
            'DIRECCIÓN' : 'direccion',
            'CLASIFICACIÓN' : 'clasificacion', 
            'CODIGO CIE-10' : 'codigo_cie_10', 
            'DIAGNOSTICO 1' : 'diagnostico_1', 
            'DIAGNOSTICO  2' : 'diagnostico_2',
            'FECHA DE DIAGNÓSTICO' : 'fecha_de_diagnostico', 
            'DAS 28' : 'das_28', 
            'FECHA DAS 28' : 'fecha_das_28',
            'ESTADIO O ACTIVIDAD  LA ENFERMEDAD' : 'estadio_o_actividad_la_enfermedad', 
            'HAQ' : 'haq', 
            'FECHA HAQ' : 'fecha_haq',
            'SE ENCUENTRA EN TERAPIA BIOLOGICA' : 'se_encuentra_en_terapia_biologica', 
            'BIOLOGICO 1' : 'biologico_1', 
            'DOSIS' : 'dosis',
            'VIA ADMINISTRACION' : 'via_administracion', 
            'ULTIMA FECHA ADMINISTRACION' : 'ultima_fecha_administracion', 
            'CICLO' : 'ciclo',
            'BIOLOGICO 2' : 'biologico_2', 
            'DOSIS.1' : 'dosis_1', 
            'VIA ADMINISTRACION.1' : 'via_administracion_1',
            'ULTIMA FECHA ADMINISTRACION.1' : 'ultima_fecha_administracion_1', 
            'CICLO.1' : 'ciclo_1',
            'REQUIRIO CAMBIO  BIOLOGICO ' : 'requirio_cambio_biologico', 
            'FECHA CAMBIO BIOLOGICO' : 'fecha_cambio_biologico', 
            'RECIBE DMARS' : 'recibe_dmars',
            'DMARD 1' : 'dmard_1', 
            'VIA ADMINISTRACION ' : 'via_administracion_2', 
            'DOSIS.2' : 'dosis_2', 
            'DMARD 2' : 'dmard_2',
            'VIA ADMINISTRACION.2' : 'via_administracion_3', 
            'DOSIS.3' : 'dosis_3', 
            'DMARD 3' : 'dmard_3', 
            'VIA ADMINISTRACION.3' : 'via_administracion_4',
            'DOSIS.4' : 'dosis_4', 
            'DMARD 4' : 'dmard_4', 
            'VIA ADMINISTRACION.4' : 'via_administracion_5', 
            'DOSIS.5' : 'dosis_5',
            'OTROS MEDICAMENTOS' : 'otros_medicamentos', 
            'OTROS MEDICAMENTOS.1' : 'otros_medicamentos_1', 
            'OTROS MEDICAMENTOS.2' : 'otros_medicamentos_2',
            'FECHA' : 'fecha', 
            'ACTIVIDAD RECIBIDA' : 'actividad_recibida', 
            'RAPID3' : 'rapid_3', 
            'FECHA RAPID3' : 'fecha_rapid_3',
            'ESTADIO O ACTIVIDAD  LA ENFERMEDAD RAPID3' : 'estadio_o_actividad_la_enfermedad_rapid_3'
        }
    )


    str_cols = [
        'biologico_1',
        'biologico_2',
        'dmard_1',
        'dmard_2',
        'dmard_3',
        'dmard_4'
    ]

    for i in str_cols:
        df[i] =  df[i].astype(str)

    date_col = [
        'fecha_reporte',
        'fecha_de_diagnostico',
        'fecha_das_28',
        'fecha_haq',
        'ultima_fecha_administracion',
        'ultima_fecha_administracion_1',
        'fecha_cambio_biologico',
        'fecha',
        'fecha_rapid_3'
    ]
    for i in date_col:
        df[i] = df[i].fillna('')
        df[i] = df[i].astype(str)
        df[i] = pd.to_datetime(df[i], errors='coerce')

    float_col = [
        'das_28',
        'haq',
        'rapid_3'
    ]

    for i in float_col:
        df[i] = df[i].replace(300, 0)
        df[i] = df[i].astype(str)
        df[i] = pd.to_numeric(df[i], errors='coerce')
        df[i] = df[i].fillna(0)

    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_SAL_ESP_FA_Reumatologia ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_data(path)

    print("antes de duplicados", df)


    df = df.drop_duplicates(subset=[
        'tipo_doc' , 
        'identificacion' , 
        'clasificacion' , 
        'codigo_cie_10' , 
        'estadio_o_actividad_la_enfermedad' , 
        'fecha' , 
        'actividad_recibida' , 
        'estadio_o_actividad_la_enfermedad_rapid_3'
       ]
    )
    print("después de duplicados", df)
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
    # Se establece el cargue de los datos el primer día de cada mes.
    schedule_interval= '50 3 1 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_SAL_ESP_FA_Reumatologia_python_task = PythonOperator(task_id = "get_SAL_ESP_FA_Reumatologia",
        python_callable = func_get_SAL_ESP_FA_Reumatologia,
        # email_on_failure=True,
        # email='BI@clinicos.com.co',
        # dag=dag,
        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_SAL_ESP_FA_Reumatologia = MsSqlOperator(task_id='Load_SAL_ESP_FA_Reumatologia',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_SAL_ESP_FA_Reumatologia",
                                    #    email_on_failure=True,
                                    #    email='BI@clinicos.com.co',
                                    #    dag=dag,
                                    )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> check_connection_task >> get_SAL_ESP_FA_Reumatologia_python_task >> load_SAL_ESP_FA_Reumatologia >> task_end