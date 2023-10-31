from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import date
import pandas as pd
from variables import sql_connid
from utils import open_xls_as_xlsx,load_df_to_sql,file_get, red_excel_big, excel_date_format, remove_accents_cols, remove_special_chars, regular_snake_case
import xlrd
import numpy as np
from pandas.api.types import is_string_dtype

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'rotacionpersonas'
dirname = '/opt/airflow/dags/files_rotacion_personas/'
filename = 'THM_VYD_OPS.xlsx'
db_table = "THM_VYD_OPS"
db_tmp_table = "tmp_THM_VYD_OPS"
dag_name = 'dag_' + db_table
container_to = 'historicos'


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_table(path):    
    ### Adicion 20230323 - Lectura de todas las hojas y union
    dfs:list[pd.DataFrame] = red_excel_big(path, ["OPS CLÍNICOS","PERSONAL RETIRADO OPS CLINICOS","OPS INNOVAR","PERSONAL RETIRADO OPS INNOVAR"])
    print("Lectura del archivo completo en xlrd")
    print("Archivos cargados con Pandas")
    ###
    for indx in dfs.keys():
        dfs[indx] = dfs[indx].drop(dfs[indx].columns[31:] ,axis=1)
        if 'INNOVAR' in indx:
            dfs[indx]['organizacion'] = 'INNOVAR'
        else:
            dfs[indx]['organizacion'] = 'CLÍNICOS'
        dfs[indx].columns = remove_accents_cols(dfs[indx].columns)
        dfs[indx].columns = remove_special_chars(dfs[indx].columns)
        dfs[indx].columns = regular_snake_case(dfs[indx].columns)

    df = pd.concat(dfs, ignore_index= True)
    print(df.columns) 
    # Eliminacion de registros nulos
    df = df.loc[(df['id'] != '')]
    # Merge del mismo nombre
    # df = df.groupby(level=0, axis=1).apply(lambda x: x.apply(lambda x: ','.join(x[x.notnull()].astype(str)), axis=1))
    #df = df.groupby(level=0, axis=1).agg(lambda x: ','.join(x.dropna().astype(str)))
    #print("Merge de dataframes - sospecha")
    print(df.head(5))
    print(df.columns)
    ### Adicion 20230323
    if 'apellidos_y_nombre' in df.columns:
        df['nombre_completo'] = df['apellidos_y_nombre'].fillna(df['nombre_completo'])
    df = df[df['nombre_completo'].notnull()]

    # Eliminacion de columnas adicionales
    #df_col_adi = df.columns[30:]
    #df = df.drop(df_col_adi ,axis=1)

    df['especialidad'] = df['funcion'].str.upper()

    df['inicio_contrato'] = df['inicio_contrato'].astype(str)
    df['inicio_contrato'] = df['inicio_contrato'].str.strip()
    # df['inicio_contrato'] = df['inicio_contrato'].replace(' ', '')
    df['inicio_contrato'] = df['inicio_contrato'].apply(lambda x: x.replace("/","-"))
    df['inicio_contrato'] = df['inicio_contrato'].apply(lambda x: x.replace(" 00:00:00",""))
    df['inicio_contrato'] = df['inicio_contrato'].apply(lambda x: x.replace("19-06-2019","2019-06-19"))

    df['fecha_verificacion_titulo'] = df['fecha_verificacion_titulo'].replace('...', '')
    df['fecha_verificacion_titulo'] = df['fecha_verificacion_titulo'].replace('.', '')
    # df['fecha_verificacion_titulo'] = pd.to_datetime(df['fecha_verificacion_titulo'], format="%d-%m-%Y", errors='coerce')
    print("Procesamiento 1")
    date_columns = ['inicio_contrato','fecha_nacimiento','fecha_verificacion_titulo','fecha_retiro']
    print(df['inicio_contrato'].head(5))
    print(set(df['fecha_retiro']))
    for i in date_columns:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.strip()
        df[i] = excel_date_format(df,i)

    print(df['fecha_verificacion_titulo'].head(50))

    ## Fecha de ingreso clinicos. Solicitud 20230321
    df['fecha_ingreso_clinicos'] = [date.fromisoformat('2022-10-01') if x < date.fromisoformat('2022-10-01') and y == 'INNOVAR' else x for (x,y) in zip(df['inicio_contrato'],df['organizacion'])]
    df['sede'] = df['sede'].str.lower()
    df['sede'] = df['sede'].fillna('')
    df.loc[df['sede'].str.contains('américas'), 'sede'] = 'americas'
    df.loc[df['sede'].str.contains('domiciliaria'), 'sede'] = 'americas'
    df.loc[df['sede'].str.contains('teleorientación'), 'sede'] = 'teleorientación'
    df.loc[df['sede'].str.contains('remoto'), 'sede'] = 'trabajo en casa'
    df.loc[df['sede'].str.contains('remota'), 'sede'] = 'trabajo en casa'
    df.loc[df['sede'].str.contains(','), 'sede'] = 'varios'
    df.loc[df['sede'].str.contains(';'), 'sede'] = 'varios'
    df.loc[df['sede'].str.contains(' y '), 'sede'] = 'varios'
    df['sede'] = df['sede'].str.capitalize()

    print(df['sede'].unique())
    print("Procesamiento 2")
    ### Adicion 20230323
    df['unidad_para_informe_mensual'] = df['unidad_de_negocio_principal']

    df['unidad_para_informe_mensual'] = df['unidad_para_informe_mensual'].fillna('')
    df['unidad_para_informe_mensual'] = df['unidad_para_informe_mensual'].str.replace('[^\x00-\x7F]','')
    df.loc[df['unidad_para_informe_mensual'].str.contains('(Administrativa)+'), 'unidad_para_informe_mensual'] = 'Unidad Administrativa'
    df.loc[df['unidad_para_informe_mensual'].str.contains('Gerencia'), 'unidad_para_informe_mensual'] = 'Unidad Administrativa'
    df.loc[df['unidad_para_informe_mensual'].str.contains('Proyecto Merk'), 'unidad_para_informe_mensual'] = 'Unidad Administrativa'
    df.loc[df['unidad_para_informe_mensual'].str.contains('(Domiciliaria)+', case = False), 'unidad_para_informe_mensual'] = 'Unidad Domiciliaria'
    df.loc[df['unidad_para_informe_mensual'].str.contains('(Primaria)+'), 'unidad_para_informe_mensual'] = 'Unidad Primaria'
    df.loc[df['unidad_para_informe_mensual'].str.contains('(Especializada)+'), 'unidad_para_informe_mensual'] = 'Unidad Especializada'
    df.loc[df['unidad_para_informe_mensual'].str.contains('(Cientf)+'), 'unidad_para_informe_mensual'] = 'VICEPRESIDENCIA CIENTIFICA'
    df['unidad_para_informe_mensual'] = df['unidad_para_informe_mensual'].str.strip()
    df['unidad_para_informe_mensual'] = df['unidad_para_informe_mensual'].str.upper()

    df['unidad_de_negocio'] = df['unidad_para_informe_mensual']
    print(df['unidad_para_informe_mensual'])
    print(df['unidad_para_informe_mensual'].unique())

    df['valor_contrato'] = df['valor_contrato'].fillna(df['valor_contrato_principal'])
    df['valor_contrato'] = df['valor_contrato'].astype(str)

    df_valor = df['valor_contrato'].str.split(' /', n=1, expand = True)
    df['valor_contrato'] = df_valor[0]
    df['valor_contrato'] = df['valor_contrato'].apply(lambda x: x.replace("$ ",""))
    df['valor_contrato'] = pd.to_numeric(df['valor_contrato'], errors = 'coerce')
    ### Adicion 20230323
    adj_cols = [col for col in df.columns if 'tarifa_ajustada' in col]
    print(df.info())
    if len(adj_cols)>0:
        df_valor = df[adj_cols[0]].str.split(' /', n=1, expand = True)
        df['tarifa_ajustada'] = df_valor[0]
        if is_string_dtype(df['tarifa_ajustada']):
            try:
                df['tarifa_ajustada'] = df['tarifa_ajustada'].str.replace("\$\s*","",regex=True)
                df['tarifa_ajustada'] = df['tarifa_ajustada'].str.replace("[\.\,]","", regex=True)
                df['tarifa_ajustada'] = df['tarifa_ajustada'].str.strip()
            except AttributeError:
                print('Problema de datos que pasan como reales')
        df['tarifa_ajustada'] = pd.to_numeric(df['tarifa_ajustada'], errors = 'coerce')
        df['valor_contrato'] = df['tarifa_ajustada'].fillna(df['valor_contrato'])
    df['valor_contrato'] = [x * 1000 if x< 500 else x for x in df['valor_contrato']]

    print(df_valor.head(20))
    print(df['valor_contrato'].head(20))
    print(df['valor_contrato'].dtypes)

    df['tipo_cuenta'] = df['tipo_cuenta'].fillna('')
    df.loc[df['tipo_cuenta'].str.contains('Compensar'), 'tipo_cuenta'] = ''

    print(df['tipo_cuenta'].head(20))
    print(df['tipo_cuenta'].unique())
    print("Procesamiento 3")
    ### Adicion 20230323
    df['estado'] = df['estado'].str.split(',').str[0]
    df['estado_activo_inactivo_retirado'] = df['estado']
    df['tipo_id'] = df['tipo_id'].fillna('CC') 
    df['id'] = df['id'].astype(str)
    df['id'] = df['id'].str.slice(0,20)
    df['id'] = df['id'].str.replace('\..*','')
    df['direccion'] = df['direccion'].str[:100]

    df = df[['tipo_id','id','nombre_completo','especialidad','sede','inicio_contrato',
        'valor_contrato','unidad_de_negocio','unidad_para_informe_mensual',
        'fecha_nacimiento','lugar_expedicion_id','rh','correo','telefono','direccion',
        'banco','numero_cuenta','tipo_cuenta','eps','pension','arl',
        'fecha_verificacion_titulo','estado_activo_inactivo_retirado','fecha_retiro',
        'observaciones','fecha_ingreso_clinicos', 'organizacion']]   

    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_THM_VYD_OPS ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_table(path)
    print(df.columns)
    print(df.dtypes)
    print(df.head(30))

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_table, sql_connid)


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
    # Se establece la ejecución del dag todos los viernes a las 12:40 am(Hora servidor)
    schedule_interval= '40 10 * * fri',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_THM_VYD_OPS_python_task = PythonOperator(task_id = "get_THM_VYD_OPS",
                                                python_callable = func_get_THM_VYD_OPS,
                                                email_on_failure=True, 
                                                email='BI@clinicos.com.co',
                                                dag=dag
                                                )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_THM_VYD_OPS_python_task >> task_end