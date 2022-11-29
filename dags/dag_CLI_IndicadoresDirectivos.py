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
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'n10directivo'
dirname = '/opt/airflow/dags/files_n10_directivo/'
filename = 'CLI_IndicadoresDirectivos.xlsx'
db_table = "CLI_IndicadoresDirectivos"
db_tmp_table = "tmp_CLI_IndicadoresDirectivos"
dag_name = 'dag_' + db_table


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_data (path):
    df = pd.read_excel(path, header = [7])
    df['Meta'] = df['Meta'].replace("Por definir","")
    df_metas = df['Meta'].str.split(' ', expand = True)
    print(df_metas.head(12))
    print(df_metas.dtypes)
    df_metas[2] = df_metas[1].str.contains('%', regex = True)
    df_metas[3] = df_metas[1].str.contains('M', regex = True)
    df_metas[4] = df_metas.apply(lambda z: '%' if (z[2] == True) else '$' if (z[3]== True) else 'Otro', axis = 1)
    df['unidad'] = df_metas[4]
    print(df_metas.head(12))
    df_metas[1] = df_metas[1].replace({'M':'', '%':'', ',':''}, regex=True)
    df_metas_1 = df_metas[1].str.split('$', expand = True)
    print(df_metas_1)

    for col, contenido in df_metas_1.items():
        df_metas_1[col] = pd.to_numeric(df_metas_1[col])
        df_metas_1[col] = df_metas_1[col].fillna(0)

    df_metas[1] = df_metas_1[0] + df_metas_1[1]
    df[['Tipo Meta','Valor Meta']] = df_metas[[0,1]]
    df['lim_1'] = df['Meta'].str.contains('%', regex=True)
    df['Valor Meta'] = df.apply(lambda x: x['Valor Meta'] if (x['lim_1'] == False) else x['Valor Meta'] * 0.01, axis = 1)
    df = df.drop(['Meta'], axis = 1)
    df = df.drop(['lim_1'], axis = 1)
    df_m = df.columns[5:-3]
    print(df_m)
    df = pd.melt(df.reset_index(), id_vars = ["#","Tema", "Indicador", "Calculo", "Responsable", "Tipo Meta", "Valor Meta", "unidad"], value_vars = df_m, var_name='Fecha', value_name='Valor')
    df = df.rename(
        columns = {
            '#' : "indice",
            'Tema' : "tema",
            'Indicador' : "indicadores",
            'Calculo' : "calculo",
            'Responsable' : "responsable",
            'Tipo Meta' : "tipo_meta",
            'Valor Meta' : "valor_meta",
            'Fecha' : "fecha",
            'unidad' : "unidad",
            'Valor' : "valor"
        }
    )
    print(df['fecha'].tail(5))
    # Filtro de indicadores en desuso (Noviembre 15 de 2022, David Cardenas)
    df = df[df['indice']!='x']
    df['indice'] = df['indice'].astype(int)
    #
    df['valor']=df['valor'].astype(str)
    df['valor']=df['valor'].replace({',':'.', 'Pendiente':'', 'nan':''}, regex=True)
    df['lim'] = df['valor'].str.contains('%', regex=True)
    df['valor']=df['valor'].str.replace('%','')
    df['valor']=pd.to_numeric(df['valor'])
    df['valor'] = df.apply(lambda x: x['valor'] if (x['lim'] == False) else x['valor'] * 0.01, axis = 1)
    df_con = df[['valor', 'tipo_meta', 'valor_meta']]
    df_con = df_con.astype(str)
    df_con['valor'] = df_con['valor'].replace('nan','0')
    df_con['valor_meta'] = df_con['valor_meta'].replace({'nan':'0'}, regex=True)
    print(df_con.head(30))
    df['cumplimiento'] = df_con.apply(lambda y: eval(y['valor'] + y['tipo_meta'] + y['valor_meta']), axis=1)
    print(df['cumplimiento'].head(30))
    df = df.drop(['lim'], axis = 1)
    
    return df


# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_CLI_IndicadoresDirectivos ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_data(path)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', -1)
    print(df.columns)
    print(df.dtypes)
    print(df)

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
    # Se establece la ejecución del dag los días martes a las 1:30 am(Hora servidor)
    schedule_interval= '40 5 * * 2',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_CLI_IndicadoresDirectivos_python_task = PythonOperator(task_id = "get_CLI_IndicadoresDirectivos",
                                        python_callable = func_get_CLI_IndicadoresDirectivos,
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag,
                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_CLI_IndicadoresDirectivos = MsSqlOperator(task_id='Load_CLI_IndicadoresDirectivos',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE sp_load_CLI_IndicadoresDirectivos",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_CLI_IndicadoresDirectivos_python_task >> load_CLI_IndicadoresDirectivos >> task_end