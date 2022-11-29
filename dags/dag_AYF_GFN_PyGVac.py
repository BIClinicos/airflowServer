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
from utils import load_df_to_sql,search_for_file_prefix, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'gestionfinanciera'
dirname = '/opt/airflow/dags/files_gestion_financiera/'
filename = 'AYF_GFN_PyGVac.xlsx'
db_table = "AYF_GFN_PyGVac"
db_tmp_table = "tmp_AYF_GFN_PyGVac"
dag_name = 'dag_' + db_table


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Función de transformación de los archivos xlsx
def transform_tables (sheets, filename, dirname, container):

    # lectura del archivo
    df = pd.read_excel(filename, sheet_name=sheets, header = [9])

    # Limpieza del dataframe: eliminación de filas y columnas sobrantes
    df = df.rename(columns = {"Unnamed: 0" : "Concepto"})
    del_rows = ["Ingresos Netos", "Costos Totales", "Utilidad bruta", "% Margen", "Gastos Totales", "Utilidad Operativa", "EBITDA Ajustado"]
    df = df.dropna(subset = ["Concepto"])
    df = df[~df["Concepto"].isin(del_rows)]
    df = df.reset_index()
    df = df.drop(['index'], axis=1)
    df = df.drop(df.columns[25:], axis=1)
    df_m = df.columns[1:25]

    # Se anula la dinamización de las columnas de los meses para crear una sola columna de fecha y otra columna de monto
    df = pd.melt(df.reset_index(), id_vars="Concepto", value_vars=df_m, var_name='Fecha', value_name='Monto')

    # Se añade una columna nueva con el nombre de la Hoja
    df=df.assign(Hoja=sheets)
    df = df[["Hoja", "Concepto", "Fecha", "Monto"]]
    df = df.fillna(0)

    # Cambio de tipo de dato a las columnas.
    df_dates = df["Fecha"].str.split('-', expand=True)
    df_dates[0] = df_dates[0].map(
        {"Ene" : "01/01/20",
         "Feb" : "01/02/20",
         "Mar" : "01/03/20",
         "Abr" : "01/04/20",
         "May" : "01/05/20",
         "Jun" : "01/06/20",
         "Jul" : "01/07/20",
         "Ago" : "01/08/20",
         "Sep" : "01/09/20",
         "Oct" : "01/10/20",
         "Nov" : "01/11/20",
         "Dic" : "01/12/20",
        }
    )
    df_date = df_dates[0] + df_dates[1]
    df["Fecha"] = df_date
    df["Fecha"] = pd.to_datetime(df["Fecha"], format="%d/%m/%Y")
    return df

# Función que recorre las hojas dentro del archivo xlsx
def to_run_sheets_excel(filename, dirname, container):
    xls = pd.ExcelFile(filename)
    sheets = xls.sheet_names
    print(sheets)
    df = pd.DataFrame()
    for sheet in sheets:
        if sheet != "Ppto":
            df_hoja = transform_tables (sheet, filename, dirname, container)
            df = pd.concat([df_hoja, df])
    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_AYF_GFN_PyGVac ():

    path = dirname + filename
    file_get(path, container, filename, wb = wb)
    df = to_run_sheets_excel(path, dirname, container)
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
    # Se establece el cargue de los datos el primer día de cada mes.
    schedule_interval= '20 4 1 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_AYF_GFN_PyGVac_python_task = PythonOperator(task_id = "get_AYF_GFN_PyGVac",
        python_callable = func_get_AYF_GFN_PyGVac)
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_AYF_GFN_PyGVac = MsSqlOperator(task_id='Load_AYF_GFN_PyGVac',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_AYF_GFN_PyGVac",
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_AYF_GFN_PyGVac_python_task >> load_AYF_GFN_PyGVac >> task_end