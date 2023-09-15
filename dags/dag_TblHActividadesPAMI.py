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
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_contains_name,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'pami-ecopetrol'
dirname = '/opt/airflow/dags/files_pami/'
filenames = ['Enfermeria.xlsx', 'PAMIResolutividad.xlsx', 'TerapiasDomi.xlsx', 'TeleSalud.xlsx']
db_tables = ['TblDConceptosPAMI', 'TblDConceptoDetallePAMI', 'TblHActividadesPAMI']
db_tmp_table = 'TmpTblHActividadesPAMI'
dag_name = 'dag_TblHActividadesPAMI'
container_to = 'historicos'


# Se realiza un chequeo de la conexión al blob storage
def check_connection(container, filename):
    print('Conexión OK')
    return wb.check_for_blob(container,filename)

# Función de transformación de los archivos xlsx
def transform_table(dir, filenames):
    # Lectura y transformacion de df
    df_nursery = pd.read_excel(dir + filenames[0], header = [0], sheet_name = 0)
    df_nursery['concepto'] = 'Enfermeria'
    df_nursery.columns = ['sede', 'fecha', 'detalle', 'cantidad', 'concepto']
    df_nursery = df_nursery[['fecha', 'detalle', 'cantidad', 'concepto', 'sede']]
    #
    df_pam_ind = pd.read_excel(dir + filenames[1], header = [0], sheet_name = 0)
    df_pam_ind = pd.melt(frame = df_pam_ind, id_vars = ['MES'], var_name = 'detalle', value_name = 'cantidad')
    df_pam_ind['concepto'] = 'Resolutividad'
    df_pam_ind['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_pam_ind.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']
    #
    df_therapy = pd.read_excel(dir + filenames[2], header = [0], sheet_name = 0)
    df_therapy['concepto'] = 'Terapias'
    df_therapy['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_therapy.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']
    #
    df_tele_ind1 = pd.read_excel(dir + filenames[3], header = [0], sheet_name = 'NivelServicio')
    df_tele_ind1 = pd.melt(frame = df_tele_ind1, id_vars = ['MES'], var_name = 'detalle', value_name = 'cantidad')
    df_tele_ind1['concepto'] = 'Telesalud - Nivel Servicio'
    df_tele_ind1['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_tele_ind1.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']    
    #
    df_tele_ind2 = pd.read_excel(dir + filenames[3], header = [0], sheet_name = 'TeleSalud')
    df_tele_ind2['concepto'] = 'Telesalud - Atenciones'
    df_tele_ind2['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_tele_ind2.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']
    #
    df_tele_ind3 = pd.read_excel(dir + filenames[3], header = [0], sheet_name = 'TeleSaludSat')
    df_tele_ind3 = pd.melt(frame = df_tele_ind3, id_vars = ['MES', 'Concepto'], var_name = 'detalle', value_name = 'cantidad')
    df_tele_ind3.columns = ['fecha', 'detalle ext', 'detalle', 'cantidad']    
    df_tele_ind3['detalle'] = df_tele_ind3['detalle ext'] + '-' + df_tele_ind3['detalle'] 
    df_tele_ind3['concepto'] = 'Telesalud - Nivel Servicio'
    df_tele_ind3['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_tele_ind3 = df_tele_ind3[['fecha', 'detalle', 'cantidad', 'concepto', 'sede']]    
    #
    df_tele_ind4 = pd.read_excel(dir + filenames[3], header = [0], sheet_name = 'TeleorientacionLlamadas')
    df_tele_ind4['concepto'] = 'Teleorientacion - Llamadas'
    df_tele_ind4['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_tele_ind4.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']
    #
    df_tele_ind5 = pd.read_excel(dir + filenames[3], header = [0], sheet_name = 'TeleorientacionCovid')
    df_tele_ind5['concepto'] = 'Teleorientacion - Covid'
    df_tele_ind5['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_tele_ind5.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']
    #
    df_tele_ind6 = pd.read_excel(dir + filenames[3], header = [0], sheet_name = 'LineaSaludResolutividad')
    df_tele_ind6 = pd.melt(frame = df_tele_ind6, id_vars = ['MES'], var_name = 'detalle', value_name = 'cantidad')
    df_tele_ind6['concepto'] = 'Lineasalud - Resolutividad'
    df_tele_ind6['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_tele_ind6.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']    
    #
    df_tele_ind7 = pd.read_excel(dir + filenames[3], header = [0], sheet_name = 'LineaSaludTriage')
    df_tele_ind7 = pd.melt(frame = df_tele_ind7, id_vars = ['FECHA '], var_name = 'detalle', value_name = 'cantidad')
    df_tele_ind7['concepto'] = 'Lineasalud - Triage'
    df_tele_ind7['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_tele_ind7.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']    
    #
    df_tele_ind8 = pd.read_excel(dir + filenames[3], header = [0], sheet_name = 'LineaSalud')
    df_tele_ind8['concepto'] = 'Lineasalud - Atenciones'
    df_tele_ind8['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_tele_ind8.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']    
    #
    df_tele_ind9 = pd.read_excel(dir + filenames[3], header = [0], sheet_name = 'Perfil')
    df_tele_ind9['concepto'] = 'Lineasalud - Perfil'
    df_tele_ind9['sede'] = 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS'
    df_tele_ind9.columns = ['fecha', 'detalle', 'cantidad', 'concepto', 'sede']    
    ### Concatenar
    df = pd.concat([df_nursery, df_pam_ind, df_therapy, df_tele_ind1, df_tele_ind2, df_tele_ind3, df_tele_ind4, 
                    df_tele_ind5, df_tele_ind6, df_tele_ind7, df_tele_ind8, df_tele_ind9])
    ### Cambiar de formato
    df['fecha'] = pd.to_datetime(df['fecha'], format='%Y/%m/%d')
    ### Cambiar sede
    dict_replace ={
        'Bulevar':'Clínicos IPS Sede Bulevar',
        'San Martin':'Clínicos IPS Sede San Martín',
        'Cartagnera':'Clínicos IPS Sede Cartagena'
    }
    df = df.replace({'sede':dict_replace})
    ### Llenar cantidades nulas y transformar a enteros
    df['cantidad'] = df['cantidad'].fillna(0)
    df['cantidad'] = df['cantidad'].astype(int)
    ### Acomodar columnas
    df = df[['fecha', 'concepto', 'detalle', 'sede', 'cantidad']]
    print(df.info())
    print(df.head(5))
    return df


# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_TblHActividadesPAMI():
    #
    for filename in filenames:
        path = dirname + filename
        print(path)
        check_connection(container, filename)
        file_get(path, container,filename, wb = wb)
    #
    df = transform_table(dirname, filenames)
    print(df.columns)
    print(df.dtypes)
    print(df.tail(50))
    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)

    #for filename in filenames:
    #    move_to_history_contains_name(dirname, container, filename, container_to)


# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'CLÍNICOS PROGRAMAS DE ATENCIÓN INTEGRAL SAS IPS',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}
# Se declara el DAG con sus respectivos parámetros
with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '0 0 11 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_tmp_TblHActividadesPAMI = PythonOperator(task_id = "Get_tmp_TblHActividadesPAMI",
                                                  python_callable = func_get_TblHActividadesPAMI,
                                                  email_on_failure=True, 
                                                  email='BI@clinicos.com.co',
                                                  dag=dag,
                                                  )
    
    # Carge del dimensional conceptos
    load_TblDConceptosPAMI = MsSqlOperator(task_id ='Load_TblDConceptosPAMI',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblDConceptosPAMI",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )
    
    # Carge del dimensional detalle
    load_TblDConceptoDetallePAMI = MsSqlOperator(task_id ='Load_TblDConceptoDetallePAMI',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblDConceptoDetallePAMI",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )

    # Carge de hechos, actividades PAMI
    load_TblHActividadesPAMI = MsSqlOperator(task_id ='Load_TblHActividadesPAMI',
                                       mssql_conn_id = sql_connid,
                                       autocommit = True,
                                       sql = "EXECUTE uspCarga_TblHActividadesPAMI",
                                       email_on_failure = True, 
                                       email = 'BI@clinicos.com.co',
                                       dag = dag,
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_tmp_TblHActividadesPAMI >> load_TblDConceptosPAMI >> load_TblDConceptoDetallePAMI >> load_TblHActividadesPAMI >> task_end