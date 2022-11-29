import os
import xlrd
import re
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from utils import remove_accents_cols, regular_snake_case, remove_special_chars, normalize_str_categorical
from datetime import datetime, timedelta, time
from datetime import date
import pandas as pd
import requests
import json
from pandas import read_excel, read_html, json_normalize
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,normalize_str_categorical
# from bs4 import BeautifulSoup


#  Se nombran las variables a utilizar en el dag
db_table = "BI_MIPRES_prescripcion"
db_tmp_table = 'TMP_BI_MIPRES_prescripcion'
#db_tmp_table2 = 'TMP_BI_MIPRESS_serviciosComplementarios'
db_table_meds = 'BI_MIPRES_medicamentos_prueba_mayo'
db_tmp_table_meds = 'TMP_BI_MIPRES_medicamentos_prueba_mayo'
#db_tmp_table4 = 'TMP_BI_MIPRESS_PrincipiosActivos'
#db_tmp_table5 = 'TMP_BI_MIPRESS_productosnutricionales'
#db_tmp_table6 = 'TMP_BI_MIPRESS_procedimientos'
dag_name = 'dag_' + 'BI_MIPRES_prueba_mayo'


# Se declaran los valores de los componentes necesarios para construir la URL con la cual hacemos el request a la API
nit='900496641'
date='2022-01-03'
token='148CA3F2-9233-411E-A728-76CE02ABFED5'

#se compone la URL con estas string previamente declaradas usando format.
url = f'https://wsmipres.sispro.gov.co/WSMIPRESNOPBS/api/Prescripcion/{nit}/{date}/{token}'

#esta función implementa el método get de la librería requests utilizando la URL
req = requests.get(url)

#esta función trae a req e implementa el método .json para traer al objeto en memoria una lista con un contenido en formato json
res = req.json()

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_BI_MIPRES():
    
    #dates = ['2022-01-31', '2022-02-28', '2022-03-31', '2022-04-30','2022-05-31', '2022-06-30', '2022-07-31']

    dates = pd.date_range(start='01/05/2022', end = '31/05/2022',freq='D')

    dates = dates.tolist()

    print(dates)
          
    dates2 = []
    for i in dates:
      dates2.append(i.strftime("%Y-%m-%d"))

    def charge_tables(dates2):
        nit='900496641'
        token='148CA3F2-9233-411E-A728-76CE02ABFED5'
        df_prescripcion = pd.DataFrame()
        df_servicios_complementarios = pd.DataFrame()
        df_meds = pd.DataFrame()
        df_principios_act = pd.DataFrame()
        df_principios_nutri = pd.DataFrame()
        df_procedimientos = pd.DataFrame()
        for date in dates2:
          url=f'https://wsmipres.sispro.gov.co/WSMIPRESNOPBS/api/Prescripcion/{nit}/{date}/{token}'
          req = requests.get(url)
          if req.status_code == 200:
              res = req.json()
              for obj in res:    
                No_prescripcion = obj['prescripcion']['NoPrescripcion']
                desc_data = pd.DataFrame([s['prescripcion'] for s in res])
                df_prescripcion = df_prescripcion.append(desc_data)
                serv_data = pd.json_normalize(res, record_path=['serviciosComplementarios'])
                serv_data['NoPrescripcion'] = No_prescripcion
                df_servicios_complementarios = df_servicios_complementarios.append(serv_data)
                med_data = pd.json_normalize(res, record_path=['medicamentos'], max_level=1)
                med_data['NoPrescripcion'] = No_prescripcion
                df_meds = df_meds.append(med_data)
                princip_act_data = pd.json_normalize(res, record_path=['medicamentos','PrincipiosActivos'], max_level=1)
                princip_act_data['NoPrescripcion'] = No_prescripcion
                df_principios_act = df_principios_act.append(princip_act_data)
                nutri_data = pd.json_normalize(res, record_path=['productosnutricionales'], max_level=1)
                nutri_data['NoPrescripcion'] = No_prescripcion
                df_principios_nutri = df_principios_nutri.append(nutri_data)
                proce_data = pd.DataFrame([s['procedimientos'] for s in res])
                proce_data['NoPrescripcion'] = No_prescripcion
                df_procedimientos = df_procedimientos.append(proce_data)
                proce_data['NoPrescripcion'] = No_prescripcion
                #tables = [df_servicios_complementarios, df_meds, df_principios_act, df_principios_nutri, df_procedimientos]
                #for i in tables: 
                  #i['NoPrescripcion'] = df_prescripcion['NoPrescripcion']
          else: 
              print("Request to {} failed".format(date))

        return df_prescripcion, df_servicios_complementarios, df_meds, df_principios_act, df_principios_nutri, df_procedimientos


    [df_prescripcion, df_servicios_complementarios, df_meds, df_principios_act, df_principios_nutri, df_procedimientos] = charge_tables(dates2)

    print(df_prescripcion.columns)

#Dado qué los nombres de las columnas en  los DataFrames son iguales a los de las tablas en SQL
# No se precisa de aplicar funciones de normalización de columnas
#df_meds.drop(['PrincipiosActivos'], axis=1, inplace=True)

    #df_prescripcion = df_prescripcion[['NoPrescripcion', 'FPrescripcion', 'HPrescripcion', 'CodHabIPS',
    #   'TipoIDIPS', 'NroIDIPS', 'CodDANEMunIPS', 'DirSedeIPS', 'TelSedeIPS',
    #   'TipoIDProf', 'NumIDProf', 'PNProfS', 'SNProfS', 'PAProfS', 'SAProfS',
    #   'RegProfS', 'TipoIDPaciente', 'NroIDPaciente', 'PNPaciente',
    #   'SNPaciente', 'PAPaciente', 'SAPaciente', 'CodAmbAte', 'RefAmbAte',
    #   'PacCovid19', 'EnfHuerfana', 'CodEnfHuerfana', 'EnfHuerfanaDX',
    #   'CodDxPpal', 'CodDxRel1', 'CodDxRel2', 'SopNutricional', 'CodEPS',
    #   'TipoIDMadrePaciente', 'NroIDMadrePaciente', 'TipoTransc',
    #   'TipoIDDonanteVivo', 'NroIDDonanteVivo', 'EstPres','FechaProceso']]

    df_meds.drop(['PrincipiosActivos','IndicacionesUNIRS'], axis=1, inplace=True)   
    
    df_meds = df_meds[['ConOrden', 'TipoMed', 'TipoPrest', 'CausaS1', 'CausaS2', 'CausaS3',
       'MedPBSUtilizado', 'RznCausaS31', 'DescRzn31', 'RznCausaS32',
       'DescRzn32', 'CausaS4', 'MedPBSDescartado', 'RznCausaS41', 'DescRzn41',
       'RznCausaS42', 'DescRzn42', 'RznCausaS43', 'DescRzn43', 'RznCausaS44',
       'DescRzn44', 'CausaS5', 'RznCausaS5', 'CausaS6', 'DescMedPrinAct',
       'CodFF', 'CodVA', 'JustNoPBS', 'Dosis', 'DosisUM', 'NoFAdmon',
       'CodFreAdmon', 'IndEsp', 'CanTrat', 'DurTrat', 'CantTotalF',
       'UFCantTotal', 'IndRec', 'EstJM','NoPrescripcion']]
    
    float_col_meds = [
        'ConOrden',
        'TipoMed',
        'TipoPrest',
        'CodFreAdmon',
        'IndEsp',
        'DurTrat',
        'EstJM',
    ]

    for i in float_col_meds:
        df_meds[i] = df_meds[i].astype(str)
        df_meds[i] = df_meds[i].replace(' ','')
        df_meds[i] = pd.to_numeric(df_meds[i], errors='coerce')


    #df_servicios_complementarios = df_servicios_complementarios[['ConOrden', 'TipoPrest', 'CausaS1', 'CausaS2', 'CausaS3', 'CausaS4',
    #   'DescCausaS4', 'CausaS5', 'CodSerComp', 'DescSerComp', 'CanForm',
    #   'CadaFreUso', 'CodFreUso', 'Cant', 'CantTotal', 'CodPerDurTrat',
    #   'TipoTrans', 'ReqAcom', 'TipoIDAcomAlb', 'NroIDAcomAlb',
    #   'ParentAcomAlb', 'NombAlb', 'CodMunOriAlb', 'CodMunDesAlb', 'JustNoPBS',
    #   'IndRec', 'EstJM','NoPrescripcion']]
    
    #df_principios_act = df_principios_act[['ConOrden', 'CodPriAct', 'ConcCant', 'UMedConc', 'CantCont',
    #   'UMedCantCont','NoPrescripcion']]
    
    #df_principios_nutri = df_principios_nutri['ConOrden', 'TipoPrest', 'CausaS1', 'CausaS2', 'CausaS3', 'CausaS4',
    #   'ProNutUtilizado', 'RznCausaS41', 'DescRzn41', 'RznCausaS42',
    #   'DescRzn42', 'CausaS5', 'ProNutDescartado', 'RznCausaS51', 'DescRzn51',
    #   'RznCausaS52', 'DescRzn52', 'RznCausaS53', 'DescRzn53', 'RznCausaS54',
    #   'DescRzn54', 'DXEnfHuer', 'DXVIH', 'DXCaPal', 'DXEnfRCEV', 'DXDesPro',
    #   'TippProNut', 'DescProdNutr', 'CodForma', 'CodViaAdmon', 'JustNoPBS',
    #   'Dosis', 'DosisUM', 'NoFAdmon', 'CodFreAdmon', 'IndEsp', 'CanTrat',
    #   'DurTrat', 'CantTotalF', 'UFCantTotal', 'IndRec', 'NoPrescAso',
    #   'EstJM','NoPrescripcion']

    print(df_prescripcion.dtypes)
    print(df_prescripcion.columns)
    print(df_prescripcion)
    print('columnas de las tablas',df_prescripcion.columns) 
    
    df_meds = df_meds.drop_duplicates()

    if ~df_meds.empty and len(df_meds.columns) >0:
        load_df_to_sql(df_meds, db_tmp_table_meds, sql_connid)

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
    schedule_interval= None,
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    func_get_BI_MIPRES_python_task = PythonOperator(task_id = "get_BI_MIPRES",
                                                python_callable = func_get_BI_MIPRES,
                                                #email_on_failure=True, 
                                                #email='BI@clinicos.com.co',
                                                #dag=dag,
                                                )

    #Se declara la función encargada de ejecutar el "Stored Procedure"
    #Load_BI_MIPRES_prescripcion = MsSqlOperator(task_id='Load_BI_MIPRES_prescripcion',
    #                                  mssql_conn_id=sql_connid,
    #                                  autocommit=True,
    #                                  sql="EXECUTE sp_load_BI_MIPRES_prescripcion",
    #                                  #email_on_failure=True, 
    #                                  #email='BI@clinicos.com.co',
    #                                  #dag=dag,
    #                                  )

    #Se declara la función encargada de ejecutar el "Stored Procedure"
    Load_BI_MIPRES_medicamentos_prueba_mayo = MsSqlOperator(task_id='Load_BI_MIPRES_medicamentos_prueba_mayo',
                                      mssql_conn_id=sql_connid,
                                      autocommit=True,
                                      sql="EXECUTE sp_load_BI_MIPRES_medicamentos_prueba_mayo",
                                      #email_on_failure=True, 
                                      #email='BI@clinicos.com.co',
                                      #dag=dag,
                                      )

    #Se declara la función encargada de ejecutar el "Stored Procedure"
    #Load_BI_MIPRES_serviciosComplementarios = MsSqlOperator(task_id='Load_BI_MIPRES_serviciosComplementarios',
    #                                  mssql_conn_id=sql_connid,
    #                                  autocommit=True,
    #                                  sql="EXECUTE sp_load_BI_MIPRES_serviciosComplementarios",
    #                                  #email_on_failure=True, 
    #                                  #email='BI@clinicos.com.co',
    #                                  #dag=dag,
    #                                  )
    
    #Se declara la función encargada de ejecutar el "Stored Procedure"
    #Load_BI_MIPRES_PrincipiosActivos = MsSqlOperator(task_id='Load_BI_MIPRES_PrincipiosActivos',
    #                                  mssql_conn_id=sql_connid,
    #                                  autocommit=True,
    #                                  sql="EXECUTE sp_load_BI_MIPRES_PrincipiosActivos",
    #                                  #email_on_failure=True, 
    #                                  #email='BI@clinicos.com.co',
    #                                  #dag=dag,
    #                                  )
    
    #Se declara la función encargada de ejecutar el "Stored Procedure"
    #Load_BI_MIPRES_productosnutricionales = MsSqlOperator(task_id='Load_BI_MIPRES_productosnutricionales',
    #                                  mssql_conn_id=sql_connid,
    #                                  autocommit=True,
    #                                  sql="EXECUTE sp_load_BI_MIPRES_productosnutricionales",
    #                                  #email_on_failure=True, 
    #                                  #email='BI@clinicos.com.co',
    #                                  #dag=dag,
    #                                  )
    
    #Se declara la función encargada de ejecutar el "Stored Procedure"
    #Load_BI_MIPRES_procedimientos = MsSqlOperator(task_id='Load_BI_MIPRES_procedimientos',
    #                                  mssql_conn_id=sql_connid,
    #                                  autocommit=True,
    #                                  sql="EXECUTE sp_load_BI_MIPRES_procedimientos",
    #                                  #email_on_failure=True, 
    #                                  #email='BI@clinicos.com.co',
    #                                  #dag=dag,
    #                                  )

    
    
    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> func_get_BI_MIPRES_python_task  >> Load_BI_MIPRES_medicamentos_prueba_mayo >> task_end