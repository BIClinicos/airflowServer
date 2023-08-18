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
import json
import requests
from pandas import read_excel, read_html, json_normalize
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,normalize_str_categorical
# from bs4 import BeautifulSoup


wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
#  Se nombran las variables a utilizar en el dag
db_table = "BI_MIPRES_prescripcion"
db_tmp_table = 'TMP_BI_MIPRES_prescripcion'
db_tmp_table2 = 'TMP_BI_MIPRES_serviciosComplementarios'
db_tmp_tablemayo = 'TMP_BI_MIPRES_medicamentos_prueba_mayo'
db_table_meds = 'BI_MIPRES_medicamentos_prueba_mayo'
db_tmp_table4 = 'TMP_BI_MIPRES_PrincipiosActivos'
db_tmp_table5 = 'TMP_BI_MIPRES_productosnutricionales'
db_tmp_table6 = 'TMP_BI_MIPRES_procedimientos'
dag_name = 'dag_' + 'BI_MIPRES_prescripcion_pruebas'

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_BI_MIPRES_prescripcion():
    
    #dates = ['2022-01-31', '2022-02-28', '2022-03-31', '2022-04-30','2022-05-31', '2022-06-30', '2022-07-31']

    dates = pd.date_range(start='01/05/2022', end ='31/05/2022',freq='D')

    dates = dates.tolist()

    print(dates)

    #dates2 = map(lambda x: x.strftime("%Y-%m-%d"), dates)
    dates2 = []
    for i in dates:
      dates2.append(i.strftime("%Y-%m-%d"))
      
    print(dates2)

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

    
    # Transformación de datos de tabla de prescripción

    #se procesa columnas para llevarlas a datetime, dado qué luego de leer la API, pandas las interpreta cómo tipo string
    date_columns = ['FPrescripcion']

    date_columns = ['FPrescripcion']
    for i in date_columns:
        df_prescripcion[i] = df_prescripcion[i].astype(str)
        df_prescripcion[i] = df_prescripcion[i].str.strip()
        df_prescripcion[i] = pd.to_datetime(df_prescripcion[i], format="%Y-%m-%d", errors = 'coerce')
    
    df_prescripcion['HPrescripcion'] = pd.to_datetime(df_prescripcion['HPrescripcion'], format='%H:%M:%S')
    # df_prescripcion['HPrescripcion'] = df_prescripcion['HPrescripcion'].dt.strftime('%H:%M:%S')
    #df_prescripcion.update(df_prescripcion[['HPrescripcion']].applymap('"{}"'.format))

    df_prescripcion['FechaProceso'] = datetime.now().strftime("%Y-%m-%d")
    df_prescripcion['FechaProceso'] = pd.to_datetime(df_prescripcion['FechaProceso'], format="%Y-%m-%d", errors = 'coerce')

    df_prescripcion = df_prescripcion.drop_duplicates(subset=['NoPrescripcion','FPrescripcion','CodHabIPS','PAPaciente','CodDxPpal','CodEPS'])

    df_prescripcion = df_prescripcion[['NoPrescripcion', 'FPrescripcion', 'HPrescripcion', 'CodHabIPS',
       'TipoIDIPS', 'NroIDIPS', 'CodDANEMunIPS', 'DirSedeIPS', 'TelSedeIPS',
       'TipoIDProf', 'NumIDProf', 'PNProfS', 'SNProfS', 'PAProfS', 'SAProfS',
       'RegProfS', 'TipoIDPaciente', 'NroIDPaciente', 'PNPaciente',
       'SNPaciente', 'PAPaciente', 'SAPaciente', 'CodAmbAte', 'RefAmbAte',
       'PacCovid19', 'EnfHuerfana', 'CodEnfHuerfana', 'EnfHuerfanaDX',
       'CodDxPpal', 'CodDxRel1', 'CodDxRel2', 'SopNutricional', 'CodEPS',
       'TipoIDMadrePaciente', 'NroIDMadrePaciente', 'TipoTransc',
       'TipoIDDonanteVivo', 'NroIDDonanteVivo', 'EstPres','FechaProceso']]

    print(df_prescripcion)
    print(df_prescripcion.dtypes)
    print(df_prescripcion['HPrescripcion'])
    print(df_prescripcion['FechaProceso'])
    # df_prescripcion.to_excel('/opt/airflow/dags/generated_files/output.xlsx')

    
    # Transformación de datos de tabla de Medicamentos

    float_col_meds = [
    'ConOrden',
    'TipoMed',
    'TipoPrest',
    'CodFreAdmon',
    'IndEsp',
    'DurTrat',
    'EstJM'
    ]
    
    df_meds[float_col_meds] = df_meds[float_col_meds].astype('int')

    df_meds.drop(['PrincipiosActivos','IndicacionesUNIRS'], axis=1, inplace=True)   
    
    df_meds = df_meds[['ConOrden', 'TipoMed', 'TipoPrest', 'CausaS1', 'CausaS2', 'CausaS3',
       'MedPBSUtilizado', 'RznCausaS31', 'DescRzn31', 'RznCausaS32',
       'DescRzn32', 'CausaS4', 'MedPBSDescartado', 'RznCausaS41', 'DescRzn41',
       'RznCausaS42', 'DescRzn42', 'RznCausaS43', 'DescRzn43', 'RznCausaS44',
       'DescRzn44', 'CausaS5', 'RznCausaS5', 'CausaS6', 'DescMedPrinAct',
       'CodFF', 'CodVA', 'JustNoPBS', 'Dosis', 'DosisUM', 'NoFAdmon',
       'CodFreAdmon', 'IndEsp', 'CanTrat', 'DurTrat', 'CantTotalF',
       'UFCantTotal', 'IndRec', 'EstJM','NoPrescripcion']]

    # Transformación de datos de tabla de Servicios complementarios

    df_servicios_complementarios = df_servicios_complementarios[['ConOrden', 'TipoPrest', 'CausaS1', 'CausaS2', 'CausaS3', 'CausaS4',
      'DescCausaS4', 'CausaS5', 'CodSerComp', 'DescSerComp', 'CanForm',
      'CadaFreUso', 'CodFreUso', 'Cant', 'CantTotal', 'CodPerDurTrat',
      'TipoTrans', 'ReqAcom', 'TipoIDAcomAlb', 'NroIDAcomAlb',
      'ParentAcomAlb', 'NombAlb', 'CodMunOriAlb', 'CodMunDesAlb', 'JustNoPBS',
      'IndRec', 'EstJM','NoPrescripcion']]
    
    toint_cols = ['CanForm','CadaFreUso','Cant','CantTotal']
    df_servicios_complementarios[toint_cols] = df_servicios_complementarios[toint_cols].apply(pd.to_numeric, errors='coerce', axis=1)

    # Transformación de datos de tabla de principios activos
    
    df_principios_act = df_principios_act[['ConOrden', 'CodPriAct', 'ConcCant', 'UMedConc', 'CantCont',
      'UMedCantCont','NoPrescripcion']]
    
    # to_int_cols_act = ['ConcCant','CantCont']
    # df_principios_act[to_int_cols_act] = df_principios_act[to_int_cols_act].apply(pd.to_numeric, errors='coerce', axis=1)
    df_principios_act.to_excel('/opt/airflow/dags/generated_files/output_df_principios_act.xlsx')

    #Transformación de datos de tabla de Productos nutricionales

    df_principios_nutri = df_principios_nutri[['ConOrden', 'TipoPrest', 'CausaS1', 'CausaS2', 'CausaS3', 'CausaS4',
      'ProNutUtilizado', 'RznCausaS41', 'DescRzn41', 'RznCausaS42',
      'DescRzn42', 'CausaS5', 'ProNutDescartado', 'RznCausaS51', 'DescRzn51',
      'RznCausaS52', 'DescRzn52', 'RznCausaS53', 'DescRzn53', 'RznCausaS54',
      'DescRzn54', 'DXEnfHuer', 'DXVIH', 'DXCaPal', 'DXEnfRCEV', 'DXDesPro',
      'TippProNut', 'DescProdNutr', 'CodForma', 'CodViaAdmon', 'JustNoPBS',
      'Dosis', 'DosisUM', 'NoFAdmon', 'CodFreAdmon', 'IndEsp', 'CanTrat',
      'DurTrat', 'CantTotalF', 'UFCantTotal', 'IndRec', 'NoPrescAso',
      'EstJM','NoPrescripcion']]

    toint_cols_nutri = ['Dosis']
    df_principios_nutri[toint_cols_nutri] = df_principios_nutri[toint_cols_nutri].apply(pd.to_numeric, errors='coerce', axis=1)
    df_principios_nutri.to_excel('/opt/airflow/dags/generated_files/output_df_principios_nutri.xlsx')

    print("Medicamentos ",df_meds)
    print(df_meds.columns)
    print("Procedimientos ",df_procedimientos)
    print(df_procedimientos.columns)
    print("Principios activos ",df_principios_act)
    print(df_principios_act.columns)
    print("Productos nutricionales ",df_principios_nutri)
    print(df_principios_nutri.columns)
    print("Servicios complementarios ",df_servicios_complementarios)
    print(df_servicios_complementarios.columns)


    # if ~df_prescripcion.empty and len(df_prescripcion.columns) >0:
    #     load_df_to_sql(df_prescripcion, db_tmp_table, sql_connid)
    
    if ~df_meds.empty and len(df_meds.columns) >0:
        load_df_to_sql(df_meds, db_tmp_tablemayo, sql_connid)
    
    # if ~df_procedimientos.empty and len(df_procedimientos.columns) >0:
    #     load_df_to_sql(df_procedimientos, db_tmp_table6, sql_connid)
        
    #if ~df_principios_act.empty and len(df_principios_act.columns) >0:
    #    load_df_to_sql(df_principios_act, db_tmp_table4, sql_connid)

    # if ~df_principios_nutri.empty and len(df_principios_nutri.columns) >0:
    #     load_df_to_sql(df_principios_nutri, db_tmp_table5, sql_connid)

    # if ~df_servicios_complementarios.empty and len(df_servicios_complementarios.columns) >0:
    #     load_df_to_sql(df_servicios_complementarios, db_tmp_table2, sql_connid)
        

# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1)
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

    func_get_BI_MIPRES_prescripcion_python_task = PythonOperator(task_id = "get_BI_MIPRES_prescripcion",
                                                python_callable = func_get_BI_MIPRES_prescripcion
                                                #email_on_failure=True, 
                                                #email='BI@clinicos.com.co',
                                                #dag=dag,
                                                )

    #Se declara la función encargada de ejecutar el "Stored Procedure"
    #Load_BI_MIPRES_prescripcion = MsSqlOperator(task_id='Load_BI_MIPRES_prescripcion',
    #                                  mssql_conn_id=sql_connid,
    #                                  autocommit=True,
    #                                  sql="EXECUTE sp_load_BI_MIPRES_prescripcion"
    #                                  #email_on_failure=True, 
    #                                  #email='BI@clinicos.com.co',
    #                                  #dag=dag,
    #                                  )

    #Se declara la función encargada de ejecutar el "Stored Procedure"
    Load_BI_MIPRES_medicamentos = MsSqlOperator(task_id='Load_BI_MIPRES_medicamentos',
                                      mssql_conn_id=sql_connid,
                                      autocommit=True,
                                      sql="EXECUTE sp_load_BI_MIPRES_medicamento_prueba_mayo",
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

start_task >> func_get_BI_MIPRES_prescripcion_python_task  >> Load_BI_MIPRES_medicamentos>> task_end