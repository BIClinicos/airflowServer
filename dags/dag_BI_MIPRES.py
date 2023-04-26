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
from utils import sql_2_df,add_days_to_date,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get,normalize_str_categorical
# from bs4 import BeautifulSoup


wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
#  Se nombran las variables a utilizar en el dag
db_table = "BI_MIPRES_prescripcion"
db_tmp_table = 'TMP_BI_MIPRES_prescripcion'
db_tmp_table2 = 'TMP_BI_MIPRES_serviciosComplementarios'
db_tmp_table3 = 'TMP_BI_MIPRES_medicamentos'
db_tmp_table4 = 'TMP_BI_MIPRES_PrincipiosActivos'
db_tmp_table5 = 'TMP_BI_MIPRES_productosnutricionales'
db_tmp_table6 = 'TMP_BI_MIPRES_procedimientos'
dag_name = 'dag_' + 'BI_MIPRES'


# Función encargada de extraer los datos desde la API de MIPRES "Leer Json y crear los dataframes"

def charge_tables(dates2):

    
    """Función encargada de extraer los datos desde la API de MIPRES "Leer Json y crear los dataframes"
    
    Args: 
        dates2 (List): Lista de de fechas en formato string "YYYY-MM-DD". 
    
    Returns: 
        "Extracción de dataframes a partir de las consultas a la API de MIPRES":
        df_prescripcion, df_servicios_complementarios, df_meds, df_principios_act, df_principios_nutri, df_procedimientos
    """

    nit='900496641'
    token='148CA3F2-9233-411E-A728-76CE02ABFED5'
    df_prescripcion = pd.DataFrame(['NoPrescripcion','FPrescripcion','HPrescripcion','CodHabIPS','TipoIDIPS','NroIDIPS','CodDANEMunIPS','DirSedeIPS','TelSedeIPS','TipoIDProf','NumIDProf','PNProfS','SNProfS','PAProfS','SAProfS','RegProfS','TipoIDPaciente','NroIDPaciente','PNPaciente','SNPaciente','PAPaciente','SAPaciente','CodAmbAte','RefAmbAte','PacCovid19','EnfHuerfana','CodEnfHuerfana','EnfHuerfanaDX','CodDxPpal','CodDxRel1','CodDxRel2','SopNutricional','CodEPS','TipoIDMadrePaciente','NroIDMadrePaciente','TipoTransc','TipoIDDonanteVivo','NroIDDonanteVivo','EstPres','FechaProceso'])
    df_servicios_complementarios = pd.DataFrame()
    df_meds = pd.DataFrame()
    df_principios_act = pd.DataFrame()
    df_principios_nutri = pd.DataFrame()
    df_procedimientos = pd.DataFrame(columns=['ConOrden', 'TipoPrest', 'CausaS11', 'CausaS12', 'CausaS2', 'CausaS3', 'CausaS4', 'ProPBSUtilizado', 'CausaS5', 'ProPBSDescartado', 'RznCausaS51', 'DescRzn51', 'RznCausaS52', 'DescRzn52', 'CausaS6', 'CausaS7', 'CodCUPS', 'CanForm', 'CadaFreUso', 'CodFreUso', 'Cant', 'CantTotal', 'CodPerDurTrat', 'JustNoPBS', 'IndRec', 'EstJM', 'NoPrescripcion'])
    try:
        for date in dates2:
            url=f'https://wsmipres.sispro.gov.co/WSMIPRESNOPBS/api/Prescripcion/{nit}/{date}/{token}'
            req = requests.get(url, verify=False)
            #if req.status_code == 200:
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
            #else: 
            #    print("Request to {} failed".format(date)
    except Exception as e: 
        print('Error in MIPRES request:', e)


    return df_prescripcion, df_servicios_complementarios, df_meds, df_principios_act, df_principios_nutri, df_procedimientos

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_BI_MIPRES_prescripcion():

    # Get date list from the last 5 days
    now = pd.datetime.today()
    days_ago = add_days_to_date(now,-5)
    now = datetime(2022,12,31)
    days_ago = datetime(2022,12,1)

    dates = pd.date_range(start=days_ago, end = now,freq='D')
    print(dates)

    dates = dates.tolist()

    dates2 = []
    for i in dates:
      dates2.append(i.strftime("%Y-%m-%d"))
      
    # Get dataframes from API response
    [df_prescripcion, df_servicios_complementarios, df_meds, df_principios_act, df_principios_nutri, df_procedimientos] = charge_tables(dates2)

    print(df_prescripcion.info())

    # Processing dataframes: prescripcion
    date_columns = ['FPrescripcion']
    for i in date_columns:
        df_prescripcion[i] = df_prescripcion[i].astype(str)
        df_prescripcion[i] = df_prescripcion[i].str.strip()
        df_prescripcion[i] = pd.to_datetime(df_prescripcion[i], format="%Y-%m-%d", errors = 'coerce')

    df_prescripcion['HPrescripcion'] = pd.to_datetime(df_prescripcion['HPrescripcion'], format='%H:%M:%S')

    df_prescripcion['FechaProceso'] = datetime.now().strftime("%Y-%m-%d")
    df_prescripcion['FechaProceso'] = pd.to_datetime(df_prescripcion['FechaProceso'], format="%Y-%m-%d", errors = 'coerce')
    df_prescripcion['FechaProceso'] = df_prescripcion['FechaProceso'].astype(str)

    df_prescripcion = df_prescripcion.drop_duplicates(subset=['NoPrescripcion'])
    df_prescripcion = df_prescripcion.dropna(subset=['NoPrescripcion'])

    int_col_pres = ['CodAmbAte', 'PacCovid19', 'EnfHuerfana', 'EstPres']

    for i in int_col_pres:
        df_prescripcion[i] = df_prescripcion[i].fillna(0)
        df_prescripcion[i] = df_prescripcion[i].astype(int)
        df_prescripcion[i] = df_prescripcion[i].astype(str)
        df_prescripcion[i] = df_prescripcion[i].str.replace('.0','', regex=False)
    
    df_prescripcion = df_prescripcion[['NoPrescripcion', 'FPrescripcion', 'HPrescripcion', 'CodHabIPS',
       'TipoIDIPS', 'NroIDIPS', 'CodDANEMunIPS', 'DirSedeIPS', 'TelSedeIPS',
       'TipoIDProf', 'NumIDProf', 'PNProfS', 'SNProfS', 'PAProfS', 'SAProfS',
       'RegProfS', 'TipoIDPaciente', 'NroIDPaciente', 'PNPaciente',
       'SNPaciente', 'PAPaciente', 'SAPaciente', 'CodAmbAte', 'RefAmbAte',
       'PacCovid19', 'EnfHuerfana', 'CodEnfHuerfana', 'EnfHuerfanaDX',
       'CodDxPpal', 'CodDxRel1', 'CodDxRel2', 'SopNutricional', 'CodEPS',
       'TipoIDMadrePaciente', 'NroIDMadrePaciente', 'TipoTransc',
       'TipoIDDonanteVivo', 'NroIDDonanteVivo', 'EstPres','FechaProceso']]

    
    # Processing dataframes: medicamentos

    int_col_med = ['ConOrden', 'DosisUM', 'CanTrat', 'DurTrat', 'CantTotalF', 'UFCantTotal', 'EstJM']

    for i in int_col_med:
        df_meds[i] = df_meds[i].fillna(0)
        df_meds[i] = df_meds[i].astype(int)
        df_meds[i] = df_meds[i].astype(str)
        df_meds[i] = df_meds[i].str.replace('.0','', regex=False)
    
    float_col_meds = ['TipoMed', 'Dosis']

    for i in float_col_meds:
        df_meds[i] = df_meds[i].replace(',', '.')
        df_meds[i] = pd.to_numeric(df_meds[i], errors='coerce')


    df_meds.drop(['PrincipiosActivos','IndicacionesUNIRS'], axis=1, inplace=True)   
    
    df_meds = df_meds[['ConOrden', 'TipoMed', 'TipoPrest', 'CausaS1', 'CausaS2', 'CausaS3',
       'MedPBSUtilizado', 'RznCausaS31', 'DescRzn31', 'RznCausaS32',
       'DescRzn32', 'CausaS4', 'MedPBSDescartado', 'RznCausaS41', 'DescRzn41',
       'RznCausaS42', 'DescRzn42', 'RznCausaS43', 'DescRzn43', 'RznCausaS44',
       'DescRzn44', 'CausaS5', 'RznCausaS5', 'CausaS6', 'DescMedPrinAct',
       'CodFF', 'CodVA', 'JustNoPBS', 'Dosis', 'DosisUM', 'NoFAdmon',
       'CodFreAdmon', 'IndEsp', 'CanTrat', 'DurTrat', 'CantTotalF',
       'UFCantTotal', 'IndRec', 'EstJM','NoPrescripcion']]
    df_meds.to_excel('/opt/airflow/dags/generated_files/output_df_meds.xlsx')

    df_meds = df_meds.drop_duplicates(subset=['ConOrden','DescMedPrinAct','NoPrescripcion'])
    
    print(df_meds.dtypes)

    # Processing dataframes: servicios complementarios
    int_col_servcom = ['ConOrden', 'CanForm', 'CadaFreUso', 'CodFreUso', 'Cant', 'CantTotal', 'CodPerDurTrat', 'EstJM']

    for i in int_col_servcom:
        df_servicios_complementarios[i] = df_servicios_complementarios[i].fillna(0)
        df_servicios_complementarios[i] = df_servicios_complementarios[i].astype(int)
        df_servicios_complementarios[i] = df_servicios_complementarios[i].astype(str)
        df_servicios_complementarios[i] = df_servicios_complementarios[i].str.replace('.0','', regex=False)

    df_servicios_complementarios = df_servicios_complementarios[['ConOrden', 'TipoPrest', 'CausaS1', 'CausaS2', 'CausaS3', 'CausaS4',
      'DescCausaS4', 'CausaS5', 'CodSerComp', 'DescSerComp', 'CanForm',
      'CadaFreUso', 'CodFreUso', 'Cant', 'CantTotal', 'CodPerDurTrat',
      'TipoTrans', 'ReqAcom', 'TipoIDAcomAlb', 'NroIDAcomAlb',
      'ParentAcomAlb', 'NombAlb', 'CodMunOriAlb', 'CodMunDesAlb', 'JustNoPBS',
      'IndRec', 'EstJM','NoPrescripcion']]
    
    toint_cols = ['CanForm','CadaFreUso','Cant','CantTotal']
    for i in toint_cols:
        df_servicios_complementarios[i] = df_servicios_complementarios[i].str.replace(",",'.')
        df_servicios_complementarios[i] = pd.to_numeric(df_servicios_complementarios[i], errors='coerce')

    df_servicios_complementarios = df_servicios_complementarios.drop_duplicates(subset=['ConOrden','CodSerComp','NoPrescripcion'])

    # Processing dataframes: principios activos
    int_col_seract = ['ConOrden', 'UMedCantCont']

    for i in int_col_seract:
        df_principios_act[i] = df_principios_act[i].fillna(0)
        df_principios_act[i] = df_principios_act[i].astype(int)
        df_principios_act[i] = df_principios_act[i].astype(str)
        df_principios_act[i] = df_principios_act[i].str.replace('.0','', regex=False)
    
    df_principios_act = df_principios_act[['ConOrden', 'CodPriAct', 'ConcCant', 'UMedConc', 'CantCont',
      'UMedCantCont','NoPrescripcion']]
    
    float_col_pa = ['ConcCant', 'CantCont']

    for i in float_col_pa:
        df_principios_act[i] = df_principios_act[i].str.replace(',','.')
        df_principios_act[i] = pd.to_numeric(df_principios_act[i], errors='coerce')

    df_principios_act = df_principios_act.drop_duplicates(subset=['ConOrden', 'CodPriAct', 'NoPrescripcion'])

    # Processing dataframes: productos nutricionales
    int_col_prodnut = ['ConOrden', 'CanTrat','DurTrat', 'CantTotalF', 'UFCantTotal', 'EstJM']

    for i in int_col_prodnut:
        df_principios_nutri[i] = df_principios_nutri[i].fillna(0)
        df_principios_nutri[i] = df_principios_nutri[i].astype(int)
        df_principios_nutri[i] = df_principios_nutri[i].astype(str)
        df_principios_nutri[i] = df_principios_nutri[i].str.replace('.0','', regex=False)

    df_principios_nutri = df_principios_nutri[['ConOrden', 'TipoPrest', 'CausaS1', 'CausaS2', 'CausaS3', 'CausaS4',
      'ProNutUtilizado', 'RznCausaS41', 'DescRzn41', 'RznCausaS42',
      'DescRzn42', 'CausaS5', 'ProNutDescartado', 'RznCausaS51', 'DescRzn51',
      'RznCausaS52', 'DescRzn52', 'RznCausaS53', 'DescRzn53', 'RznCausaS54',
      'DescRzn54', 'DXEnfHuer', 'DXVIH', 'DXCaPal', 'DXEnfRCEV', 'DXDesPro',
      'TippProNut', 'DescProdNutr', 'CodForma', 'CodViaAdmon', 'JustNoPBS',
      'Dosis', 'DosisUM', 'NoFAdmon', 'CodFreAdmon', 'IndEsp', 'CanTrat',
      'DurTrat', 'CantTotalF', 'UFCantTotal', 'IndRec', 'NoPrescAso',
      'EstJM','NoPrescripcion']]
    
    df_principios_nutri['Dosis'] = df_principios_nutri['Dosis'].str.replace(",",'.')
    df_principios_nutri['Dosis'] = pd.to_numeric(df_principios_nutri['Dosis'], errors='coerce')

    df_principios_nutri = df_principios_nutri.drop_duplicates(subset=['ConOrden', 'CodForma', 'CodFreAdmon', 'NoPrescripcion'])
    
    # Processing dataframes: procedimientos
    int_col_proc = ['ConOrden', 'CanForm', 'CadaFreUso', 'CodFreUso', 'Cant', 'CantTotal', 'EstJM']

    for i in int_col_proc:
        df_procedimientos[i] = df_procedimientos[i].fillna(0)
        df_procedimientos[i] = df_procedimientos[i].astype(int)
        df_procedimientos[i] = df_procedimientos[i].astype(str)
        df_procedimientos[i] = df_procedimientos[i].str.replace('.0','', regex=False)


    df_procedimientos = df_procedimientos.dropna(subset=['ConOrden', 'TipoPrest', 'CausaS11', 'CausaS12', 'CausaS2', 'CausaS3', 'CausaS4', 'ProPBSUtilizado', 'CausaS5', 'ProPBSDescartado', 'RznCausaS51', 'DescRzn51', 'RznCausaS52', 'DescRzn52', 'CausaS6', 'CausaS7', 'CodCUPS', 'CanForm', 'CadaFreUso', 'CodFreUso', 'Cant', 'CantTotal', 'CodPerDurTrat', 'JustNoPBS', 'IndRec', 'EstJM'])
    df_procedimientos = df_procedimientos.drop_duplicates(subset=['ConOrden', 'CodCUPS', 'NoPrescripcion'])

    # Load data to tmp tables
    if ~df_prescripcion.empty and len(df_prescripcion.columns) >0:
        load_df_to_sql(df_prescripcion, db_tmp_table, sql_connid)
    
    if ~df_meds.empty and len(df_meds.columns) >0:
        load_df_to_sql(df_meds, db_tmp_table3, sql_connid)
    
    if ~df_procedimientos.empty and len(df_procedimientos.columns) >0:
        load_df_to_sql(df_procedimientos, db_tmp_table6, sql_connid)
        
    if ~df_principios_act.empty and len(df_principios_act.columns) >0:
        load_df_to_sql(df_principios_act, db_tmp_table4, sql_connid)

    if ~df_principios_nutri.empty and len(df_principios_nutri.columns) >0:
        load_df_to_sql(df_principios_nutri, db_tmp_table5, sql_connid)

    if ~df_servicios_complementarios.empty and len(df_servicios_complementarios.columns) >0:
        load_df_to_sql(df_servicios_complementarios, db_tmp_table2, sql_connid)
        

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
    schedule_interval= '20 10 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    func_get_BI_MIPRES_prescripcion_python_task = PythonOperator(task_id = "get_BI_MIPRES_prescripcion",
                                                python_callable = func_get_BI_MIPRES_prescripcion,
                                                email_on_failure=True, 
                                                email='BI@clinicos.com.co',
                                                dag=dag,
                                                )

    #Se declara la función encargada de ejecutar el "Stored Procedure"
    Load_BI_MIPRES_prescripcion = MsSqlOperator(task_id='Load_BI_MIPRES_prescripcion',
                                      mssql_conn_id=sql_connid,
                                      autocommit=True,
                                      sql="EXECUTE sp_load_BI_MIPRES_prescripcion",
                                      email_on_failure=True, 
                                      email='BI@clinicos.com.co',
                                      dag=dag,
                                     )

    #Se declara la función encargada de ejecutar el "Stored Procedure"
    Load_BI_MIPRES_medicamentos = MsSqlOperator(task_id='Load_BI_MIPRES_medicamentos',
                                     mssql_conn_id=sql_connid,
                                     autocommit=True,
                                     sql="EXECUTE sp_load_BI_MIPRES_medicamentos",
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag,
                                     )

    #Se declara la función encargada de ejecutar el "Stored Procedure"
    Load_BI_MIPRES_serviciosComplementarios = MsSqlOperator(task_id='Load_BI_MIPRES_serviciosComplementarios',
                                     mssql_conn_id=sql_connid,
                                     autocommit=True,
                                     sql="EXECUTE sp_load_BI_MIPRES_serviciosComplementarios",
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag,
                                     )
    
    #Se declara la función encargada de ejecutar el "Stored Procedure"
    Load_BI_MIPRES_PrincipiosActivos = MsSqlOperator(task_id='Load_BI_MIPRES_PrincipiosActivos',
                                     mssql_conn_id=sql_connid,
                                     autocommit=True,
                                     sql="EXECUTE sp_load_BI_MIPRES_PrincipiosActivos",
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag,
                                     )
    
    #Se declara la función encargada de ejecutar el "Stored Procedure"
    Load_BI_MIPRES_productosnutricionales = MsSqlOperator(task_id='Load_BI_MIPRES_productosnutricionales',
                                     mssql_conn_id=sql_connid,
                                     autocommit=True,
                                     sql="EXECUTE sp_load_BI_MIPRES_productosnutricionales",
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag,
                                     )
    
    #Se declara la función encargada de ejecutar el "Stored Procedure"
    Load_BI_MIPRES_procedimientos = MsSqlOperator(task_id='Load_BI_MIPRES_procedimientos',
                                     mssql_conn_id=sql_connid,
                                     autocommit=True,
                                     sql="EXECUTE sp_load_BI_MIPRES_procedimientos",
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag,
                                     )

    
    
    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> func_get_BI_MIPRES_prescripcion_python_task  >> Load_BI_MIPRES_prescripcion >> Load_BI_MIPRES_medicamentos >> Load_BI_MIPRES_serviciosComplementarios >> Load_BI_MIPRES_PrincipiosActivos >> Load_BI_MIPRES_productosnutricionales >> Load_BI_MIPRES_procedimientos >> task_end