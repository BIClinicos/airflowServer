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
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

db_table = "TEC_PYR_GEFDispensacion"
db_tmp_table = "tmp_TEC_PYR_GEFDispensacion"
dag_name = 'dag_' + db_table

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_TEC_PYR_GEFDispensacion ():

    # En postdesarrollo se tienen las siguientes caracteristicas
    # Ejecucion diaria,
    # Trae datos de ultima semana
    now = datetime.now()
    last_week = now - timedelta(weeks=1)
    last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
    #last_week=datetime.strptime('2023-03-01 04:00:00', '%Y-%m-%d %H:%M:%S')
    print(now)
    print(last_week)

    now = datetime.now()
    last_month = now - timedelta(days=30)
    last_month = last_month.strftime('%Y-%m-%d %H:%M:%S')
    print(now)
    print(last_month)


    dispensing_query = f"""
        SELECT A.*, CONCAT(USR_B.firstGivenName, ' ', USR_B.secondGiveName, ' ', USR_B.firstFamilyName, ' ', USR_B.secondFamilyName) AS namePractitioner, USR_B.documentNumber AS idPractitioner
        FROM (
        SELECT
            USR.idUser, USR.firstGivenName, USR.secondGiveName, USR.firstFamilyName, USR.secondFamilyName
            , DCT.code  AS 'idType', USR.documentNumber 'identificationNumber'
            , GNR.code AS 'gender', PEO.homeAddress, PLD.name city, 'CUNDINAMARCA' AS departament,	PEO.Telecom as telecom
            , ENC.identifier formulation
            , ENR.idActualPractitioner
            , DGN.code as diagnostic
            , EVE.idPatient, EVE.idEHREvent, EVE.actionRecordedDate
            , PRQ.dateRecord, 
                /*solicitada (RE), cancelada (CA), anulada (AN), atendida total (DT) o parcialmente (DP)*/
                CASE 
                    WHEN PRQ.state = 'RE' THEN 'solicitud'
                    WHEN PRQ.state = 'DT' THEN 'despacho'
                    WHEN PRQ.state = 'CA' THEN 'cancelada'
                    WHEN PRQ.state = 'AN' THEN 'anulada'
                    WHEN PRQ.state = 'DP' THEN 'parcialmente'
                ELSE '-1'
                END AS pharmacy
            , PRD.requestedQuantity, PRD.authorizedQuantity, PRD.dispensedQuantity 
            , PGD.drugConcentration, PGD.genericProductChemical, PGD.idGenericProduct
            , ENC.idEncounter
            , PPF.name AS 'pharmaceuticalForm'
        FROM 
            dbo.encounters ENC
            , dbo.encounterRecords ENR
            , dbo.diagnostics DGN
            , dbo.EHREvents EVE
            , dbo.users USR
            , dbo.pharmacyRequests PRQ
            , dbo.pharmacyRequestDetails PRD
            , dbo.productGenericDrugs PGD
            , dbo.productConfPharmaceuticalForm PPF
            , dbo.userConfTypeDocuments DCT
            , dbo.userConfAdministrativeSex GNR
            , dbo.userPeople PEO
            , dbo.generalPoliticalDivisions PLD
            
        WHERE 
            PRQ.state <> 'RE' AND
            ENR.idPrincipalContract IN (44, 45, 46, 47)
            AND ENC.dateStart > '{last_week}'
            AND ENR.idEncounter = ENC.idEncounter
            AND ENC.idEncounter = EVE.idEncounter
            --AND EVE.idAction = 108
            AND PRQ.EHREvent = EVE.idEHREvent
            AND DGN.idDiagnostic = ENR.idFirstDiagnosis
            AND USR.idUser = ENC.idUserPatient
            AND PRQ.idPharmacyRequest = PRD.idPharmacyRequest
            AND PGD.idGenericProduct = PRD.idGenericProduct
            AND PGD.idPharmaceuticalForm = PPF.idPharmaceuticalForm
            AND USR.idDocumentType = DCT.idTypeDocument
            AND USR.idUser = PEO.idUser
            AND PLD.idPoliticalDivision = PEO.idHomePlacePoliticalDivision
            AND PEO.idAdministrativeSex = GNR.idAdministrativeSex
            AND PRD.idGenericProduct IN ('41109','2468','40140','38420','41153','40921','1785','1627','41107','39295','1393','41140','39215'
            ,'1950','41154','2614','38483','41054','1628','41155','2167','41156','2561','39835','41152','41157','1953','39163','2017','41158'
            ,'1504','41133','40925','2316','41119','2474','41116','38157','41159','1901','41160','36597','2248','36606','41268','41161','36598'
            ,'41269','2472','41162','38284','41052','40987','41046','2048','36622','2314','41134','41150','38182','2602','41110','39513','2604'
            ,'41163','39365','1296','36732','41164','2019','41165','2392','41166','2578','41113','2020','41112','36765','36757','41126','36758'
            ,'2478','1503','41143','36786','36792','36787','36841','41132','2482','2483','41167','41115','39154','1754','2626','41114','36897'
            ,'41136','38328','41168','1324','41146','36916','40910','41169','36997','2490','36998','41053','2400','41082','40622','41141','39157'
            ,'1516','37047','37045','41170','1649','40933','41129','2321','40927','41123','39101','1908','2012','41120','1648','41171','37188'
            ,'1957','41078','41117','37206','1909','2257','41125','39056','41139','2595','2324','41172','37248','2258','41077','37059','2584'
            ,'41076','37060','40926','2028','41173','41174','1513','39489','40929','2285','41175','37312','2078','2079','38362','41128','2385'
            ,'41080','41124','1581','38851','38842','40627','40628','1521','41111','40932','2423','41176','37438','1622','41177','37437','1896'
            ,'41138','2671','41108','37493','41081','37516','1522','41151','37522','41178','37520','37533','41130','1859','38772','41179','1525'
            ,'40934','37581','41135','37572','2069','41148','37595','2146','37599','37596','41144','2147','1615','41145','37625','37649','1893'
            ,'41122','37642','1384','41137','37752','2338','37766','41147','2723','37767','41180','2422','41131','2251','41079','40160','40586'
            ,'40159','1318','38207','38210','38211','2434','41118','39096','1373','41181','41142','39231','2171','41182','41183','2183','41184'
            ,'39353','41185','1676','41121','2442','1796','41186','38791','41325','41127','38012','2655','39104','41187','1715','39103','2340'
            ,'41149')
            ) A
            , dbo.users USR_B
        WHERE 
            A.idActualPractitioner = USR_B.idUser

        ORDER BY A.idUser, A.idEHREvent

    """
    df = sql_2_df(dispensing_query, sql_conn_id=sql_connid_gomedisys)
    df['unitPrice'] = ""
    print(df.columns)
    print(df.dtypes)
    print(df)

    cols_dates = ['actionRecordedDate','dateRecord']
    for col in cols_dates:
        df[col] = df[col].astype(str)

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
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '0 5 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_TEC_PYR_GEFDispensacion_python_task = PythonOperator(
                                                            task_id = "get_TEC_PYR_GEFDispensacion",
                                                            python_callable = func_get_TEC_PYR_GEFDispensacion,
                                                            email_on_failure=True, 
                                                            email='BI@clinicos.com.co',
                                                            dag=dag
                                                            )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_TEC_PYR_GEFDispensacion = MsSqlOperator(task_id='Load_TEC_PYR_GEFDispensacion',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE sp_load_TEC_PYR_GEFDispensacion",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_TEC_PYR_GEFDispensacion_python_task >> load_TEC_PYR_GEFDispensacion >> task_end