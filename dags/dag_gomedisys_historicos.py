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
from utils import sql_2_df

#  Se nombran las variables a utilizar en el dag

dag_name = 'dag_gomedisys_gestion_farmaceutica'

def func_get_table ():

    now = date.today().strftime("%Y-%m-%d")

    formulation_query = "select 	USR.idUser, USR.firstGivenName, USR.secondGiveName, USR.firstFamilyName, USR.secondFamilyName 	, DCT.code  AS 'tipoIdentificacion' 	, USR.documentNumber 'numeroIdentificacion' 	, GNR.code AS 'genero', PEO.homeAddress 'direccion', PLD.name ciudad, 'CUNDINAMARCA' AS departamento,	PEO.Telecom AS 'telefonoCelular' 	, 'Clinicos Programas de Atenci�n Integral S.A.S IPS' AS 'nombreInstitucion', 110012347106 AS 'codigo' 	, PGD.genericProductChemical, PGD.drugConcentration 	, PPF.name AS 'formaFarmaceutica' 	, PAR.name AS 'viaAdministracion'  	, CONCAT(CAST(round(EVF.doseValue, 0, 0)AS INT), ' ',  EFP.name) frecuencia  	, EVF.valueAdministrationTime, EVF.formulatedAmount, EVF.dateRecorded 'fechaPrescripcion', EVF.dateRecorded 'fechaAutorizacion', EVF.idProductGeneric, PGD.genericProductChemical, EVF.idEHREvent  	, ENC.idEncounter 	, EVE.idEHREvent, EVE.idAction 	, DGN.code as diagnostic from 	dbo.encounters ENC 	, dbo.users USR 	, dbo.userPeople PEO 	, dbo.generalPoliticalDivisions PLD 	, dbo.encounterRecords ENR 	, dbo.EHREvents EVE 	, dbo.EHREventFormulation EVF LEFT JOIN dbo.productConfAdministrationRoute PAR ON PAR.idAdministrationRoute = EVF.idAdministrationRoute 	, dbo.productGenericDrugs PGD 	, dbo.productConfPharmaceuticalForm PPF 	, dbo.EHRConfFormulationPeriodicity EFP 	, dbo.diagnostics DGN 	, dbo.userConfTypeDocuments DCT 	, dbo.userConfAdministrativeSex GNR where 	ENC.idUserPatient = USR.idUser  	AND EVF.isSuspended <> 1 	and EVF.idProductType = 2  	and EVE.idAction in ('99','100','101') 	AND USR.idUser = PEO.idUser 	AND PLD.idPoliticalDivision = PEO.idHomePlacePoliticalDivision 	AND ENR.idPrincipalContract in (44, 45, 46, 47) 	AND ENR.idEncounter = ENC.idEncounter 	AND ENC.idEncounter = EVE.idEncounter 	AND EVE.idEHREvent = EVF.idEHREvent 	AND EVF.idProductGeneric = PGD.idGenericProduct 	AND PGD.idPharmaceuticalForm = PPF.idPharmaceuticalForm 	AND EFP.idPeriodicity = EVF.idPeriodicity  	AND DGN.idDiagnostic = ENR.idFirstDiagnosis 	AND USR.idDocumentType = DCT.idTypeDocument 	AND USR.idUser = PEO.idUser     AND PEO.idAdministrativeSex = GNR.idAdministrativeSex 	AND EVF.idProductGeneric IN ('41109','2468','40140','38420','41153','40921','1785','1627','41107','39295','1393','41140','39215' 	,'1950','41154','2614','38483','41054','1628','41155','2167','41156','2561','39835','41152','41157','1953','39163','2017','41158' 	,'1504','41133','40925','2316','41119','2474','41116','38157','41159','1901','41160','36597','2248','36606','41268','41161','36598' 	,'41269','2472','41162','38284','41052','40987','41046','2048','36622','2314','41134','41150','38182','2602','41110','39513','2604' 	,'41163','39365','1296','36732','41164','2019','41165','2392','41166','2578','41113','2020','41112','36765','36757','41126','36758' 	,'2478','1503','41143','36786','36792','36787','36841','41132','2482','2483','41167','41115','39154','1754','2626','41114','36897' 	,'41136','38328','41168','1324','41146','36916','40910','41169','36997','2490','36998','41053','2400','41082','40622','41141','39157' 	,'1516','37047','37045','41170','1649','40933','41129','2321','40927','41123','39101','1908','2012','41120','1648','41171','37188' 	,'1957','41078','41117','37206','1909','2257','41125','39056','41139','2595','2324','41172','37248','2258','41077','37059','2584' 	,'41076','37060','40926','2028','41173','41174','1513','39489','40929','2285','41175','37312','2078','2079','38362','41128','2385' 	,'41080','41124','1581','38851','38842','40627','40628','1521','41111','40932','2423','41176','37438','1622','41177','37437','1896' 	,'41138','2671','41108','37493','41081','37516','1522','41151','37522','41178','37520','37533','41130','1859','38772','41179','1525' 	,'40934','37581','41135','37572','2069','41148','37595','2146','37599','37596','41144','2147','1615','41145','37625','37649','1893' 	,'41122','37642','1384','41137','37752','2338','37766','41147','2723','37767','41180','2422','41131','2251','41079','40160','40586' 	,'40159','1318','38207','38210','38211','2434','41118','39096','1373','41181','41142','39231','2171','41182','41183','2183','41184' 	,'39353','41185','1676','41121','2442','1796','41186','38791','41325','41127','38012','2655','39104','41187','1715','39103','2340' 	,'41149')"
    dispensing_query = "select 	USR.firstGivenName, USR.secondGiveName, USR.firstFamilyName, USR.secondFamilyName, USR.idUser 	, DCT.code  AS 'tipoIdentificacion', USR.documentNumber 'numeroIdentificacion' 	, GNR.code AS 'genero', PEO.homeAddress 'direccion', PLD.name ciudad, 'CUNDINAMARCA' AS departamento,	PEO.Telecom AS 'telefonoCelular' 	, ENC.idEncounter 	, ENR.idActualPractitioner 	, EVE.idEHREvent, EVE.idEncounter 	, DGN.code as diagnostic  	, EVE.idPatient, EVE.idEHREvent, EVE.actionRecordedDate 	, CONCAT(USR.firstGivenName, ' ', USR.secondGiveName, ' ', USR.firstFamilyName, ' ', USR.secondFamilyName) AS namePracticioner, USR.documentNumber AS idPracticioner 	, PRQ.dateRecord 'fechaDispensacion', 		/*solicitada (RE), cancelada (CA), anulada (AN), atendida total (DT) o parcialmente (DP)*/ 		CASE  			WHEN PRQ.state = 'RE' THEN 'solicitud' 			WHEN PRQ.state = 'DT' THEN 'despacho' 			WHEN PRQ.state = 'CA' THEN 'cancelada' 			WHEN PRQ.state = 'AN' THEN 'anulada' 			WHEN PRQ.state = 'DP' THEN 'parcialmente' 		ELSE '-1' 		END AS pharmacy 	, PRD.requestedQuantity, PRD.authorizedQuantity, PRD.dispensedQuantity 	, PGD.drugConcentration, PGD.genericProductChemical, PGD.idGenericProduct 	, PPF.name AS 'formaFarmaceutica' 	, PPL.value unitPrice from 	dbo.encounters ENC 	, dbo.encounterRecords ENR 	, dbo.diagnostics DGN 	, dbo.EHREvents EVE 	, dbo.users USR 	, dbo.pharmacyRequests PRQ 	, dbo.pharmacyRequestDetails PRD 	, dbo.productGenericDrugs PGD 	, dbo.productConfPharmaceuticalForm PPF 	, dbo.userConfTypeDocuments DCT 	, dbo.userConfAdministrativeSex GNR 	, dbo.userPeople PEO 	, dbo.generalPoliticalDivisions PLD 	, [dbo].[pharmacyPurchasePriceLists] PPL LEFT JOIN dbo.products PDS ON PPL.idProduct = PDS.idProduct where 	PDS.idGeneric = PGD.idGenericProduct 	AND PRQ.state <> 'RE'  	AND ENR.idPrincipalContract IN (44, 45, 46, 47) 	AND ENR.idEncounter = ENC.idEncounter 	AND ENC.idEncounter = EVE.idEncounter 	AND EVE.idAction = 108 	AND PRQ.EHREvent = EVE.idEHREvent 	AND DGN.idDiagnostic = ENR.idFirstDiagnosis 	AND USR.idUser = ENC.idUserPatient 	AND PRQ.idPharmacyRequest = PRD.idPharmacyRequest 	AND PGD.idGenericProduct = PRD.idGenericProduct 	AND PGD.idPharmaceuticalForm = PPF.idPharmaceuticalForm 	AND USR.idDocumentType = DCT.idTypeDocument 	AND USR.idUser = PEO.idUser 	AND PLD.idPoliticalDivision = PEO.idHomePlacePoliticalDivision 	AND PEO.idAdministrativeSex = GNR.idAdministrativeSex 	AND PRD.idGenericProduct IN ('41109','2468','40140','38420','41153','40921','1785','1627','41107','39295','1393','41140','39215' 	,'1950','41154','2614','38483','41054','1628','41155','2167','41156','2561','39835','41152','41157','1953','39163','2017','41158' 	,'1504','41133','40925','2316','41119','2474','41116','38157','41159','1901','41160','36597','2248','36606','41268','41161','36598' 	,'41269','2472','41162','38284','41052','40987','41046','2048','36622','2314','41134','41150','38182','2602','41110','39513','2604' 	,'41163','39365','1296','36732','41164','2019','41165','2392','41166','2578','41113','2020','41112','36765','36757','41126','36758' 	,'2478','1503','41143','36786','36792','36787','36841','41132','2482','2483','41167','41115','39154','1754','2626','41114','36897' 	,'41136','38328','41168','1324','41146','36916','40910','41169','36997','2490','36998','41053','2400','41082','40622','41141','39157' 	,'1516','37047','37045','41170','1649','40933','41129','2321','40927','41123','39101','1908','2012','41120','1648','41171','37188' 	,'1957','41078','41117','37206','1909','2257','41125','39056','41139','2595','2324','41172','37248','2258','41077','37059','2584' 	,'41076','37060','40926','2028','41173','41174','1513','39489','40929','2285','41175','37312','2078','2079','38362','41128','2385' 	,'41080','41124','1581','38851','38842','40627','40628','1521','41111','40932','2423','41176','37438','1622','41177','37437','1896' 	,'41138','2671','41108','37493','41081','37516','1522','41151','37522','41178','37520','37533','41130','1859','38772','41179','1525' 	,'40934','37581','41135','37572','2069','41148','37595','2146','37599','37596','41144','2147','1615','41145','37625','37649','1893' 	,'41122','37642','1384','41137','37752','2338','37766','41147','2723','37767','41180','2422','41131','2251','41079','40160','40586' 	,'40159','1318','38207','38210','38211','2434','41118','39096','1373','41181','41142','39231','2171','41182','41183','2183','41184' 	,'39353','41185','1676','41121','2442','1796','41186','38791','41325','41127','38012','2655','39104','41187','1715','39103','2340' 	,'41149')"
    join_drugs_query = "SELECT * FROM [dbo].[OP_GEF_prodGenericDrugs]"
    df_formulation = sql_2_df(formulation_query, sql_conn_id=sql_connid_gomedisys)
    df_dispensing = sql_2_df(dispensing_query, sql_conn_id=sql_connid_gomedisys)
    df_join_drugs = sql_2_df(join_drugs_query , sql_conn_id=sql_connid)
    print(df_formulation.columns)
    print(df_formulation.dtypes)
    print(df_dispensing.columns)
    print(df_dispensing.dtypes)
    print(df_join_drugs.columns)
    print(df_join_drugs.dtypes)

    if ~df_formulation.empty and len(df_formulation.columns) >0:
        load_df_to_sql(df_formulation, tmp_OP_GEF_Formulacion, sql_connid)

    if ~df_dispensing.empty and len(df_dispensing.columns) >0:
        load_df_to_sql(df_dispensing, tmp_OP_GEF_Dispensacion, sql_connid)

    # df_merge_formulation = df_join_drugs.merge(df_formulation, left_on='idGenericProduct', right_on='idProductGeneric')
    # print(df_merge_formulation.columns)
    # print(df_merge_formulation)

    # df_merge_dispensing = df_join_drugs.merge(df_dispensing, left_on='idGenericProduct', right_on='idGenericProduct')
    # print(df_merge_dispensing.columns)
    # print(df_merge_dispensing)

    # df_merge_formulation.to_excel('/opt/airflow/dags/generated_files/merge_formulation.xlsx', index = False)
    # df_merge_dispensing.to_excel('/opt/airflow/dags/generated_files/merge_dispensing.xlsx', index = False)

    

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
    schedule_interval= '0 12 * * 5',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    get_table = PythonOperator(task_id = "get_table",
        python_callable = func_get_table)

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    # load_table = MsSqlOperator(task_id='Load_table',
    #                                    mssql_conn_id=sql_connid,
    #                                    autocommit=True,
    #                                    sql="EXECUTE sp_load_gomedisys",
    #                                    )
        
    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_table >> task_end