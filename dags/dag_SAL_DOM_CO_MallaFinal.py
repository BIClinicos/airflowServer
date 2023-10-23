import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators import email_operator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
import numpy as np
from pandas import read_excel
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df, load_df_to_sql 

########
#### Ficha tecnica
#######
### Elabora: David Cardenas Pienda
### Fecha: 2023-01-19
### Documentacion: Si
### Proceso: Reporte de malla final domiciliaria. Compensar
#######

### Parametros de tablas (def y temp) y nombre del dag

db_table = "SAL_DOM_CO_MallaFinal"
db_tmp_table = "tmp_SAL_DOM_CO_MallaFinal"
dag_name = 'dag_' + db_table + '_old'

### Parametros de tiempo
# Obtencion dinamica
end_date = date.today() - timedelta(days=1)
start_date = end_date.replace(day=1)
# Carge historico
#start_date = date(2022, 11, 1) 
#end_date = date(2022, 11, 30)
# Formato
start_date = start_date.strftime('%Y-%m-%d')
end_date = end_date.strftime('%Y-%m-%d')

#######
#### Funciones del script
#######

### Metodo para lectura del script

def query_SAL_DOM_CO_MallaFinal (start_date, end_date):
    """Listado de internacion domiciliaria

    Parametros: 
    start (date): Fecha inicial del periodo de consulta
    end (date): Fecha final del periodo de consulta

    Retorna:
    data frame con la informacion del listado de internacion domiciliaria compensar
    """
    ## Verficacion de fechas
    print('Fecha inicio ', start_date)
    print('Fecha fin ', end_date)
    
    query = f"""
        DECLARE 
            @idUserCompany INT = 1,
            @startDate DATE = '{start_date}',
            @endDate DATE = '{end_date}',
            @hcPlanF VARCHAR(MAX) = 2;--(SELECT STRING_AGG(idHCActivity,',') FROM dbo.EHRConfHCActivity)

        WITH CTE AS(
            SELECT 
                        DATEFROMPARTS(YEAR(@startDate),MONTH(@startDate),1) AS [FECHA DEL REGISTRO],
                ROW_NUMBER() OVER (ORDER BY Enc.idEncounter) AS [REGISTRO],
                FORMAT(@startDate,'MMMMM','es') AS [MES DEL REGISTRO],
                Comp.businessName AS [IPS DOMICILIO],
                CONCAT(Pat.firstGivenName,' ' + Pat.secondGiveName,' ' + Pat.firstFamilyName,' ' + Pat.secondFamilyName) AS [NOMBRES COMPLETOS],
                DocT.code AS [TIPO DOCUMENTO],
                Pat.documentNumber AS [NÚMERO DOCUMENTO DE IDENTIFICACIÓN],
                DATEDIFF(HOUR,PatP.birthDate,Enc.dateStart)/8766 AS [EDAD],
                CONCAT((DATEDIFF(HOUR,PatP.birthDate,Enc.dateStart)/8766/5)*5,'-',((DATEDIFF(HOUR,PatP.birthDate,Enc.dateStart)/8766/5)*5)+4) AS [GRUPO DE EDAD],
                Gender.code AS [SEXO],
                ContP.name AS [PLAN DE BENEFICIOS],
                PatP.homeAddress AS [DIRECCIÓN],
                IIF(Neig.name LIKE '%-%',RTRIM(LEFT(Neig.name,LEN(Neig.name) - CHARINDEX('-',REVERSE(Neig.name)) -1)),Neig.name) AS [BARRIO],
                IIF(Neig.name LIKE '%-%',LTRIM(RIGHT(Neig.name,CHARINDEX('-',REVERSE(Neig.name)) - 1)),'') AS [LOCALIDAD],
                Mun.name AS [MUNICIPIO],
                EncRP.name AS [CUIDADOR RESPONSABLE],
                ISNULL(PatP.phoneHome,Loc.[TELF]) AS [TELÉFONO],
                ISNULL(PatP.telecom,Loc.[CEL]) AS [CELULAR],

                (SELECT TOP 1 EHRAct.valueText FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
                        WHERE EV.idPatient = Enc.idUserPatient
                            AND EHRAct.idConfigActivity = 33
                            AND EHRAct.idElement = 1
                        ORDER BY EV.idEHREvent DESC) AS [VÍA DE INGRESO],

                (SELECT TOP 1 EHRAct.valueText FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)			
                        WHERE EHRAct.idEvent = EVDom.idEHREvent
                            AND EHRAct.idConfigActivity = 36
                            AND EHRAct.idElement = 1) AS [PRONÓSTICO],

                (SELECT TOP 1 FORMAT(CONVERT(DATE,EHRAct.valueText),'dd/MM/yyyy') FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
                        WHERE EV.idPatient = Enc.idUserPatient
                            AND EHRAct.idConfigActivity = 33
                            AND EHRAct.idElement = 2
                        ORDER BY EV.idEHREvent DESC) AS [FECHA DE PRESENTACIÓN],

                (SELECT TOP 1 DATEPART(YEAR,CONVERT(DATE,EHRAct.valueText)) FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
                        WHERE EV.idPatient = Enc.idUserPatient
                            AND EHRAct.idConfigActivity = 25
                            AND EHRAct.idElement = 1
                        ORDER BY EV.idEHREvent DESC) AS [AÑO INGRESO],

                (SELECT TOP 1 DATEPART(MONTH,CONVERT(DATE,EHRAct.valueText)) FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
                        WHERE EV.idPatient = Enc.idUserPatient
                            AND EHRAct.idConfigActivity = 25
                            AND EHRAct.idElement = 1
                        ORDER BY EV.idEHREvent DESC) AS [MES INGRESO],

                (SELECT TOP 1 DATEPART(DAY,CONVERT(DATE,EHRAct.valueText)) FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
                        WHERE EV.idPatient = Enc.idUserPatient
                            AND EHRAct.idConfigActivity = 25
                            AND EHRAct.idElement = 1
                        ORDER BY EV.idEHREvent DESC) AS [DÍA INGRESO],

                (SELECT TOP 1 FORMAT(CONVERT(DATE,EHRAct.valueText),'dd/MM/yyyy') FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
                        WHERE EV.idPatient = Enc.idUserPatient
                            AND EHRAct.idConfigActivity = 25
                            AND EHRAct.idElement = 1
                        ORDER BY EV.idEHREvent DESC) AS [FECHA INGRESO],

                DATEPART(YEAR,ISNULL(Enc.dateStartDischarge,Enc.dateDischarge)) AS [AÑO EGRESO],
                DATEPART(MONTH,ISNULL(Enc.dateStartDischarge,Enc.dateDischarge)) AS [MES DEL EGRESO],
                DATEPART(DAY,ISNULL(Enc.dateStartDischarge,Enc.dateDischarge)) AS [DÍA EGRESO],
                FORMAT(ISNULL(Enc.dateStartDischarge,Enc.dateDischarge),'dd/MM/yyyy HH:mm') AS [FECHA EGRESO],

                (SELECT TOP 1 DATEDIFF(DAY,CONVERT(DATE,EHRAct.valueText),ISNULL(Enc.dateStartDischarge,Enc.dateDischarge)) FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
                        WHERE EV.idPatient = Enc.idUserPatient
                            AND EHRAct.idConfigActivity = 25
                            AND EHRAct.idElement = 1
                        ORDER BY EV.idEHREvent DESC) AS [DIAS ESTANCIA],

                (SELECT MAX(Dx.code) FROM dbo.EHREventMedicalDiagnostics AS EVDx WITH(NOLOCK)
                    INNER JOIN dbo.diagnostics AS Dx WITH(NOLOCK) ON EVDx.idDiagnostic = Dx.idDiagnostic
                    WHERE EVDx.idEHREvent = EVDom.idEHREvent
                        AND EVDx.isPrincipal = 1) AS [CÓDIGO CIE 10 PRINCIPAL],

                (SELECT MAX(Dx.name) FROM dbo.EHREventMedicalDiagnostics AS EVDx WITH(NOLOCK)
                    INNER JOIN dbo.diagnostics AS Dx WITH(NOLOCK) ON EVDx.idDiagnostic = Dx.idDiagnostic			
                    WHERE EVDx.idEHREvent = EVDom.idEHREvent
                        AND EVDx.isPrincipal = 1) AS [DESCRIPCIÓN DIAGNÓSTICO CIE 10 (PRINCIPAL)],

                (SELECT TOP 1 Dx.code
                FROM dbo.EHREventMedicalDiagnostics AS EVDx WITH(NOLOCK)
                    INNER JOIN dbo.diagnostics AS Dx WITH(NOLOCK) ON EVDx.idDiagnostic = Dx.idDiagnostic			
                WHERE EVDx.idEHREvent = EVDom.idEHREvent
                    AND EVDx.isPrincipal = 0 ) AS [CÓDIGO CIE 10 SECUNDARIO],

                (SELECT TOP 1 Dx.name
                FROM dbo.EHREventMedicalDiagnostics AS EVDx WITH(NOLOCK)
                    INNER JOIN dbo.diagnostics AS Dx WITH(NOLOCK) ON EVDx.idDiagnostic = Dx.idDiagnostic			
                WHERE EVDx.idEHREvent = EVDom.idEHREvent
                        AND EVDx.isPrincipal = 0) AS [DESCRIPCIÓN DIAGNÓSTICO CIE 10 (SECUNDARIO)],

                ISNULL((SELECT TOP 1 CONVERT(VARCHAR,SUM(EVScaleQ.value)) FROM dbo.EHREventMedicalScaleQuestions AS EVScaleQ WITH(NOLOCK)
                        WHERE EVScaleQ.idEHREvent = EVDom.idEHREvent
                            AND EVScaleQ.idScale = 11),'N/A') AS [BARTHEL],

                FORMAT(EVDom.actionRecordedDate,'dd/MM/yyyy') AS [VISITA MÉDICA],

                (SELECT FORMAT(DATEADD(MONTH,1,EVDom.actionRecordedDate),'dd/MM/yyyy')) AS [PRÓXIMA VISITA MÉDICA],

                (SELECT MAX('X') FROM dbo.EHREventMedicalDiagnostics AS EVDx WITH(NOLOCK)
                    INNER JOIN dbo.diagnostics AS Dx WITH(NOLOCK) ON EVDx.idDiagnostic = Dx.idDiagnostic
                        WHERE EVDx.idEHREvent = EVDom.idEHREvent
                        AND Dx.code BETWEEN 'I10' AND 'I15') AS [HIPERTENSIÓN ARTERIAL],

                (SELECT MAX('X') FROM dbo.EHREventMedicalDiagnostics AS EVDx WITH(NOLOCK)
                    INNER JOIN dbo.diagnostics AS Dx WITH(NOLOCK) ON EVDx.idDiagnostic = Dx.idDiagnostic
                        WHERE EVDx.idEHREvent = EVDom.idEHREvent
                        AND Dx.code BETWEEN 'E10' AND 'E14') AS [DIABETES],

                (SELECT MAX('X') FROM dbo.EHREventMedicalDiagnostics AS EVDx WITH(NOLOCK)
                    INNER JOIN dbo.diagnostics AS Dx WITH(NOLOCK) ON EVDx.idDiagnostic = Dx.idDiagnostic
                        WHERE EVDx.idEHREvent = EVDom.idEHREvent
                        AND Dx.code LIKE 'N18%') AS [ENFERMEDAD RENAL CRÓNICA],

                PatPC.value AS [RECOBRO],

                (SELECT ISNULL(MAX('SI'),'NO') FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Enfermer_a%') AS [ENFERMERÍA ORDENADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.legalCode = '89010502') AS [ORDENADO ENFER12 HORAS DIARIAS POR AUXILIAR DE ENFERMERÍA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.isActive = 1
                        AND Prod.legalCode = '89010502') AS [EJECUTADO ENFER12 HORAS DIARIAS POR AUXILIAR DE ENFERMERÍA],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.legalCode = '89010503') AS [ORDENADO 12 HORAS NOCTURNAS POR AUXILIAR DE ENFERMERÍA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.isActive = 1
                        AND Prod.legalCode = '89010503') AS [EJECUTADO 12 HORAS NOCTURNAS POR AUXILIAR DE ENFERMERÍA],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.legalCode = '89010506') AS [ORDENADO 6 HORAS DIARIAS POR AUXILIAR DE ENFERMERÍA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.isActive = 1
                        AND Prod.legalCode = '89010506') AS [EJECUTADO 6 HORAS DIARIAS POR AUXILIAR DE ENFERMERÍA],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.legalCode = '89010501') AS [ORDENADO 8 HORAS POR AUXILIAR DE ENFERMERÍA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.isActive = 1
                        AND Prod.legalCode = '89010501') AS [EJECUTADO 8 HORAS POR AUXILIAR DE ENFERMERÍA],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.legalCode = '890105') AS [ORDENADO ATENCION (VISITA) DOMICILIARIA, POR ENFERMERÍA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.isActive = 1
                        AND Prod.legalCode = '890105') AS [EJECUTADO ATENCION (VISITA) DOMICILIARIA, POR ENFERMERÍA],


                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND (Prod.name LIKE '%Terapia%F_sica%'
                            OR Prod.name LIKE '%Fisioter%')) AS [TF ORDENADO],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND (Prod.name LIKE '%Terapia%F_sica%'
                            OR Prod.name LIKE '%Fisioter%')) AS [TF EJECUTADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Terapia%Ocupac%') AS [TO ORDENADO],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name lIKE '%Terapia%Ocupac%') AS [TO EJECUTADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND (Prod.name LIKE '%Terapia%Leng%'
                            OR Prod.name LIKE '%Fonoaudio%')) AS [TL ORDENADO],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND (Prod.name LIKE '%Terapia%Leng%'
                            OR Prod.name LIKE '%Fonoaudio%')) AS [TL EJECUTADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Terapia%Resp%') AS [TR ORDENADO],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Terapia%Resp%') AS [TR EJECUTADO],

                (SELECT ISNULL(MAX('SI'),'NO') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.productGenerics AS Gen WITH(NOLOCK) ON EVForm.idproductGeneric = Gen.idGenericProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Gen.name LIKE '%OXIGENO%'
                        AND EVForm.formulatedAmount > 0) AS [O2],

                (SELECT ISNULL(MAX('SI'),'NO') FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Curaci_n%'
                        AND EncHCAct.quantityTODO > 0) AS [CURACIONES],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Curaci_n%') AS [NÚMERO DE CURACIONES ORDENADAS],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Curaci_n%') AS [CURACIONES EJECUTADAS],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Nutrici_n%') AS [NUTRICIÓN],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Nutrici_n%') AS [NUTRICIÓN EJECUTADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Psicolog_a%') AS [PSICOLOGÍA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Psicolog_a%') AS [PSICOLOGÍA EJECUTADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Trabajo%social%') AS [TRABAJO SOCIAL],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Trabajo%social') AS [TRABAJO SOCIAL EJECUTADO],
                    
                (SELECT STUFF((SELECT DISTINCT ',' + Prod.name FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.Products AS Prod WITH(NOLOCK )ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND (Prod.name LIKE '%INTERCONSULTA%'
                            OR Prod.name LIKE '%CONSULTA%')
                        FOR XML PATH (''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')) AS [ESPECIALIDADES],

                (SELECT ISNULL(MAX('SI'),'NO') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                        INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                        INNER JOIN dbo.productCategories AS ProdC WITH(NOLOCK) ON Prod.idProductCategory = ProdC.idProductCategory
                        INNER JOIN dbo.EHREvents AS EVL WITH(NOLOCK) ON EVForm.idEHREvent = EVL.idEHREvent
                    WHERE EVL.idEncounter = Enc.idEncounter
                        AND ProdC.code = 'IMG') AS [LABORATORIO CLÍNICO],

                (SELECT TOP 1 EHRAct.valueText FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                        WHERE EHRAct.idEvent = EVDom.idEHREvent
                            AND EHRAct.idConfigActivity = 34
                            AND EHRAct.idElement = 1) AS [TELEMEDICINA],

                (SELECT TOP 1 EHRAct.valueText FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                        WHERE EHRAct.idEvent = EVDom.idEHREvent
                            AND EHRAct.idConfigActivity = 35
                            AND EHRAct.idElement = 1) AS [TIPO TRANSPORTE],

                IIF(Enc.idDischargeUser <> 1, ISNULL(EncDest.description, (SELECT TOP 1 Dest.name FROM dbo.EHREventMedicalCarePlan AS EVCP WITH(NOLOCK)
                        INNER JOIN dbo.EHRConfEventDestination AS Dest WITH(NOLOCK) ON EVCP.idEventDestination = Dest.idEventDestination
                    WHERE EVCP.idEHREvent = EVDom.idEHREvent)),
                        ISNULL((SELECT TOP 1 Dest.name FROM dbo.EHREventMedicalCarePlan AS EVCP WITH(NOLOCK)
                        INNER JOIN dbo.EHRConfEventDestination AS Dest WITH(NOLOCK) ON EVCP.idEventDestination = Dest.idEventDestination
                    WHERE EVCP.idEHREvent = EVDom.idEHREvent), EncDest.description)) AS [DESTINO DE EGRESO],

                (SELECT ISNULL(MAX('SI'),'NO')
                    FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                        INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHRevent = EVF.idEHRevent
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND EVForm.idProductType = 2) AS [MANEJO FARMACOLÓGICO],

                (SELECT TOP 1 Inc.days FROM dbo.EHReventIncapacities AS Inc WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVInc WITH(NOLOCK) ON Inc.idEHREvent = EVInc.idEHREvent
                    WHERE EVInc.idEncounter = Enc.idENcounter
                    ORDER BY EVInc.idEHREvent DESC) AS [DÍAS DE INCAPACIDAD],

                '' AS [TALLER DE CUIDADORES], --BLANK
                (SELECT STUFF((SELECT DISTINCT ',' + UPPER(Edu.title)
                            FROM dbo.EHREventPromotionEducation  AS EHREdu WITH(NOLOCK)
                                INNER JOIN dbo.EHRConfEducation AS Edu WITH(NOLOCK) ON EHREdu.idRecordEducation = Edu.idRecord
                            WHERE EHREdu.idEHREvent = EVDom.idEHREvent
                        FOR XML PATH (''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '')) AS [OBSERVACIONES Y RECOMENDACIONES POR EL PROVEEDOR],

                (SELECT TOP 1 EVCP.carePlan FROM dbo.EHREventMedicalCarePlan AS EVCP WITH(NOLOCK)
                    WHERE EVCP.idEHREvent = EVDom.idEHREvent) AS [EVOLUCIÓN],

                (SELECT TOP 1 Note.note FROM dbo.EHREvents AS EVT WITH(NOLOCK)
                    INNER JOIN dbo.EHREventNotes AS Note WITH(NOLOCK) ON EVT.idEHREvent = Note.idEHREvent
                        WHERE EVT.idEncounter = Enc.idEncounter
                            AND EVT.idAction = 405
                        ORDER BY EVT.idEHREvent DESC) AS [SEGUIMIENTO TELEFÓNICO],

                (SELECT STRING_AGG(EHREle.nameElement,'-')
                        FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                            INNER JOIN dbo.EHRConfCustomActivityElements AS EHREle WITH(NOLOCK) ON EHRAct.idConfigActivity = EHREle.idConfigActivity
                                AND EHRAct.idElement = EHREle.idElement
                        WHERE EHRAct.valueText IN ('True','Si')
                            AND EHRAct.idElement IN (1,2,3,4,5,6)
                            AND EHRAct.idEvent = (SELECT MAX(EVE.idEHREvent) FROM dbo.EHREvents AS EVE
                                    INNER JOIN dbo.EHREventCustomActivities AS EVCAct ON EVE.idEHREvent = EVCAct.idEvent
                                WHERE EVE.idPatient = Enc.idUserPatient
                                    AND EVE.idAction = 405 -- Seguimiento Telefónico DOMI
                                    AND EVE.actionRecordedDate < DATEADD(DAY,1,@endDate)
                                    AND EVCact.idConfigActivity = 181
                                    AND EVCAct.idElement IN (1,2,3,4,5,6))
                                    ) AS [JUNTA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        ) AS [NÚMERO DE VALORACIONES],

                '' AS [PAÑALES],

                STUFF((SELECT DISTINCT '/' + CONCAT('(',Prod.legalCode,')',Prod.name) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    INNER JOIN dbo.productCategories AS ProdC WITH(NOLOCK) ON Prod.idProductCategory = ProdC.idProductCategory
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND ProdC.code = 'IMG'
                            FOR XML PATH (''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS [LABORATORIOS],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode IN ('890274','890374','890474')) AS [NEUROLOGÍA],

                (SELECT COUNT(EVN.idEHREvent) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                        INNER JOIN dbo.generalActions AS Act WITH(NOLOCK) ON EVN.idAction = Act.idAction
                    WHERE EVN.idPatient = Pat.idUser
                        AND CONVERT(DATE,EVN.actionRecordedDate) BETWEEN @startDate AND @endDate
                        AND EVN.actionRecordedDate > (SELECT ISNULL(MAX(EVForm.dateRecorded),'') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                            INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                            INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                            WHERE EVF.idEncounter = Enc.idEncounter
                                AND Prod.legalCode IN ('890274','890374','890474'))
                        AND Act.code = 'HCC047') AS [NEUROLOGÍA EJECUTADO],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode IN ('890264','890364','890464')) AS [FISIATRÍA],

                (SELECT COUNT(EVN.idEHREvent) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                        INNER JOIN dbo.generalActions AS Act WITH(NOLOCK) ON EVN.idAction = Act.idAction
                    WHERE EVN.idPatient = Pat.idUser
                        AND CONVERT(DATE,EVN.actionRecordedDate) BETWEEN @startDate AND @endDate
                        AND EVN.actionRecordedDate > (SELECT ISNULL(MAX(EVForm.dateRecorded),'') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                            INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                            INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                            WHERE EVF.idEncounter = Enc.idEncounter
                                AND Prod.legalCode IN ('890264','890364','890464'))
                        AND Act.code = 'HCC088') AS [FISIATRÍA EJECUTADO],

                (SELECT ISNULL(SUM(EncHCAct.quantityTODO),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.legalCode = '890502') AS [JUNTA FISIATRÍA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.legalCode = '890502') AS [JUNTA FISIATRÍA EJECUTADO],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode IN ('890266','890366','890466')) AS [MEDICINA INTERNA],

                (SELECT COUNT(EVN.idEHREvent) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                        INNER JOIN dbo.generalActions AS Act WITH(NOLOCK) ON EVN.idAction = Act.idAction
                    WHERE EVN.idPatient = Pat.idUser
                        AND CONVERT(DATE,EVN.actionRecordedDate) BETWEEN @startDate AND @endDate
                        AND EVN.actionRecordedDate > (SELECT ISNULL(MAX(EVForm.dateRecorded),'') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                            INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                            INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                            WHERE EVF.idEncounter = Enc.idEncounter
                                AND Prod.legalCode IN ('890266','890366','890466'))
                        AND Act.code = 'HCC021') AS [MEDICINA INTERNA EJECUTADO],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode IN ('890271', '890371', '890471')) AS [NEUMOLOGÍA],

                (SELECT COUNT(EVN.idEHREvent) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                        INNER JOIN dbo.generalActions AS Act WITH(NOLOCK) ON EVN.idAction = Act.idAction
                    WHERE EVN.idPatient = Pat.idUser
                        AND CONVERT(DATE,EVN.actionRecordedDate) BETWEEN @startDate AND @endDate
                        AND EVN.actionRecordedDate > (SELECT ISNULL(MAX(EVForm.dateRecorded),'') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                            INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                            INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                            WHERE EVF.idEncounter = Enc.idEncounter
                                AND Prod.legalCode IN ('890271', '890371', '890471'))
                        AND Act.code = 'HCC025') AS [NEUMOLOGÍA EJECUTADO],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode IN ('890228','890328','890428','890329')) AS [CARDIOLOGÍA],

                (SELECT COUNT(EVN.idEHREvent) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                        INNER JOIN dbo.generalActions AS Act WITH(NOLOCK) ON EVN.idAction = Act.idAction
                    WHERE EVN.idPatient = Pat.idUser
                        AND CONVERT(DATE,EVN.actionRecordedDate) BETWEEN @startDate AND @endDate
                        AND EVN.actionRecordedDate > (SELECT ISNULL(MAX(EVForm.dateRecorded),'') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                            INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                            INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                            WHERE EVF.idEncounter = Enc.idEncounter
                                AND Prod.legalCode IN ('890228','890328','890428','890329'))
                        AND Act.code = 'HCC053') AS [CARDIOLOGÍA EJECUTADO],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode IN ('890272','890372','890472')) AS [NEUMOLOGÍA PEDIÁTRICA],

                (SELECT COUNT(EVN.idEHREvent) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                        INNER JOIN dbo.generalActions AS Act WITH(NOLOCK) ON EVN.idAction = Act.idAction
                    WHERE EVN.idPatient = Pat.idUser
                        AND CONVERT(DATE,EVN.actionRecordedDate) BETWEEN @startDate AND @endDate
                        AND EVN.actionRecordedDate > (SELECT ISNULL(MAX(EVForm.dateRecorded),'') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                            INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                            INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                            WHERE EVF.idEncounter = Enc.idEncounter
                                AND Prod.legalCode IN ('890272','890372','890472'))
                        AND Act.code = 'HCC015') AS [NEUMOLOGÍA PEDIÁTRICA EJECUTADO],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode IN ('890275', '890375', '890475')) AS [NEUROLOGÍA PEDIÁTRICA],

                (SELECT COUNT(EVN.idEHREvent) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                        INNER JOIN dbo.generalActions AS Act WITH(NOLOCK) ON EVN.idAction = Act.idAction
                    WHERE EVN.idPatient = Pat.idUser
                        AND CONVERT(DATE,EVN.actionRecordedDate) BETWEEN @startDate AND @endDate
                        AND EVN.actionRecordedDate > (SELECT ISNULL(MAX(EVForm.dateRecorded),'') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                            INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                            INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                            WHERE EVF.idEncounter = Enc.idEncounter
                                AND Prod.legalCode IN ('890275', '890375', '890475'))
                        AND Act.code = 'HCC050') AS [NEUROLOGÍA PEDIÁTRICA EJECUTADO],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode IN ('890291', '890391', '890491')) AS [TOXICOLOGÍA],

                (SELECT COUNT(EVN.idEHREvent) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                        INNER JOIN dbo.generalActions AS Act WITH(NOLOCK) ON EVN.idAction = Act.idAction
                    WHERE EVN.idPatient = Pat.idUser
                        AND CONVERT(DATE,EVN.actionRecordedDate) BETWEEN @startDate AND @endDate
                        AND EVN.actionRecordedDate > (SELECT ISNULL(MAX(EVForm.dateRecorded),'') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                            INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                            INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                            WHERE EVF.idEncounter = Enc.idEncounter
                                AND Prod.legalCode IN ('890291', '890391', '890491'))
                        AND Act.code = 'HCC028') AS [TOXICOLOGÍA EJECUTADO],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode = '') AS [FARMACOLOGÍA CLÍNICA],

                (SELECT ISNULL(COUNT(EVN.idEHREvent),0) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                    WHERE EVN.idENcounter = Enc.idEncounter
                        AND EVN.idAction = 0) AS [FARMACOLOGÍA CLÍNICA EJECUTADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Geriatr_a%') AS [GERIATRÍA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Geriatr_a%') AS [GERIATRÍA EJECUTADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Psiquiatr_a%') AS [PSIQUIATRÍA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Psiquiatr_a%') AS [PSIQUIATRÍA EJECUTADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Ortopedia%') AS [ORTOPEDIA],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Ortopedia%') AS [ORTOPEDIA EJECUTADO],

                (SELECT ISNULL(SUM(EVForm.formulatedAmount),0) FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                    WHERE EVF.idEncounter = Enc.idEncounter
                        AND Prod.legalCode IN ('890283', '890383', '890483')) AS [PEDIATRÍA],

                (SELECT COUNT(EVN.idEHREvent) FROM dbo.EHREvents AS EVN WITH(NOLOCK)
                        INNER JOIN dbo.generalActions AS Act WITH(NOLOCK) ON EVN.idAction = Act.idAction
                    WHERE EVN.idPatient = Pat.idUser
                        AND CONVERT(DATE,EVN.actionRecordedDate) BETWEEN @startDate AND @endDate
                        AND EVN.actionRecordedDate > (SELECT ISNULL(MAX(EVForm.dateRecorded),'') FROM dbo.EHREventFormulation AS EVForm WITH(NOLOCK)
                            INNER JOIN dbo.EHREvents AS EVF WITH(NOLOCK) ON EVForm.idEHREvent = EVF.idEHREvent
                            INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EVForm.idProduct = Prod.idProduct
                            WHERE EVF.idEncounter = Enc.idEncounter
                                AND Prod.legalCode IN ('890283', '890383', '890483'))
                        AND Act.code = 'HCC008') AS [PEDIATRÍA EJECUTADO],

                (SELECT MAX(EHRAct.valueText) FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
                        WHERE EV.idEncounter = Enc.idEncounter
                            AND EHRAct.idConfigActivity = 33
                            AND EHRAct.idElement = 5) AS [INGRESOS REINGRESOS],

                (SELECT TOP 1 EHRT.valueText
                    FROM dbo.EHREventCustomActivities AS EHRT WITH(NOLOCK)
                    WHERE EHRT.idConfigActivity = 181
                        AND EHRT.idElement = 6
                        AND EHRT.valueText <> 'NO'
                        AND EHRT.idEvent = (SELECT MAX(EVE.idEHREvent) FROM dbo.EHREvents AS EVE
                                    INNER JOIN dbo.EHREventCustomActivities AS EVCAct ON EVE.idEHREvent = EVCAct.idEvent
                                WHERE EVE.idPatient = Enc.idUserPatient
                                    AND EVE.idAction = 405 -- Seguimiento Telefónico DOMI
                                    AND EVE.actionRecordedDate < DATEADD(DAY,1,@endDate)
                                    AND EVCact.idConfigActivity = 181
                                    AND EVCAct.idElement IN (1,2,3,4,5,6))) AS [CERCA DE TI],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Apoyo%Espiritual%') AS [APOYO ESPIRITUAL],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Apoyo%Espiritual%') AS [APOYO ESPIRITUAL EJECUTADO],

                (SELECT ISNULL(SUM(HCAssig.quantity),0) FROM dbo.encounterHCActivityEventGroups AS HCAssig WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON HCAssig.idProduct = Prod.idProduct
                        AND HCAssig.idEHREvent = EVAssig.idEHREvent
                        AND Prod.name LIKE '%Cuidados%Paliativos%') AS [CUIDADOS PALIATIVOS],

                (SELECT ISNULL(SUM(EncHCAct.quantityDone),0) FROM dbo.encounterHCActivities AS EncHCAct WITH(NOLOCK)
                    INNER JOIN dbo.products AS Prod WITH(NOLOCK) ON EncHCAct.idProduct = Prod.idProduct
                    WHERE EncHCAct.idEncounter = EncHC.idEncounter
                        AND EncHCAct.idHCRecord = EncHC.idHCRecord
                        AND EncHCAct.isActive = 1
                        AND Prod.name LIKE '%Cuidados%Paliativos%') AS [CUIDADOS PALIATIVOS EJECUTADOS]
            FROM dbo.encounters AS Enc WITH(NOLOCK)
                INNER JOIN dbo.users AS Pat WITH(NOLOCK) ON Enc.idUserPatient = Pat.idUser
            
                INNER JOIN dbo.userConfTypeDocuments AS DocT WITH(NOLOCK) ON Pat.idDocumentType = DocT.idTypeDocument
                INNER JOIN dbo.userPeople AS PatP WITH(NOLOCK) ON Pat.idUser = PatP.idUser
                LEFT OUTER JOIN dbo.generalListDetails AS PatPC WITH(NOLOCK) ON PatP.idClassification = PatPC.idListDetail

                INNER JOIN dbo.userConfAdministrativeSex AS Gender WITH(NOLOCK) ON PatP.idAdministrativeSex = Gender.idAdministrativeSex
                LEFT OUTER JOIN dbo.generalPoliticalDivisions AS Neig WITH(NOLOCK) ON PatP.idHomePoliticalDivisionNeighborhood = Neig.idPoliticalDivision
                LEFT OUTER JOIN dbo.generalPoliticalDivisions AS Mun WITH(NOLOCK) ON PatP.idHomePlacePoliticalDivision = Mun.idPoliticalDivision

                INNER JOIN dbo.encounterRecords AS EncR WITH(NOLOCK) ON Enc.idEncounter = EncR.idEncounter
                INNER JOIN dbo.contractPlans AS ContP WITH(NOLOCK) ON EncR.idPrincipalPlan = ContP.idPlan
                INNER JOIN dbo.users AS Comp WITH(NOLOCK) ON Enc.idUserCompany = Comp.idUser
                LEFT OUTER JOIN dbo.encounterRelatedPeople AS EncRP WITH(NOLOCK) ON Enc.idEncounter = EncRP.idEncounter
                    AND (EncRP.isResponsible = 1 OR EncRP.isCompanion = 1 OR EncRP.isContact = 1)
                LEFT OUTER JOIN dbo.generalListDetails AS EncDest WITH(NOLOCK) ON Enc.idDischargeDestination = EncDest.idListDetail
            
                OUTER APPLY
                (
                    SELECT *
                    FROM
                    (
                        SELECT Loc.valueLocation,
                            TLoc.code
                        FROM dbo.userLocations AS Loc WITH(NOLOCK)
                            INNER JOIN dbo.userConfTypeLocations AS TLoc WITH(NOLOCK) ON Loc.idUserTypeLocation = TLoc.idUserTypeLocation
                        WHERE Loc.idUser = Pat.idUser
                    ) AS PV
                    PIVOT(MAX(PV.valueLocation) FOR PV.code IN ([CEL],[TELF],[EMAIL])) AS PV
                ) AS Loc

                INNER JOIN dbo.encounterHC AS EncHC WITH(NOLOCK) ON Enc.idEncounter = EncHC.idEncounter
                INNER JOIN dbo.EHRConfHCActivity AS HCAct WITH(NOLOCK) ON EncHC.idHCActivity = HCAct.idHCActivity

                INNER JOIN dbo.EHREvents AS EVDom WITH(NOLOCK) ON Enc.idUserPatient = EVDom.idPatient
                    AND EVDom.idAction = 83
                    AND EVDom.idEHREvent = (SELECT MAX(V.idEHREvent) -- LAST EVENT
                        FROM dbo.EHREvents AS V
                            INNER JOIN dbo.EHREventCustomActivities AS EVCAct WITH(NOLOCK) ON V.idEHREvent = EVCAct.idEvent
                                AND EVCAct.idConfigActivity IN (52, 54)
                                AND EVCAct.idElement = 1
                                AND EVCAct.valueNumeric IN (1759,1760,2006,2007,2009,2005,2008)
                        WHERE V.idPatient = Enc.idUserPatient --	Por Paciente
                            AND V.actionRecordedDate < DATEADD(DAY,1,@endDate)
                            AND idAction = 83)

                LEFT OUTER JOIN dbo.EHREvents AS EVAssig WITH(NOLOCK) ON Enc.idEncounter = EVAssig.idEncounter
                    AND EVAssig.idEHREvent = (SELECT MAX(idEHREvent)
                        FROM dbo.EHREvents AS V WITH(NOLOCK)
                            LEFT OUTER JOIN dbo.EHREventCUstomActivities AS VAct WITH(NOLOCK) ON V.idEHREvent = VAct.idEvent
                                AND Vact.idConfigActivity = 54
                                AND Vact.valueNumeric IN (2006,2007,2009)
                        WHERE V.idEncounter = Enc.idEncounter
                            AND V.idAction = 347
                            AND (VAct.idEvent IS NOT NULL OR V.actionRecordedDate < '20200605'))
            WHERE Enc.idUserCompany = @idUserCompany
                AND Enc.idEncounterClass = 1 -- Domiciliario
                AND EncHC.isActive = 1
                AND HCAct.idHCActivity IN (SELECT Value FROM STRING_SPLIT(@hcPlanF,','))
                AND CONVERT(DATE,Enc.dateStart) BETWEEN CONVERT(DATE,@startDate) AND CONVERT(DATE,@endDate)
                AND EXISTS (
                    SELECT 1
                    FROM dbo.EHREvents AS EVFilt
                    WHERE EVFilt.idEncounter = Enc.idEncounter
                    AND EVFilt.idAction NOT IN (313, 98, 585, 380, 783)
                )                
        ) SELECT
            [FECHA DEL REGISTRO]
            ,[REGISTRO]
            ,[MES DEL REGISTRO]
            ,[IPS DOMICILIO]
            ,[NOMBRES COMPLETOS]
            ,[TIPO DOCUMENTO]
            ,[NÚMERO DOCUMENTO DE IDENTIFICACIÓN]
            ,[EDAD]
            ,[GRUPO DE EDAD]
            ,[SEXO]
            ,[PLAN DE BENEFICIOS]
            ,[DIRECCIÓN]
            ,[BARRIO]
            ,[LOCALIDAD]
            ,[MUNICIPIO]
            ,[CUIDADOR RESPONSABLE]
            ,[TELÉFONO]
            ,[CELULAR]
            ,[VÍA DE INGRESO]
            ,[PRONÓSTICO]
            ,[FECHA DE PRESENTACIÓN]
            ,CAST([AÑO INGRESO] AS INT) AS [AÑO INGRESO]
            ,CAST([MES INGRESO] AS INT) AS [MES INGRESO]
            ,CAST([DÍA INGRESO] AS INT) AS [DÍA INGRESO]
            ,[FECHA INGRESO]
            ,CAST([AÑO EGRESO] AS INT) AS [AÑO EGRESO]
            ,CAST([MES DEL EGRESO] AS INT) AS [MES DEL EGRESO]
            ,CAST([DÍA EGRESO] AS INT) AS [DÍA EGRESO]
            ,[FECHA EGRESO]
            ,[DIAS ESTANCIA]
            ,[CÓDIGO CIE 10 PRINCIPAL]
            ,[DESCRIPCIÓN DIAGNÓSTICO CIE 10 (PRINCIPAL)]
            ,[CÓDIGO CIE 10 SECUNDARIO]
            ,[DESCRIPCIÓN DIAGNÓSTICO CIE 10 (SECUNDARIO)]
            ,[BARTHEL]
            ,IIF(DATEDIFF(MONTH,
                CASE
                WHEN SUBSTRING([VISITA MÉDICA],3,1) = '/' AND SUBSTRING([VISITA MÉDICA],6,1) = '/' THEN CONCAT(SUBSTRING([VISITA MÉDICA],7,4), '-', SUBSTRING([VISITA MÉDICA],4,2), '-', SUBSTRING([VISITA MÉDICA],1,2))
                WHEN SUBSTRING([VISITA MÉDICA],3,1) = '/' AND SUBSTRING([VISITA MÉDICA],5,1) = '/' THEN CONCAT(SUBSTRING([VISITA MÉDICA],6,4), '-0', SUBSTRING([VISITA MÉDICA],4,1), '-', SUBSTRING([VISITA MÉDICA],1,2))
                WHEN SUBSTRING([VISITA MÉDICA],2,1) = '/' AND SUBSTRING([VISITA MÉDICA],5,1) = '/' THEN CONCAT(SUBSTRING([VISITA MÉDICA],6,4), '-', SUBSTRING([VISITA MÉDICA],3,2), '-0', SUBSTRING([VISITA MÉDICA],1,1))
                WHEN SUBSTRING([VISITA MÉDICA],2,1) = '/' AND SUBSTRING([VISITA MÉDICA],4,1) = '/' THEN CONCAT(SUBSTRING([VISITA MÉDICA],5,4), '-0', SUBSTRING([VISITA MÉDICA],3,1), '-0', SUBSTRING([VISITA MÉDICA],1,1))
                END
                ,@startDate) = 0, [VISITA MÉDICA], '0') AS [VISITA MÉDICA]
            ,IIF(DATEDIFF(MONTH,
                CASE
                WHEN SUBSTRING([VISITA MÉDICA],3,1) = '/' AND SUBSTRING([VISITA MÉDICA],6,1) = '/' THEN CONCAT(SUBSTRING([VISITA MÉDICA],7,4), '-', SUBSTRING([VISITA MÉDICA],4,2), '-', SUBSTRING([VISITA MÉDICA],1,2))
                WHEN SUBSTRING([VISITA MÉDICA],3,1) = '/' AND SUBSTRING([VISITA MÉDICA],5,1) = '/' THEN CONCAT(SUBSTRING([VISITA MÉDICA],6,4), '-0', SUBSTRING([VISITA MÉDICA],4,1), '-', SUBSTRING([VISITA MÉDICA],1,2))
                WHEN SUBSTRING([VISITA MÉDICA],2,1) = '/' AND SUBSTRING([VISITA MÉDICA],5,1) = '/' THEN CONCAT(SUBSTRING([VISITA MÉDICA],6,4), '-', SUBSTRING([VISITA MÉDICA],3,2), '-0', SUBSTRING([VISITA MÉDICA],1,1))
                WHEN SUBSTRING([VISITA MÉDICA],2,1) = '/' AND SUBSTRING([VISITA MÉDICA],4,1) = '/' THEN CONCAT(SUBSTRING([VISITA MÉDICA],5,4), '-0', SUBSTRING([VISITA MÉDICA],3,1), '-0', SUBSTRING([VISITA MÉDICA],1,1))
                END
            ,@startDate) = 0, [PRÓXIMA VISITA MÉDICA], '0') AS [PRÓXIMA VISITA MÉDICA]
            ,[HIPERTENSIÓN ARTERIAL]
            ,[DIABETES]
            ,[ENFERMEDAD RENAL CRÓNICA]
            ,IIF([RECOBRO] IS NULL, 'N/A', [RECOBRO]) AS [RECOBRO]
            ,[ENFERMERÍA ORDENADO]
            ,[ORDENADO ENFER12 HORAS DIARIAS POR AUXILIAR DE ENFERMERÍA]
            ,[EJECUTADO ENFER12 HORAS DIARIAS POR AUXILIAR DE ENFERMERÍA]
            ,[ORDENADO 12 HORAS NOCTURNAS POR AUXILIAR DE ENFERMERÍA]
            ,[EJECUTADO 12 HORAS NOCTURNAS POR AUXILIAR DE ENFERMERÍA]
            ,[ORDENADO 6 HORAS DIARIAS POR AUXILIAR DE ENFERMERÍA]
            ,[EJECUTADO 6 HORAS DIARIAS POR AUXILIAR DE ENFERMERÍA]
            ,[ORDENADO 8 HORAS POR AUXILIAR DE ENFERMERÍA]
            ,[EJECUTADO 8 HORAS POR AUXILIAR DE ENFERMERÍA]
            ,[ORDENADO ATENCION (VISITA) DOMICILIARIA, POR ENFERMERÍA]
            ,[EJECUTADO ATENCION (VISITA) DOMICILIARIA, POR ENFERMERÍA]
            ,[TF ORDENADO]
            ,[TF EJECUTADO]
            ,[TO ORDENADO]
            ,[TO EJECUTADO]
            ,[TL ORDENADO]
            ,[TL EJECUTADO]
            ,[TR ORDENADO]
            ,[TR EJECUTADO]
            ,[O2]
            ,[CURACIONES]
            ,[NÚMERO DE CURACIONES ORDENADAS]
            ,[CURACIONES EJECUTADAS]
            ,[NUTRICIÓN]
            ,[NUTRICIÓN EJECUTADO]
            ,[PSICOLOGÍA]
            ,[PSICOLOGÍA EJECUTADO]
            ,[TRABAJO SOCIAL]
            ,[TRABAJO SOCIAL EJECUTADO]
            ,[ESPECIALIDADES]
            ,[LABORATORIO CLÍNICO]
            ,[TELEMEDICINA]
            ,[TIPO TRANSPORTE]
            ,[DESTINO DE EGRESO]
            ,[MANEJO FARMACOLÓGICO]
            ,[DÍAS DE INCAPACIDAD]
            ,[TALLER DE CUIDADORES]
            ,[OBSERVACIONES Y RECOMENDACIONES POR EL PROVEEDOR]
            ,[EVOLUCIÓN]
            ,[SEGUIMIENTO TELEFÓNICO]
            ,[JUNTA]
            ,[NÚMERO DE VALORACIONES]
            ,[PAÑALES]
            ,[LABORATORIOS]
            ,[NEUROLOGÍA]
            ,[NEUROLOGÍA EJECUTADO]
            ,[FISIATRÍA]
            ,[FISIATRÍA EJECUTADO]
            ,[JUNTA FISIATRÍA]
            ,[JUNTA FISIATRÍA EJECUTADO]
            ,[MEDICINA INTERNA]
            ,[MEDICINA INTERNA EJECUTADO]
            ,[NEUMOLOGÍA]
            ,[NEUMOLOGÍA EJECUTADO]
            ,[CARDIOLOGÍA]
            ,[CARDIOLOGÍA EJECUTADO]
            ,[NEUMOLOGÍA PEDIÁTRICA]
            ,[NEUMOLOGÍA PEDIÁTRICA EJECUTADO]
            ,[NEUROLOGÍA PEDIÁTRICA]
            ,[NEUROLOGÍA PEDIÁTRICA EJECUTADO]
            ,[TOXICOLOGÍA]
            ,[TOXICOLOGÍA EJECUTADO]
            ,[FARMACOLOGÍA CLÍNICA]
            ,[FARMACOLOGÍA CLÍNICA EJECUTADO]
            ,[GERIATRÍA]
            ,[GERIATRÍA EJECUTADO]
            ,[PSIQUIATRÍA]
            ,[PSIQUIATRÍA EJECUTADO]
            ,[ORTOPEDIA]
            ,[ORTOPEDIA EJECUTADO]
            ,[PEDIATRÍA]
            ,[PEDIATRÍA EJECUTADO]
            ,[INGRESOS REINGRESOS]
            ,[CERCA DE TI]
            ,[APOYO ESPIRITUAL]
            ,[APOYO ESPIRITUAL EJECUTADO]
            ,[CUIDADOS PALIATIVOS]
            ,[CUIDADOS PALIATIVOS EJECUTADOS]
        FROM CTE
    """
    # Lectura de data frame
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    print("Como esta leyendo el dataframe inicialmente",df)
    print("Nombres y tipos de columnas leídos del dataframe sin transformar",df.dtypes)
    # Convertir a str los campos de tipo fecha 
    cols_dates = [i for i in df.columns if 'Fecha' in i]
    for col in cols_dates:
        df[col] = df[col].astype(str)
        
    # Convertir en int los campos float
    cols_float = df.select_dtypes(include=[np.float]).columns
    for col in cols_float:
        df[col] = df[col].astype('Int64')

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', -1)
    print(df.columns)
    print(df.dtypes)
    print(df)
    ### Retorno
    return df
    
### Metodo de cargue de malla final

def func_get_SAL_DOM_CO_MallaFinal():
	"""Metodo ETL para el DAG #1

	Parametros: 
	Ninguno

	Retorna:
	Retorno vacio
	"""
	df = query_SAL_DOM_CO_MallaFinal(start_date = start_date, end_date = end_date)
	# pd.set_option('display.max_rows', None)
	# pd.set_option('display.max_columns', None)
	# pd.set_option('display.width', None)
	# pd.set_option('display.max_colwidth', -1)
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

with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los dias a las 6 15 am (hora servidor)
    schedule_interval= '15 6 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_SAL_DOM_CO_MallaFinal_python_task = PythonOperator(
                                                    task_id = "get_SAL_DOM_CO_MallaFinal",
                                                    python_callable = func_get_SAL_DOM_CO_MallaFinal,
                                                    email_on_failure = True, 
                                                    email='BI@clinicos.com.co',
                                                    dag=dag
                                                    )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_SAL_DOM_CO_MallaFinal_Task = MsSqlOperator(task_id='Load_SAL_DOM_CO_MallaFinal',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE sp_load_SAL_DOM_CO_MallaFinal",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_SAL_DOM_CO_MallaFinal_python_task >> load_SAL_DOM_CO_MallaFinal_Task >> task_end