"""
Proyecto: Masiva NEPS

author dag: Luis Esteban Santamaría. Ingeniero de Datos.
Fecha creación: 28/08/2023

"""

# Librerias
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


#  Creación de variables
db_table = "TblHMasiva"
db_tmp_table = "TmpMasiva"
dag_name = 'dag_' + db_table

# Para correr manualmente las fechas
fecha_texto = '2023-01-31 00:00:00'
now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

now = now.strftime('%Y-%m-%d %H:%M:%S')
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

def func_get_factMasiva():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)

    # LECTURA DE DATOS
    query = f"""    
        -- DECLARACIÓN DE VARIABLES
        DECLARE 
                @idUserCompany INT= 1,
                @dateStart DATETIME = '2023/06/01', -- '2023/06/01',
                @dateEnd DATETIME = '2023/06/02', -- '2023/08/07',
                @OfficeFilter VARCHAR(MAX) = '1,17',--(SELECT STRING_AGG(idOffice,',') FROM companyOffices WHERE idUserCompany = 352666),
                @idIns VARCHAR(MAX) = '16,33,285991,20,266465,422816,289134,17,150579,358811,39,88813,4,24,22,25,150571,302708,26,289154,365849,266467,7,28,23,420,32,421',
                @idCont VARCHAR(MAX) = '83,81,79,76,84,77,88,82,78,80,92'

        SELECT
            Todo.idMasiva,
            Todo.idEncounter,
            Todo.idUser,
            -- Todo.TheDate,
            Todo.idNombreAseguradora,
            Todo.idContratoPrincipal,
            Todo.idEsquemaConfigurable,
            Todo.idConsultaMedica,
            Todo.idOffice,
            Todo.codigoHabilitacion,
            Todo.idEHREvent,
            Todo.idMedicionSignoVital,
            Todo.idDiagnostic,
            Todo.idMedicionMonitoria,
            Todo.INGRESO as ingreso,
            Todo.[NIT IPS] as nitIPS,
            Todo.codigoSucursal,
            Todo.[FECHA DE INGRESO DEL USUARIO A LA IPS PAD] as fechaIngresoUsuarioIPSPAD,
            Todo.TALLA as talla,
            Todo.PESO as peso,
            Todo.[TENSIÓN ARTERIAL SISTÓLICA] as tensionArterialSistolica,
            Todo.[TENSIÓN ARTERIAL DIASTÓLICA] as tensionArterialDiastolica,
            Todo.[CIRCUNFERENCIA ABDOMINAL] as circunferenciaAbdominal,
            Todo.ASPECTO_GENERAL as aspectoGeneral,
            Todo.[INTEGRIDAD DE LA PIEL] as integridadPiel,
            Todo.[RED DE APOYO] as redApoyo,
            Todo.[SOPORTE DE CUIDADOR] as soporteCuidador,
            Todo.[SITUACIÓN ACTUAL DE DISCAPACIDAD] as situacionActualDiscapacidad,
            Todo.ALIMENTACIÓN as alimentacion,
            Todo.[ACTIVIDADES EN BAÑO] as actividadesEnBaño,
            Todo.VESTIRSE as vestirse,
            Todo.[ASEO PERSONAL] as aseoPersonal,
            Todo.[DEPOSICIONES-CONTROL ANAL] as deposicionesControlAnal,
            Todo.[MICCION-CONTROL VESICAL] as miccionControlVesical,
            Todo.[MANEJO DE INODORO O RETRETE] as manejoInodoroORetrete,
            Todo.[TRASLADO SILLA-CAMA] as trasladoSillaCama,
            Todo.[DEAMBULACIÓN TRASLADO] as deambulacionTraslado,
            Todo.[SUBIR O BAJAR ESCALONES] as subirOBajarEscalones,
            Todo.[VALORACIÓN BARTHEL] as valoracionBarthel,
            Todo.[INDICE KARNOFSKY] as indiceKarnofsky,
            Todo.[CARACTERÍSTICAS DE LAS PATOLOGÍAS DE INGRESO DEL PACIENTE] as caracteristicasDePatologiasDeIngresoDelPaciente,
            Todo.[FASE DE LA ENFERMEDAD DE INGRESO EN LA QUE PRESENTA EL USUARIO(A)] as faseEnfermedadDeIngresoQuePresentaUsuario,
            Todo.[ACCIONES INSEGURAS] as accionesInseguras,
            Todo.[EVENTOS ADVERSOS PRESENTADOS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO] as eventosAdversosPresentadosEnAtencionPacientesEnDomicilio,
            Todo.[DESCRIPCIÓN DE OTROS EVENTOS ADVERSOS] as descripcionOtrosEventosAdversos,
            Todo.[NIT IPS DE OCURRENCIA DEL EVENTO ADVERSO] as nitIPSOcurrenciaEventoAdverso,
            Todo.[FECHA DE EVENTO ADVERSO] as fechaEventoAdverso,
            Todo.[GRADO DE LESIÓN DEL EVENTO ADVERSO] as gradoLesionEventoAdverso,
            Todo.[PLAN DE INTERVENCIÓN- EVENTOS ADVERSOS] as planIntervencionEventosAdversos,
            Todo.[PLAN DE INTERVENCIÓN- FALLAS DE CALIDAD] as planIntervecionFallasDeCalidad,
            Todo.OBSERVACIÓN as observacion,
            Todo.[FALLAS DE CALIDAD PRESENTADAS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO] as fallasDeCalidadPresentadasEnAtencionPacientesEnDomicilio,
            Todo.[FECHA DE INGRESO AL PROGRAMA PAD] as fechaIngresoProgramaPAD,
            Todo.[DIAGNÓSTICO PRINCIPAL CIE 10] as diagnosticoPrincipal_CIE_10,
            Todo.[DIAGNÓSTICO NO.02 COMORBILIDAD PRINCIPAL CIE 10] as diagnosticoNo02ComorbilidadPrincipal_CIE_10,
            Todo.[DIAGNÓSTICO NO.03 OTRAS COMORBILIDADES CIE 10] as diagnosticoNo03OtrasComorbilidades_CIE_10,
            Todo.[CANTIDAD DE SERVICIOS SOLICITADOS] as cantidadServiciosSolicitados,
            Todo.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO] as codigoServicioAtencionRequeridaPorUsuario,
            Todo.[MEDICINA GENERAL] as medicinaGeneral,
            Todo.[MEDICINA ESPECIALIZADA] as medicinaEspecializada,
            Todo.[ESPECIALIDAD MÉDICA DE INTERVENCIÓN] as especialidadMedicaDeIntervencion,
            Todo.[ENFERMERIA PROFESIONAL] as enfermeriaProfesional,
            Todo.[NUTRICIÓN Y DIETÉTICA] as nutricionYDietetica,
            Todo.PSICOLOGÍA as psicologia,
            Todo.[TRABAJO SOCIAL] as trabajoSocial,
            Todo.[FONIATRIA Y FONOAUDIOLOGÍA] as FoniatriaYFonoaudiologia,
            Todo.FISIOTERAPIA as fisioterapia,
            Todo.[TERAPIA RESPIRATORIA] as terapiaRespiratoria,
            Todo.[TERAPIA OCUPACIONAL] as terapiaOcupacional,
            Todo.[AUXILIAR DE ENFERMERÍA] as auxiliarEnfermeria,
            Todo.[CLASIFICACIÓN DE LA HERIDA] as clasificacionHerida,
            Todo.[DIMENSIÓN DE LA HERIDA] as dimensionHerida,
            Todo.[PROFUNDIDAD/TEJIDOS AFECTADOS] as profundidadTejidosAfectados,
            Todo.COMORBILIDAD as comorbilidad,
            Todo.[ESTADIO DE LA HERIDA] as estadioDeLaHerida,
            Todo.[INFECCIÓN] as infeccion,
            Todo.[TIEMPO DE EVOLUCIÓN EN TRATAMIENTO CON CLÍNICA DE HERIDAS] as tiempoDeEvolucionEnTratamientoConClinicaDeHeridas,
            Todo.[EVOLUCIÓN SOPORTADA EN VISITA MÉDICA O REGISTRO FOTOGRAFICO] as evolucionSoportadaEnVisitaMedicaORegistroFotografico,
            Todo.[NIVEL ALBUMINA SÉRICA] as nivelAlbuminaSerica,
            Todo.[FECHA DE REPORTE DE ALBUMINA] as fechaReporteAlbumina,
            Todo.[TIPO DE SOPORTE DE OXÍGENO] as tipoSoporteOxigeno,
            Todo.[CONSUMO DE OXÍGENO EN LITROS/MINUTO] as consumoOxigenoEnLitrosPorMinuto,
            Todo.[HORAS DE ADMINISTRACIÓN DE OXÍGENO AL DÍA] as horasAdministracionOxigenoAlDia,
            Todo.[FECHAS DE INICIO DE SOPORTE DE OXÍGENO] as fechasInicioDeSoporteDeOxigeno,
            Todo.[EQUIPO PARA PRESIÓN POSITIVA] as equipoParaPresionPositiva,
            Todo.[TIEMPO REQUERIDO DE TRATAMIENTO] as tiempoRequeridoDeTratamiento, -- revisar
            Todo.[FECHA INICIO VENTILACIÓN MÉCANICA CRÓNICA] as fechaInicioVentilacionMecanicaCronica, -- revisar
            Todo.[MODO DE VENTILACIÓN MÉCANICA] as modoVentilacionMecanica,
            Todo.[DESCRIPCION MODALIDAD VENTILATORIA] as descripcionOtroModoDeVentilacionMecanica,
            Todo.OBSERVACIONES as observaciones,
            Todo.[FECHA DE CONTROL MÉDICO] as fechaControlMedico,
            Todo.HTA,
            Todo.[FECHA DE DIÁGNOSTICO HTA] as fechaDiagnosticoHTA,
            Todo.[MEDICAMENTO 1  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL] as medicamento1FormuladoParaManejoHTAActual,
            Todo.[MEDICAMENTO 2  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL] as medicamento2FormuladoParaManejoHTAActual,
            Todo.[MEDICAMENTO 3  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL] as medicamento3FormuladoParaManejoHTAActual,
            Todo.[RIESGO DE LA HTA AL INGRESO] as riesgoDeLAHTAAlIngreso,
            Todo.DM,
            Todo.[TIPO DE DIABETES] as tipoDiabetes,
            Todo.[FECHA DE DIAGNÓSTICO DM] as fechaDiagnosticoDM,
            Todo.[MEDICAMENTO 1 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM] as medicamento1QueEstaFormuladoParaManejoDMActualM,
            Todo.[MEDICAMENTO 2 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM] as medicamento2QueEstaFormuladoParaManejoDMActualM,
            Todo.[MEDICAMENTO 3 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM] as medicamento3QueEstaFormuladoParaManejoDMActualM,
            Todo.[TIPO DE INSULINA ADMINISTRADA AL INGRESO DEL PROGRAMA] as tipoInsulinaAdministradaIngresoDelPrograma,
            Todo.[TIPO DE INSULINA ADMINISTRADA DURANTE EL CONTROL] as tipoInsulinaAdministradaDuranteElControl,
            Todo.[RIESGO DE LA DM AL INGRESO] as riesgoDeLaDMIngreso,
            Todo.ERC,
            Todo.[FECHA DE DIAGNÓSTICO ERC] as fechaDiagnosticoERC,
            Todo.[TFG INGRESO] as TFGIngreso,
            Todo.[FECHA TFG INGRESO] as fechaTFGIngreso,
            Todo.[TFG ACTUAL] as TFGActual,
            Todo.[FECHA TFG ACTUAL] as fechaTFGActual,
            Todo.[MICROALBUMINURIA AL INGRESO DEL PROGRAMA] as microAlbuminuriaIngresoDelPrograma,
            Todo.[FECHA MICROALBUMINURIA AL INGRESO DEL PROGRAMA] as fechaMicroAlbuminuriaIngresoPrograma,
            Todo.[ESTADIO ACTUAL DE LA PATOLOGÌA] as estadioActualPatologia,
            Todo.[CREATININA SUERO] as creatininaSuerto,
            Todo.[FECHA DE CREATININA] as fechaCreatinina,
            Todo.GLICEMIA as glicemia,
            Todo.[FECHA DE TOMA DE GLICEMIA] as fechaTomaGlicemia,
            Todo.[HEMOGLOBINA GLICOSILADA] as hemoglobinaGlicosilada,
            Todo.[FECHA DE TOMA DE HEMOGLOBINA GLICOSILADA] as fechaTomaHemoglobinaGlicosilada,
            Todo.[COLESTEROL TOTAL] as colesterolTotal,
            Todo.[FECHA DE TOMA DE COLESTEROL TOTAL] as fechaTomaColesterolTotal,
            Todo.[COLESTEROL HDL] as colesterolHDL,
            Todo.[FECHA DE TOMA DE COLESTEROL HDL] as fechaTomaColesterolHDL,
            Todo.[COLESTEROL LDL] as colesterolLDL,
            Todo.[FECHA DE TOMA DE COLESTEROL LDL] as fechaTomaColesterolLDL,
            Todo.TRIGLICERIDOS as trigliceridos,
            Todo.[FECHA DE TOMA DE TRIGLICERIDOS] as fechaTomaTrigliceridos,
            Todo.[MICRO ALBUMINURIA] as microAlbuminuria,
            Todo.[FECHA DE TOMA DE MICRO ALBUMINURIA] as fechaTomaMicroAlbuminuria,
            Todo.[RELACIÓN MICROALBUMINURIA/CREATINURIA] as relacionMicroAlbuminuriaCreatinuria,
            Todo.[FECHA DE RELACIÓN MICROALBUMINURIA/CREATINURIA] as fechaRelacionMicroAlbuminuriaCreatinuria
        FROM (
            SELECT
                CONCAT(Enc.idEncounter, '-',
                        Enc.idUserPatient, '-',
                        Enc.idOffice, '-',
                        Enc.dateStart, '-',
                        EncR.idPrincipalContractee, '-',
                        EncHc.idHCRecord, '-',
                        EncR.idPrincipalContract, '-',
                        EHRconfAct.codeActivity
                        ) AS idMasiva,
                -- ROW_NUMBER() OVER(ORDER BY Enc.idEncounter, Pat.idUser, Eve.idEHREvent) AS id_Masiva_NEPS, -- RowNum,
                Enc.idEncounter,
                Pat.idUser,
                EncR.idPrincipalContractee as 	idNombreAseguradora,
                EncR.idPrincipalContract as idContratoPrincipal,
                -- TheDate,

                CONCAT(EHREvCust.idEvent, '-',
                Eve.idEncounter, '-',
                Enc.idUserPatient, '-',
                EHREvCust.idConfigActivity, '-',
                EHREvCust.idElement) AS idEsquemaConfigurable,

                CONCAT(EventMSC.idEHREvent, '-',
                Enc.idEncounter, '-',
                Enc.idUserPatient, '-',
                EventMSC.idScale, '-',
                EventMSC.idQuestion, '-',
                EventMSC.idAnswer, '-',
                EventMS.idEvaluation, '-',
                ConfgSV.idRecord) AS idConsultaMedica,


                Office.idOffice,
                Office.legalCode AS [codigoHabilitacion],
                Eve.idEHREvent,

                CONCAT(EHRCM.idMeasurement,'-',
                EHRPM.idUserPatient) AS idMedicionSignoVital,

                Diag.idDiagnostic,

                CONCAT(EventICUMM.idEHREvent, '-',
                Enc.idUserPatient, '-',
                Eve.idEncounter, '-',
                EventICUMM.idMonitoring, '-',
                EventICUMM.idMedition) AS idMedicionMonitoria,


                Enc.identifier AS [INGRESO], -- Campo OK sin modificaciones
                Ucom.documentNumber AS [NIT IPS],
                RIGHT(Office.legalCode, 1) AS [codigoSucursal], -- Luis Santamaria 30/08/2023 Ajustado.
                
                -- Req. FECHA DE INGRESO DEL USUARIO A LA IPS PAD: Se debe traer el dato del campo FECHA del Esquema dinámico 375 del último registro del esquema de HC “INGRESO A PACIENTE NEPS”. (DUDA)
                -- (version anterior) Enc.dateStart AS
                (SELECT TOP 1 CONVERT(DATE,EHRAct.valueText) FROM dbo.EHREventCustomActivities AS EHRAct WITH(NOLOCK)
                    INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EHRAct.idEvent = EV.idEHREvent
                        WHERE EV.idPatient = Enc.idUserPatient
                            AND EHRAct.idConfigActivity = 375
                            AND EHRAct.idElement = 1 -- ?
                        ORDER BY EV.idEHREvent DESC) AS [FECHA DE INGRESO DEL USUARIO A LA IPS PAD],
            
                -- [TALLA],
                /*  Se debe traer el dato de los signos vitales del último registro de HC de Historia Clínica Medicina General
                    o Historia Clínica Cuidados Paliativos Campo Talla Sin decimales, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1 CONVERT(INT, EHRPatM.recordedValue) -- sin decimales
                        FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
                            INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
                            INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHRPatM.idEHREvent = Eve.idEHREvent
                        WHERE EHRPatM.idUserPatient = Pat.idUser --idPatient
                            AND EHRCM.name LIKE '%Talla%'
                            AND (Eve.idAction = 1013
                                    OR Eve.idAction = 1004
                                    OR Eve.idAction = 1023)
                        ORDER BY EHRPatM.recordedDate DESC) AS TALLA, -- ajuste 30/08/2023: Antes EHRPatM.idRecord 
            
                -- [PESO]
                /* Se debe traer el dato de los signos vitales del último registro de HC de Historia Clínica Medicina General
                    o Historia Clínica Cuidados Paliativos Campo Peso Sin decimales, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1 CONVERT(INT, EHRPatM.recordedValue) -- sin decimales
                        FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
                            INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
                        WHERE EHRPatM.idUserPatient = Pat.idUser -- idPatient
                            AND EHRCM.name LIKE '%Peso%'
                            AND (Eve.idAction = 1013
                                    OR Eve.idAction = 1004
                                    OR Eve.idAction = 1023)
                        ORDER BY EHRPatM.recordedDate DESC) AS PESO,

                -- [TENSIÓN ARTERIAL SISTÓLICA]
                /* Se debe traer el dato de los signos vitales del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos Campo P.A. Sistólica Sin decimales, con 
                    respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1 CONVERT(INT, EHRPatM.recordedValue) -- sin decimales
                        FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
                            INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
                        WHERE EHRPatM.idUserPatient = Pat.idUser -- idPatient
                            AND EHRCM.name LIKE '%P.A.%Sist_lica%'
                            AND (Eve.idAction = 1013
                                    OR Eve.idAction = 1004
                                    OR Eve.idAction = 1023)
                        ORDER BY EHRPatM.recordedDate DESC) AS [TENSIÓN ARTERIAL SISTÓLICA],

                -- [TENSIÓN ARTERIAL DIASTÓLICA]
                /* Se debe traer el dato de los signos vitales del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos Campo P.A. Diastólica Sin decimales, con 
                    respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1 CONVERT(INT, EHRPatM.recordedValue) -- sin decimales
                        FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
                            INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
                        WHERE EHRPatM.idUserPatient = Pat.idUser -- idPatient
                            AND EHRCM.name LIKE '%P.A.%Diast_lica%'
                            AND (Eve.idAction = 1013
                                    OR Eve.idAction = 1004
                                    OR Eve.idAction = 1023)
                        ORDER BY EHRPatM.recordedDate DESC) AS [TENSIÓN ARTERIAL DIASTÓLICA],

                -- [CIRCUNFERENCIA ABDOMINAL],
                /* Se debe traer el dato de los signos vitales del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos Campo Circunferencia Abdominal Sin 
                    decimales, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1 CONVERT(INT, EHRPatM.recordedValue) -- sin decimales
                        FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
                            INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
                        WHERE EHRPatM.idUserPatient = Pat.idUser -- idPatient
                            AND EHRCM.name LIKE '%Circunferencia%Abdominal%'
                            AND (Eve.idAction = 1013
                                    OR Eve.idAction = 1004
                                    OR Eve.idAction = 1023)
                        ORDER BY EHRPatM.recordedDate DESC) AS [CIRCUNFERENCIA ABDOMINAL],

                -- [ASPECTO GENERAL],
                    /* dato del esquema dinámico 301 “Aspecto General” del último registro 
                    de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos Campo Lista desplegable
                    SIN NOMBRE, con respecto a la fecha fin del periodo consultado */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 301
                        AND EHREvCust.idElement = 1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [ASPECTO_GENERAL],

                -- [INTEGRIDAD DE LA PIEL],
                    /* dato del esquema dinámico 302 “Integridad de la Piel” del último 
                    registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos Campo Lista 
                    desplegable SIN NOMBRE, con respecto a la fecha fin del periodo consultado */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 302
                        AND EHREvCust.idElement = 1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [INTEGRIDAD DE LA PIEL],

                -- [RED DE APOYO],
                    /* (DUDA)  dato del esquema dinámico 292 “Red de apoyo” del último registro de HC de 
                    Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos Campo Lista desplegable SIN 
                    NOMBRE, con respecto a la fecha fin del periodo consultado */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'RedApo') -- @idAct
                        AND EHREvCust.idElement = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'RedApoEle') -- @idActEl
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [RED DE APOYO],

                -- [SOPORTE DE CUIDADOR],
                    /* dato del esquema dinámico 293 “Soporte Cuidador:” del último 
                    registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos Campo Lista 
                    desplegable SIN NOMBRE, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'SopCui') -- @idAct
                        AND EHREvCust.idElement = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'SopCuiEle') -- @idActEl
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [SOPORTE DE CUIDADOR],

                -- [SITUACIÓN ACTUAL DE DISCAPACIDAD],
                    /* dato de la escala 11 “Escala de Barthel” del último 
                    registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos Campo Valoración 
                    Nombre, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1 
                        EHRconfS.name
                    FROM EHREventMedicalScales AS EHREVMS WITH(NOLOCK)
                        INNER JOIN EHRConfScaleValorations AS EHRconfS WITH(NOLOCK) ON EHREVMS.idScale = EHRconfS.idScale
                            AND EHRconfS.idRecord = EHREVMS.idEvaluation
                        INNER JOIN EHREvents AS EV	WITH(NOLOCK) ON EHREVMS.idEHREvent = EV.idEHREvent
                    WHERE EHREVMS.idScale = 11
                        AND EV.idEncounter = Enc.idEncounter
                        ORDER BY EV.actionRecordedDate DESC) AS [SITUACIÓN ACTUAL DE DISCAPACIDAD],
                

                -- [ALIMENTACIÓN],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica 
                    Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC ) AS [ALIMENTACIÓN],

                -- [ACTIVIDADES EN BAÑO],
                /*  Campo Ok, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 2
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [ACTIVIDADES EN BAÑO],

                -- [VESTIRSE],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica 
                    Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 3
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [VESTIRSE],

                -- [ASEO PERSONAL],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica 
                    Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 4
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [ASEO PERSONAL],

                -- [DEPOSICIONES-CONTROL ANAL],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC 
                    de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del 
                    periodo consultado. */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 6
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [DEPOSICIONES-CONTROL ANAL],

                -- [MICCION-CONTROL VESICAL],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de 
                    HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del 
                    periodo consultado. */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 5
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [MICCION-CONTROL VESICAL],

                -- [MANEJO DE INODORO O RETRETE],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de 
                    HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del 
                    periodo consultado. */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 7
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [MANEJO DE INODORO O RETRETE],

                -- [TRASLADO SILLA-CAMA],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 8
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [TRASLADO SILLA-CAMA],

                -- [DEAMBULACIÓN TRASLADO],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 9
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [DEAMBULACIÓN TRASLADO],

                -- [SUBIR O BAJAR ESCALONES],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11 
                        AND EHRCQA.idQuestion = 10
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [SUBIR O BAJAR ESCALONES],

                -- [VALORACIÓN BARTHEL],
                /* Se debe traer la puntuación total de la escala de BARTHEL debe tener en cuenta 
                    únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. (Campo Nuevo). */
                (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 1
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC)
                    +
                    (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 2
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC)
            
                    +
                    (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 3
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC)
            
                    +
                    (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 4
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC)
                    +
                    (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 5
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC)
                    +
                    (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 6
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC)
                    +
                    (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 7
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC)
                    +
                    (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 8
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC)
                    +
                    (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 9
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC)
                    +
                    (SELECT TOP 1 CONVERT(INT,EHRCQA.value)
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 11
                    AND EHRCQA.idQuestion = 10
                    AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [VALORACIÓN BARTHEL],

                -- [INDICE KARNOFSKY],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado, Si no se encuentra dato 
                    registrado se debe dejar el campo por defecto en “Seleccione” */
                ISNULL((SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 12
                        AND EHRCQA.idQuestion = 1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [INDICE KARNOFSKY], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [CARACTERÍSTICAS DE LAS PATOLOGÍAS DE INGRESO DEL PACIENTE],
                /* Campo Ok, se debe tener en cuenta 
                    únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 293
                        AND EHREvCust.idElement = 3
                        AND Eve.idEncounter = Enc.idEncounter) AS [CARACTERÍSTICAS DE LAS PATOLOGÍAS DE INGRESO DEL PACIENTE],

                -- [FASE DE LA ENFERMEDAD DE INGRESO EN LA QUE PRESENTA EL USUARIO(A)],
                /*  Campo Ok, se debe tener en 
                    cuenta únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 293
                        AND EHREvCust.idElement = 4
                        AND Eve.idEncounter = Enc.idEncounter) AS [FASE DE LA ENFERMEDAD DE INGRESO EN LA QUE PRESENTA EL USUARIO(A)],

                -- [ACCIONES INSEGURAS],
                /* Se debe traer el campo ACCIONES INSEGURAS del esquema dinámico 374 
                    “INDICIOS DE ATENCIÓN INSEGURA”, se debe tener en cuenta únicamente del último registro de HC de Reporte 
                    de Indicios de Atención Insegura NEPS, con respecto a la fecha fin del periodo consultado. Si no se encuentra 
                    dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                    INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 374
                        AND EHREvCust.idElement = 1
                        AND Eve.idEncounter = Enc.idEncounter
                    ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [ACCIONES INSEGURAS], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [EVENTOS ADVERSOS PRESENTADOS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO],
                /* Se debe traer el campo 
                    EVENTOS ADVERSOS PRESENTADOS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO del
                    esquema dinámico 374 “INDICIOS DE ATENCIÓN INSEGURA”, se debe tener en cuenta únicamente del último 
                    registro de HC de Reporte de Indicios de Atención Insegura NEPS, con respecto a la fecha fin del periodo 
                    consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 374
                        AND EHREvCust.idElement = 2
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [EVENTOS ADVERSOS PRESENTADOS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [DESCRIPCIÓN DE OTROS EVENTOS ADVERSOS],
                /* Se debe traer el campo DESCRIPCIÓN DE OTROS 
                    EVENTOS ADVERSOS del esquema dinámico 374 “INDICIOS DE ATENCIÓN INSEGURA”, se debe tener en 
                    cuenta únicamente del último registro de HC de Reporte de Indicios de Atención Insegura NEPS, con respecto 
                    a la fecha fin del periodo consultado.  */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 374
                        AND EHREvCust.idElement = 3
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [DESCRIPCIÓN DE OTROS EVENTOS ADVERSOS],

                -- [NIT IPS DE OCURRENCIA DEL EVENTO ADVERSO]
                /* NIT de la entidad */
                Ucom.documentNumber AS [NIT IPS DE OCURRENCIA DEL EVENTO ADVERSO],

                -- [FECHA DE EVENTO ADVERSO],
                /* Se debe traer el campo FECHA DE EVENTO ADVERSOdel esquema dinámico 
                    374 “INDICIOS DE ATENCIÓN INSEGURA”, se debe tener en cuenta únicamente del último registro de HC de 
                    Reporte de Indicios de Atención Insegura NEPS, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 374
                        AND EHREvCust.idElement = 4
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) as [FECHA DE EVENTO ADVERSO],


                -- [GRADO DE LESIÓN DEL EVENTO ADVERSO],
                /* Se debe traer el campo GRADO DE LESIÓN DEL EVENTO 
                    ADVERSO del esquema dinámico 374 “INDICIOS DE ATENCIÓN INSEGURA”, se debe tener en cuenta 
                    únicamente del último registro de HC de Reporte de Indicios de Atención Insegura NEPS, con respecto a la fecha 
                    fin del periodo consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en 
                    “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText 
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 374
                        AND EHREvCust.idElement = 5
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [GRADO DE LESIÓN DEL EVENTO ADVERSO], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [PLAN DE INTERVENCIÓN- EVENTOS ADVERSOS],
                /* Se debe traer el campo PLAN DE INTERVENCIÓNEVENTOS ADVERSOS del esquema dinámico 374 “INDICIOS DE ATENCIÓN INSEGURA”, se debe tener en 
                    cuenta únicamente del último registro de HC de Reporte de Indicios de Atención Insegura NEPS, con respecto 
                    a la fecha fin del periodo consultado.  */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 374
                        AND EHREvCust.idElement = 6
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [PLAN DE INTERVENCIÓN- EVENTOS ADVERSOS],

                -- [PLAN DE INTERVENCIÓN- FALLAS DE CALIDAD],
                /*  Se debe traer el campo PLAN DE INTERVENCIÓN- FALLAS 
                    DE CALIDAD del esquema dinámico 374 “INDICIOS DE ATENCIÓN INSEGURA”, se debe tener en cuenta 
                    únicamente del último registro de HC de Reporte de Indicios de Atención Insegura NEPS, con respecto a la fecha 
                    fin del periodo consultado. */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 374
                        AND EHREvCust.idElement = 7
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [PLAN DE INTERVENCIÓN- FALLAS DE CALIDAD],

                -- [OBSERVACIÓN]
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 374
                        AND EHREvCust.idElement = 8
                        AND Eve.idEncounter = Enc.idEncounter
                        order by Eve.actionRecordedDate DESC) AS [OBSERVACIÓN],

                -- [FALLAS DE CALIDAD PRESENTADAS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO],
                /* Se debe traer el campo 
                    FALLAS DE CALIDAD PRESENTADAS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO del esquema dinámico 
                    374 “INDICIOS DE ATENCIÓN INSEGURA”, se debe tener en cuenta únicamente del último registro de HC de
                    Reporte de Indicios de Atención Insegura NEPS, con respecto a la fecha fin del periodo consultado. Si no se 
                    encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText 
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 374
                        AND EHREvCust.idElement = 9
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [FALLAS DE CALIDAD PRESENTADAS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [FECHA DE INGRESO AL PROGRAMA PAD]
                Enc.dateStart AS [FECHA DE INGRESO AL PROGRAMA PAD],

                -- [DIAGNÓSTICO PRINCIPAL CIE 10],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC de 
                    Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT  TOP 1
                        Diag.code
                    FROM EHREventMedicalDiagnostics AS EHREMDiag WITH(NOLOCK)
                        INNER JOIN EHREvents AS EV WITH(NOLOCK) ON EHREMDiag.idEHREvent = EV.idEHREvent
                        INNER JOIN diagnostics AS Diag WITH(NOLOCK) ON Diag.idDiagnostic = EHREMDiag.idDiagnostic
                    WHERE EV.idEncounter = Enc.idEncounter
                        AND EHREMDiag.isPrincipal = 1
                        ORDER BY Eve.actionRecordedDate DESC) AS [DIAGNÓSTICO PRINCIPAL CIE 10], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "último registro con respecto a la fecha fin del periodo consultado"


                -- [DIAGNÓSTICO NO.02 COMORBILIDAD PRINCIPAL CIE 10],
                /* Campo Ok, se debe tener en cuenta únicamente del 
                    último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con 
                    respecto a la fecha fin del periodo consultado. */
                (SELECT  TOP 1
                        Diag.code
                    FROM EHREventMedicalDiagnostics AS EHREMDiag WITH(NOLOCK)
                        INNER JOIN EHREvents AS EV WITH(NOLOCK) ON EHREMDiag.idEHREvent = EV.idEHREvent
                        INNER JOIN diagnostics AS Diag WITH(NOLOCK) ON Diag.idDiagnostic = EHREMDiag.idDiagnostic
                    WHERE EV.idEncounter = Enc.idEncounter
                        AND EHREMDiag.isPrincipal = 0
                        ORDER BY Eve.actionRecordedDate DESC) AS [DIAGNÓSTICO NO.02 COMORBILIDAD PRINCIPAL CIE 10], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "último registro con respecto a la fecha fin del periodo consultado"

                -- [DIAGNÓSTICO NO.03 OTRAS COMORBILIDADES CIE 10],
                /* Campo Ok, se debe tener en cuenta únicamente del 
                    último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto 
                    a la fecha fin del periodo consultado. */
                (SELECT  TOP 1
                        Diag.code
                    FROM EHREventMedicalDiagnostics AS EHREMDiag WITH(NOLOCK)
                        INNER JOIN EHREvents AS EV WITH(NOLOCK) ON EHREMDiag.idEHREvent = EV.idEHREvent
                        INNER JOIN diagnostics AS Diag WITH(NOLOCK) ON Diag.idDiagnostic = EHREMDiag.idDiagnostic
                    WHERE EV.idEncounter = Enc.idEncounter
                        AND EHREMDiag.isPrincipal = 0
                        ORDER BY Eve.actionRecordedDate DESC) AS [DIAGNÓSTICO NO.03 OTRAS COMORBILIDADES CIE 10], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "último registro con respecto a la fecha fin del periodo consultado"

                -- [CANTIDAD DE SERVICIOS SOLICITADOS]
                /* Campo OK. */
                1 AS [CANTIDAD DE SERVICIOS SOLICITADOS],

                -- [CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO],
                EHRconfAct.codeActivity AS [CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO],
            
                -- [MEDICINA GENERAL],
                /* Se debe traer la cantidad ordenada de la actividad ATENCION (VISITA) 
                    DOMICILIARIA, POR MEDICINA GENERAL, relacionadas en el plan domiciliario, con respecto a la fecha 
                    fin del periodo consultado. */
                (SELECT TOP 1 
                        quantityTODO
                    FROM encounterHCActivities AS EHREplan WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREplan.idEncounter = Eve.idEncounter
                    WHERE EHREplan.idProduct = 4984
                        AND EHREplan.isActive =1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY EHREplan.dateRegister DESC) AS [MEDICINA GENERAL],

                -- [MEDICINA ESPECIALIZADA]
                (SELECT TOP 1 
                        EveA.value 
                    FROM (SELECT 
                            ROW_NUMBER() OVER(PARTITION BY EHREvCust.idMedition, Eve.idEncounter ORDER BY EHREvCust.idEHREvent DESC) rnum,
                            EHREvCust.value, 
                            Eve.idEncounter, 
                            EHREvCust.idMedition
                            FROM EHREventICUMonitoringMeditions AS EHREvCust WITH(NOLOCK)
                                INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEHREvent = Eve.idEHREvent
                                INNER JOIN EHRConfUCIMonitoringMeditions AS EHREvCUsM WITH(NOLOCK) ON EHREvCust.idMonitoring = EHREvCUsM.idMonitoring
                                    AND EHREvCUsM.idMedition = EHREvCust.idMedition
                            WHERE EHREvCust.idMonitoring IN (1036,1037)
                                AND EHREvCUsM.isNumeric = 1) AS EveA 
                    WHERE EveA.idEncounter = Enc.idEncounter
                        AND EveA.rnum = 1
                        AND EveA.idMedition = 2650) AS [MEDICINA ESPECIALIZADA],
            

                -- [ESPECIALIDAD MÉDICA DE INTERVENCIÓN], -- PENDIENTE PARA LUIS SANTAMARIA
                /* Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1 
                        EveA.value 
                    FROM (SELECT 
                            ROW_NUMBER() OVER(PARTITION BY EHREvCust.idMedition, Eve.idEncounter ORDER BY EHREvCust.idEHREvent DESC) rnum,
                            EHREvCust.value, 
                            Eve.idEncounter, 
                            EHREvCust.idMedition
                            FROM EHREventICUMonitoringMeditions AS EHREvCust WITH(NOLOCK)
                                INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEHREvent = Eve.idEHREvent
                                INNER JOIN EHRConfUCIMonitoringMeditions AS EHREvCUsM WITH(NOLOCK) ON EHREvCust.idMonitoring = EHREvCUsM.idMonitoring
                                    AND EHREvCUsM.idMedition = EHREvCust.idMedition
                            WHERE EHREvCust.idMonitoring IN (1036,1037)
                                AND EHREvCUsM.isNumeric = 1) AS EveA 
                    WHERE EveA.idEncounter = Enc.idEncounter
                        AND EveA.rnum = 1
                        AND EveA.idMedition = 2666), 'Seleccione') AS [ESPECIALIDAD MÉDICA DE INTERVENCIÓN], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "último registro con respecto a la fecha fin del periodo consultado"

                -- [ENFERMERIA PROFESIONAL],
                /* Se debe traer la cantidad ordenada de la actividad ATENCION (VISITA) 
                    DOMICILIARIA, POR ENFERMERIA y que el Perfil sea Jefe Enfermeria DOMI relacionadas en el 
                    plan domiciliario, con respecto a la fecha fin del periodo consultado */
                (SELECT TOP 1 
                        quantityTODO		
                    FROM encounterHCActivities AS EHREplan WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREplan.idEncounter = Eve.idEncounter
                    WHERE EHREplan.idProduct = 4987
                        AND EHREplan.idRol =139
                        AND EHREplan.isActive =1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY EHREplan.dateRegister DESC) AS [ENFERMERIA PROFESIONAL],


                -- [NUTRICIÓN Y DIETÉTICA],
                /* Se debe traer la cantidad ordenada de la actividad ATENCION (VISITA) 
                    DOMICILIARIA, POR NUTRICION Y DIETETICA relacionadas en el plan domiciliario, con respecto a la 
                    fecha fin del periodo consultado. */
                (Select top 1 
                        Act.quantityTODO
                    FROM encounterHC as planus
                    INNER JOIN encounters AS INGR ON planus.idEncounter= INGR.idEncounter
                    INNER JOIN encounterHCActivities AS Act ON  planus.idEncounter = Act.idEncounter 
                    INNER JOIN EHRConfHCActivity AS CONF ON planus.idHCActivity =CONF.idHCActivity
                    WHERE planus.isActive=1
                    AND Act.idProduct=40496
                    AND INGR.idEncounter = Enc.idEncounter
                    --AND CONF.codeActivity= Enc.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
                    ORDER BY Act.dateRegister desc) AS [NUTRICIÓN Y DIETÉTICA],


                -- [PSICOLOGÍA],
                /* Se debe traer la cantidad ordenada de la actividad ATENCION (VISITA) DOMICILIARIA, 
                    POR PSICOLOGIA relacionadas en el plan domiciliario, con respecto a la fecha fin del periodo consultado */
                (SELECT TOP 1 
                        quantityTODO					
                    FROM encounterHCActivities AS EHREplan WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREplan.idEncounter = Eve.idEncounter
                    WHERE EHREplan.idProduct = 4989
                        AND EHREplan.isActive =1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY EHREplan.dateRegister DESC) AS [PSICOLOGÍA],

                -- [TRABAJO SOCIAL],
                /* Se debe traer la cantidad ordenada de la actividad ATENCION (VISITA) DOMICILIARIA, 
                    POR TRABAJO SOCIALrelacionadas en el plan domiciliario, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1 
                        quantityTODO
                    FROM encounterHCActivities AS EHREplan WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREplan.idEncounter = Eve.idEncounter
                    WHERE EHREplan.idProduct = 4990
                        AND EHREplan.isActive =1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY EHREplan.dateRegister DESC) AS [TRABAJO SOCIAL],


                -- [FONIATRIA Y FONOAUDIOLOGÍA],
                /* Se debe traer la cantidad ordenada de la actividad ATENCION (VISITA) 
                    DOMICILIARIA, POR FONIATRIA Y FONOAUDIOLOGIA relacionadas en el plan domiciliario, con 
                    respecto a la fecha fin del periodo consultado. */
                    (SELECT TOP 1 
                            quantityTODO
                        FROM encounterHCActivities AS EHREplan WITH(NOLOCK)
                            INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREplan.idEncounter = Eve.idEncounter
                        WHERE EHREplan.idProduct = 4987
                            AND EHREplan.isActive =1
                            AND Eve.idEncounter = Enc.idEncounter
                            ORDER BY Eve.actionRecordedDate DESC) AS [FONIATRIA Y FONOAUDIOLOGÍA],

                -- [FISIOTERAPIA],
                /* Se debe traer la cantidad ordenada de la actividad ATENCION (VISITA) DOMICILIARIA, 
                    POR FISIOTERAPIA relacionadas en el plan domiciliario, con respecto a la fecha fin del periodo consultado */
                (SELECT TOP 1 
                        quantityTODO
                    FROM encounterHCActivities AS EHREplan WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREplan.idEncounter = Eve.idEncounter
                    WHERE EHREplan.idProduct = 4992
                        AND EHREplan.isActive =1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FISIOTERAPIA],

                -- [TERAPIA RESPIRATORIA],
                /* Se debe traer la cantidad ordenada de la actividad ATENCION (VISITA) 
                    DOMICILIARIA, POR TERAPIA RESPIRATORIA relacionadas en el plan domiciliario, con respecto a la 
                    fecha fin del periodo consultado. */
                (SELECT TOP 1 
                        quantityTODO
                    FROM encounterHCActivities AS EHREplan WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREplan.idEncounter = Eve.idEncounter
                    WHERE EHREplan.idProduct = 4993
                        AND EHREplan.isActive =1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [TERAPIA RESPIRATORIA],

                -- [TERAPIA OCUPACIONAL],
                /* Se debe traer la cantidad ordenada de la actividad ATENCION (VISITA) 
                    DOMICILIARIA, POR TERAPIA OCUPACIONAL relacionadas en el plan domiciliario, con respecto a la 
                    fecha fin del periodo consultado. */
                (SELECT TOP 1 
                        quantityTODO
                    FROM encounterHCActivities AS EHREplan WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREplan.idEncounter = Eve.idEncounter
                    WHERE EHREplan.idProduct = 4994
                        AND EHREplan.isActive =1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [TERAPIA OCUPACIONAL],

                -- [AUXILIAR DE ENFERMERÍA], (DUDA) En la consulta no se alcanza a visualizar la sumatoria
                /* Se debe traer la sumatoria de la cantidad ordenada de las actividades ATENCION 
                    (VISITA) DOMICILIARIA, POR ENFERMERIA y CURACION DE LESION EN PIEL O TEJIDO 
                    CELULAR SUBCUTANEO SOD y que el perfil sea auxiliar de enfermería DOMI, relacionadas en 
                    el plan domiciliario, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1 
                        quantityTODO
                    FROM encounterHCActivities AS EHREplan WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREplan.idEncounter = Eve.idEncounter
                    WHERE EHREplan.idProduct = 4987
                        AND EHREplan.idRol =141
                        AND EHREplan.isActive =1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [AUXILIAR DE ENFERMERÍA],


                -- [CLASIFICACIÓN DE LA HERIDA],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de la 
                    escala 58 “ESCALA CLÍNICA DE HERIDAS”, con respecto a la fecha fin del periodo consultado. Si no se encuentra 
                    dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value)) 
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ClaHer') -- @idAct = 58
                        AND EHRCQA.idQuestion =  (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ClaHerEle') -- @idActEl = 1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [CLASIFICACIÓN DE LA HERIDA], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [DIMENSIÓN DE LA HERIDA],
                /* Campo Ok, se debe tener en cuenta 
                    únicamente del último registro de la escala 58 “ESCALA CLÍNICA DE HERIDAS”, con respecto a la fecha fin del 
                    periodo consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value)) 
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'DimHer') -- @idAct 
                        AND EHRCQA.idQuestion = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'DimHerEle') -- @idActEl
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [DIMENSIÓN DE LA HERIDA], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [PROFUNDIDAD/TEJIDOS AFECTADOS],
                /* Campo Ok, se debe tener en cuenta 
                    únicamente del último registro de la escala 58 “ESCALA CLÍNICA DE HERIDAS”, con respecto a la fecha fin del 
                    periodo consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                    ISNULL((SELECT TOP 1
                            EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value)) 
                        FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                            INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                                AND EHRCQA.idQuestion = EHREvMS.idQuestion
                                AND EHRCQA.idAnswer = EHREvMS.idAnswer
                            INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                        WHERE EHRCQA.idScale = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ProHer') -- @idAct
                            AND EHRCQA.idQuestion = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ProHerEle') -- @idActEl
                            AND Eve.idEncounter = Enc.idEncounter
                            ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [PROFUNDIDAD/TEJIDOS AFECTADOS], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [COMORBILIDAD],
                /* Campo Ok, se debe tener en cuenta únicamente del último 
                    registro de la escala 58 “ESCALA CLÍNICA DE HERIDAS”, con respecto a la fecha fin del periodo consultado. Si no 
                    se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                            EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value)) 
                        FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                            INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                                AND EHRCQA.idQuestion = EHREvMS.idQuestion
                                AND EHRCQA.idAnswer = EHREvMS.idAnswer
                            INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                        WHERE EHRCQA.idScale = (SELECT  Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'Comor') -- @idAct
                            AND EHRCQA.idQuestion = (SELECT  Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ComorEle') -- @idActEl
                            AND Eve.idEncounter = Enc.idEncounter
                            ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [COMORBILIDAD], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.


                -- [ESTADIO DE LA HERIDA],
                /*  Campo Ok, se debe tener en cuenta únicamente del último 
                    registro de la escala 58 “ESCALA CLÍNICA DE HERIDAS”, con respecto a la fecha fin del periodo consultado. Si no 
                    se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'EstHer') -- @idAct
                        AND EHRCQA.idQuestion = (SELECT  Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'EstHerEle') -- @idActEl
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [ESTADIO DE LA HERIDA], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [INFECCIÓN],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de la escala 58 “ESCALA 
                    CLÍNICA DE HERIDAS”, con respecto a la fecha fin del periodo consultado. Si no se encuentra dato registrado se 
                    debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'Infecc') -- @idAct
                        AND EHRCQA.idQuestion = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'InfeccEle') -- @idActEl
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [INFECCIÓN], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado. 

                -- [TIEMPO DE EVOLUCIÓN EN TRATAMIENTO CON CLÍNICA DE HERIDAS],
                /* Se debe traer el campo Tiempo de evolución en tratamiento con Clínica de Heridas de la escala 58 “ESCALA CLÍNICA DE HERIDAS”,
                    se debe tener en cuenta únicamente del último registro de la escala 58 “ESCALA CLÍNICA DE HERIDAS”, con 
                    respecto a la fecha fin del periodo consultado. Si no se encuentra dato registrado se debe dejar el campo por 
                    defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = 58
                        AND EHRCQA.idQuestion = 7
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [TIEMPO DE EVOLUCIÓN EN TRATAMIENTO CON CLÍNICA DE HERIDAS], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [EVOLUCIÓN SOPORTADA EN VISITA MÉDICA O REGISTRO FOTOGRAFICO],
                /* Campo Ok, se debe tener en 
                    cuenta únicamente del último registro de la escala 58 “ESCALA CLÍNICA DE HERIDAS”, con respecto a la fecha 
                    fin del periodo consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en 
                    “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
                    FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
                        INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
                            AND EHRCQA.idQuestion = EHREvMS.idQuestion
                            AND EHRCQA.idAnswer = EHREvMS.idAnswer
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
                    WHERE EHRCQA.idScale = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'EvSopo') -- @idAct
                        AND EHRCQA.idQuestion = (SELECT Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'EvSopoEle') -- @idActEl
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [EVOLUCIÓN SOPORTADA EN VISITA MÉDICA O REGISTRO FOTOGRAFICO], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [NIVEL ALBUMINA SÉRICA],
                /*  Campo Ok, se debe tener en cuenta únicamente del último registro de HC de 
                    Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 303
                        AND EHREvCust.idElement = 1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [NIVEL ALBUMINA SÉRICA],

                -- [FECHA DE REPORTE DE ALBUMINA],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de 
                    HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del 
                    periodo consultado. */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 303
                        AND EHREvCust.idElement = 2
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE REPORTE DE ALBUMINA],

                -- [TIPO DE SOPORTE DE OXÍGENO],
                /* Campo Ok, se debe tener en cuenta únicamente del último registro de HC 
                    de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del 
                    periodo consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText 
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 290
                        AND EHREvCust.idElement = 13
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [TIPO DE SOPORTE DE OXÍGENO], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [CONSUMO DE OXÍGENO EN LITROS/MINUTO],
                /* Campo Ok, se debe tener en cuenta únicamente del último 
                    registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la 
                    fecha fin del periodo consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en 
                    “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText 
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 290
                        AND EHREvCust.idElement = 14
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [CONSUMO DE OXÍGENO EN LITROS/MINUTO], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [HORAS DE ADMINISTRACIÓN DE OXÍGENO AL DÍA],
                /* Campo Ok, se debe tener en cuenta únicamente del 
                    último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto 
                    a la fecha fin del periodo consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en 
                    “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText 
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 290
                        AND EHREvCust.idElement = 15
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [HORAS DE ADMINISTRACIÓN DE OXÍGENO AL DÍA], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.

                -- [FECHAS DE INICIO DE SOPORTE DE OXÍGENO],
                /* Campo Ok, se debe tener en cuenta únicamente del último 
                    registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la 
                    fecha fin del periodo consultado. */
                (SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 290
                        AND EHREvCust.idElement = 16
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHAS DE INICIO DE SOPORTE DE OXÍGENO],

                -- [EQUIPO PARA PRESIÓN POSITIVA],
                /*  Campo Ok, se debe tener en cuenta únicamente del último registro de HC 
                    de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del 
                    periodo consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText 
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 290
                        AND EHREvCust.idElement = 17
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [EQUIPO PARA PRESIÓN POSITIVA], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado. 
                    

                -- [TIEMPO REQUERIDO DE TRATAMIENTO] -- Manar Luis Santamaria 24/08/2023: Métrica no implementada en Script. Implementada por Luis Santamaria.
                /* Se debe traer el campo Tiempo requerido de tratamiento
                    del esquema dinámico 290 “OTROS SERVICIOS REQUERIDOS O PRESENTES”, se debe tener en cuenta 
                    únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. Si no se encuentra dato registrado se debe dejar 
                    el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 290
                        AND EHREvCust.idElement = 1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [TIEMPO REQUERIDO DE TRATAMIENTO],
            

                -- [FECHA INICIO VENTILACIÓN MÉCANICA CRÓNICA]
                /* Campo Ok, Si no se encuentra dato registrado se debe  dejar el campo por defecto en “Seleccione”. */
                'Seleccione' AS [FECHA INICIO VENTILACIÓN MÉCANICA CRÓNICA],

                -- [MODO DE VENTILACIÓN MÉCANICA],
                /* Campo Ok, Si no se encuentra dato registrado se debe dejar el campo  por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                    EHREvCust.valueText
                FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                    INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                WHERE EHREvCust.idConfigActivity = 290
                    AND EHREvCust.idElement = 12
                    AND Eve.idEncounter = Enc.idEncounter
                    order by Eve.actionRecordedDate DESC), 'Seleccione') AS [MODO DE VENTILACIÓN MÉCANICA], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado. 


                -- [DESCRIPCIÓN OTRO MODO DE VENTILACIÓN MÉCANICA],
                /* Campo Ok, Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                            INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 290
                        AND EHREvCust.idElement = 18
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [DESCRIPCIÓN OTRO MODO DE VENTILACIÓN MÉCANICA],

                'Seleccione' AS [MODO VENTILATORIO],
                'Seleccione' AS [MODALIDAD VENTILATORIA],
                'Seleccione' AS [DESCRIPCION MODALIDAD VENTILATORIA],
                'Seleccione' AS [PEEP],
                'Seleccione' AS [PEEP ALTO],
                'Seleccione' AS [PEEP BAJO],
                'Seleccione' AS [TIEMPO BAJO],
                'Seleccione' AS [TIEMPO ALTO],
                'Seleccione' AS [FRECUENCIA RESPIRATORIA TOTAL],
                'Seleccione' AS [FRECUENCIA RESPIRATORIA PROGRAMADA],
                'Seleccione' AS [FIO2],
                'Seleccione' AS [TIPO DE VENTILADOR EN USO POR EL PACIENTE],
                'Seleccione' AS [DESCRIPCIÓN OTRO TIPO DE VENTILADOR EN USO POR EL PACIENTE],



                -- [OBSERVACIONES],
                /* Campo Ok, Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        EHREvCust.valueText 
                    FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                    WHERE EHREvCust.idConfigActivity = 304
                        AND EHREvCust.idElement = 1
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [OBSERVACIONES], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado. 

                -- [FECHA DE CONTROL MÉDICO],
                /* Se debe traer la fecha de guardado del último registro de Historia Clínica Medicina 
                    General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1 EV.actionRecordedDate 
                    FROM EHREvents AS EV WITH(NOLOCK) 
                    WHERE (EV.idAction = 1013
                    OR EV.idAction= 1004
                    OR EV.idAction= 1023)
                    AND Ev.idEncounter = Enc.idEncounter
                    ORDER BY EV.idEHREvent DESC) AS [FECHA DE CONTROL MÉDICO],


                -- [HTA],
                /* Se debe traer el campo HTA del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta 
                    únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2780
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [HTA],

                -- [FECHA DE DIÁGNOSTICO HTA],
                /* Se debe traer el campo FECHA DE DIÁGNOSTICO HTA del esquema UCI 
                    1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2781
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS  [FECHA DE DIÁGNOSTICO HTA],

                -- [MEDICAMENTO 1  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL],
                /* Se debe traer el campo 
                    MEDICAMENTO 1 QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL del esquema UCI 
                    1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2782
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [MEDICAMENTO 1  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado. 


                -- [MEDICAMENTO 2  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL],
                /* Se debe traer el campo 
                    MEDICAMENTO 2 QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL del esquema UCI 
                    1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2783
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [MEDICAMENTO 2  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado. 

                -- [MEDICAMENTO 3  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL],
                /* Se debe traer el campo 
                    MEDICAMENTO 3 QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL del esquema UCI 
                    1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2784
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [MEDICAMENTO 3  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado. 


                -- [RIESGO DE LA HTA AL INGRESO],
                /* Se debe traer el campo RIESGO DE LA HTA AL INGRESO del esquema 
                    UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de 
                    Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2787
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [RIESGO DE LA HTA AL INGRESO], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado. 

                -- [DM],
                /* Se debe traer el campo DM del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta 
                    únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2792
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [DM],


                -- [TIPO DE DIABETES],
                /* Se debe traer el campo TIPO DE DIABETES del esquema UCI 1044 “INFORMACIÓN 
                    ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina General
                    o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. Si no se encuentra 
                    dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2793
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [TIPO DE DIABETES], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.


                -- [FECHA DE DIAGNÓSTICO DM],
                /* Se debe traer el campo FECHA DE DIAGNÓSTICO DM del esquema UCI 
                    1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado.  */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2794
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE DIAGNÓSTICO DM],


                -- [MEDICAMENTO 1 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM],
                /* Se debe traer el campo 
                    MEDICAMENTO 1 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUAL del esquema 
                    UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de 
                    Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione” */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2795
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [MEDICAMENTO 1 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.


                -- [MEDICAMENTO 2 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM],
                /*  Se debe traer el campo 
                    MEDICAMENTO 2 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUAL del esquema 
                    UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de 
                    Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2796
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [MEDICAMENTO 2 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.


                -- [MEDICAMENTO 3 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM],
                /* Se debe traer el campo 
                    MEDICAMENTO 3 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUAL del esquema 
                    UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de 
                    Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2797
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [MEDICAMENTO 3 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.


                -- [TIPO DE INSULINA ADMINISTRADA AL INGRESO DEL PROGRAMA],
                /* Se debe traer el campo TIPO DE 
                    INSULINA ADMINISTRADA AL INGRESO DEL PROGRAMA del esquema UCI 1044 “INFORMACIÓN 
                    ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina General
                    o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. Si no se encuentra 
                    dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2798
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [TIPO DE INSULINA ADMINISTRADA AL INGRESO DEL PROGRAMA], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.


                -- [TIPO DE INSULINA ADMINISTRADA DURANTE EL CONTROL],
                /*  Se debe traer el campo TIPO DE INSULINA 
                    ADMINISTRADA DURANTE EL CONTROL del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe 
                    tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica 
                    Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. Si no se encuentra dato registrado se 
                    debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2799
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [TIPO DE INSULINA ADMINISTRADA DURANTE EL CONTROL], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.


                -- [RIESGO DE LA DM AL INGRESO],
                /* Se debe traer el campo RIESGO DE LA DM AL INGRESO del esquema 
                    UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de 
                    Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. Si no se encuentra dato registrado se debe dejar el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2801
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [RIESGO DE LA DM AL INGRESO], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.


                -- [ERC],
                /* Se debe traer el campo ERC del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta 
                    únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. Si no se encuentra dato registrado se debe dejar 
                    el campo por defecto en “Seleccione”. */
                ISNULL((SELECT TOP 1
                        value 
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2804
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC), 'Seleccione') AS [ERC], -- Manar Luis Santamaria 24/08/2023: Se aplicó requerimiento "Seleccione" en caso de valor no encontrado.


                -- [FECHA DE DIAGNÓSTICO ERC],
                /* Se debe traer el campo FECHA DE DIAGNÓSTICO ERC del esquema UCI 
                    1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2805
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE DIAGNÓSTICO ERC],


                -- [TFG INGRESO],
                /*  Se debe traer el campo Cockcroft-Gault (ml/min/1.73 m²) Ingreso del esquema UCI 
                    1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del primer registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos que contenga esta información, con respecto a 
                    la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2813
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [TFG INGRESO],


                -- [FECHA TFG INGRESO],
                /* Se debe traer el campo FECHA TFG INGRESO del esquema UCI 1044 
                    “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del primer registro de HC de Historia Clínica 
                    Medicina General o Historia Clínica Cuidados Paliativos que contenga esta información, con respecto a la fecha 
                    fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2814
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA TFG INGRESO],


                -- [TFG ACTUAL],
                /* Se debe traer el campo Cockcroft-Gault (ml/min/1.73 m²) del esquema UCI 1044 
                    “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica 
                    Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2824
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [TFG ACTUAL],


                -- [FECHA TFG ACTUAL],
                /*  Se debe traer el campo FECHA TFG ACTUAL del esquema UCI 1044 “INFORMACIÓN 
                    ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina 
                    General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2826
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA TFG ACTUAL],


                -- [MICROALBUMINURIA AL INGRESO DEL PROGRAMA],
                /* Se debe traer el campo MICROALBUMINURIA AL 
                    INGRESO DEL PROGRAMA del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta 
                    únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2828
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [MICROALBUMINURIA AL INGRESO DEL PROGRAMA],


                -- [FECHA MICROALBUMINURIA AL INGRESO DEL PROGRAMA],
                /* Se debe traer el campo FECHA 
                    MICROALBUMINURIA AL INGRESO DEL PROGRAMA del esquema UCI 1044 “INFORMACIÓN 
                    ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina 
                    General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2829
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA MICROALBUMINURIA AL INGRESO DEL PROGRAMA],


                -- [ESTADIO ACTUAL DE LA PATOLOGÌA],
                /* Se debe traer el campo ESTADIO ACTUAL DE LA PATOLOGÍA
                    del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro 
                    de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin 
                    del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2830
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [ESTADIO ACTUAL DE LA PATOLOGÌA],


                -- [CREATININA SUERO],
                /* Se debe traer el campo Creatinina (mg\dL) Actual del esquema UCI 1044 
                    “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica 
                    Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2819
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [CREATININA SUERO],


                -- [FECHA DE CREATININA],
                /* Se debe traer el campo Fecha toma Creatinina Actual del esquema UCI 1044 
                    “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica 
                    Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2825
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE CREATININA],

                -- [GLICEMIA],
                /* Se debe traer el campo GLICEMIA del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe 
                    tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica 
                    Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2830
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [GLICEMIA],

                -- [FECHA DE TOMA DE GLICEMIA],
                /* Se debe traer el campo FECHA DE TOMA DE GLICEMIA del esquema UCI 
                    1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */

                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2831
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE TOMA DE GLICEMIA],

                -- [HEMOGLOBINA GLICOSILADA],
                /* Se debe traer el campo HEMOGLOBINA GLICOSILADA del esquema UCI 
                    1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia 
                    Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo 
                    consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2815
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [HEMOGLOBINA GLICOSILADA],


                -- [FECHA DE TOMA DE HEMOGLOBINA GLICOSILADA],
                /* Se debe traer el campo FECHA DE TOMA DE 
                    HEMOGLOBINA GLICOSILADA del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en 
                    cuenta únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2816
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE TOMA DE HEMOGLOBINA GLICOSILADA],

                -- [COLESTEROL TOTAL],
                /* Se debe traer el campo COLESTEROL TOTAL del esquema UCI 1044 
                    “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica 
                    Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2833
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [COLESTEROL TOTAL],

                -- [FECHA DE TOMA DE COLESTEROL TOTAL],
                /* Se debe traer el campo FECHA DE TOMA DE COLESTEROL 
                    TOTAL del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último 
                    registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la 
                    fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2834
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE TOMA DE COLESTEROL TOTAL],


                -- [COLESTEROL HDL],
                /* Se debe traer el campo COLESTEROL HDL del esquema UCI 1044 “INFORMACIÓN 
                    ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina 
                    General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2835
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [COLESTEROL HDL],


                -- [FECHA DE TOMA DE COLESTEROL HDL],
                /* Se debe traer el campo FECHA DE TOMA DE COLESTEROL 
                    HDL del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último 
                    registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la 
                    fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2836
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE TOMA DE COLESTEROL HDL],


                -- [COLESTEROL LDL],
                /* Se debe traer el campo COLESTEROL LDL del esquema UCI 1044 “INFORMACIÓN 
                    ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina 
                    General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2837
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [COLESTEROL LDL],


                -- [FECHA DE TOMA DE COLESTEROL LDL],
                /* Se debe traer el campo FECHA DE TOMA DE COLESTEROL 
                    LDL del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último 
                    registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la 
                    fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2838
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE TOMA DE COLESTEROL LDL],


                -- [TRIGLICERIDOS],
                /* Se debe traer el campo TRIGLICERIDOS del esquema UCI 1044 “INFORMACIÓN 
                    ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina 
                    General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2839
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [TRIGLICERIDOS],



                -- [FECHA DE TOMA DE TRIGLICERIDOS],
                /* Se debe traer el campo FECHA DE TOMA DE TRIGLICERIDOS del 
                    esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC 
                    de Historia Clínica Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del 
                    periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2840
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE TOMA DE TRIGLICERIDOS],


                -- [MICRO ALBUMINURIA],
                /* Se debe traer el campo MICROALBUMINURIA del esquema UCI 1044 
                    “INFORMACIÓN ADICIONAL”, se debe tener en cuenta únicamente del último registro de HC de Historia Clínica 
                    Medicina General o Historia Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2841
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [MICRO ALBUMINURIA],


                -- [FECHA DE TOMA DE MICRO ALBUMINURIA],
                /*  Se debe traer el campo FECHA DE TOMA DE 
                    MICROALBUMINURIA del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe tener en cuenta 
                    únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica Cuidados 
                    Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2842
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE TOMA DE MICRO ALBUMINURIA],


                -- [RELACIÓN MICROALBUMINURIA/CREATINURIA],
                /* Se debe traer el campo RELACIÓN 
                    MICROALBUMINURIA/CREATINURIA del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, se debe 
                    tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina General o Historia Clínica 
                    Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2843
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [RELACIÓN MICROALBUMINURIA/CREATINURIA],


                -- [FECHA DE RELACIÓN MICROALBUMINURIA/CREATINURIA]
                /*  Se debe traer el campo FECHA DE 
                    RELACIÓN MICROALBUMINURIA/CREATINURIA del esquema UCI 1044 “INFORMACIÓN ADICIONAL”, 
                    se debe tener en cuenta únicamente del último registro de HC de Historia Clínica Medicina General o Historia 
                    Clínica Cuidados Paliativos, con respecto a la fecha fin del periodo consultado. */
                (SELECT TOP 1
                        value
                    FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
                        INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
                    WHERE EHREUCI.idMonitoring = '1044'
                        AND EHREUCI.idMedition = 2844
                        AND Eve.idEncounter = Enc.idEncounter
                        ORDER BY Eve.actionRecordedDate DESC) AS [FECHA DE RELACIÓN MICROALBUMINURIA/CREATINURIA]


            FROM encounters	AS Enc WITH(NOLOCK)
                INNER JOIN users AS Pat WITH(NOLOCK) ON Enc.idUserPatient = Pat.idUser
                INNER JOIN userConfTypeDocuments AS Doc WITH(NOLOCK) ON Pat.idDocumentType = Doc.idTypeDocument
                INNER JOIN companyOffices AS Office WITH(NOLOCK) ON Enc.idOffice = Office.idOffice
                INNER JOIN userPeople AS PatU WITH(NOLOCK) ON Pat.idUser = PatU.idUser
                INNER JOIN users AS Ucom WITH(NOLOCK) ON Office.idUserCompany = Ucom.idUser
                INNER JOIN generalPoliticalDivisions AS City WITH(NOLOCK) ON PatU.idHomePlacePoliticalDivision = City.idPoliticalDivision
                INNER JOIN generalPoliticalDivisions AS CityD WITH(NOLOCK) ON City.idParent = CityD.idPoliticalDivision
                INNER JOIN encounterHC AS EncHc WITH(NOLOCK) ON Enc.idEncounter = EncHc.idEncounter
                INNER JOIN ehrconfhcActivity AS EHRconfAct WITH(NOLOCK) ON EncHc.idHCActivity = EHRconfAct.idHCActivity AND EHRconfAct.idCompany = @idUserCompany
                INNER JOIN encounterRecords AS EncR WITH(NOLOCK) ON Enc.idEncounter = EncR.idEncounter

                -- id_Medical_Scales
                LEFT JOIN EHREvents	AS Eve WITH(NOLOCK) ON Enc.idEncounter = Eve.idEncounter
                LEFT JOIN EHREventMedicalScaleQuestions as EventMSC WITH(NOLOCK) ON Eve.idEHREvent = EventMSC.idEHREvent
                LEFT JOIN EHREventMedicalScales AS EventMS WITH(NOLOCK) ON EventMSC.idEHREvent = EventMS.idEHREvent
                LEFT JOIN EHRConfScaleValorations AS ConfgSV WITH(NOLOCK) ON EventMS.idScale = ConfgSV.idScale AND ConfgSV.idRecord = EventMS.idEvaluation
            
                -- id_Esquemas_Configurables 
                LEFT JOIN EHREventCustomActivities AS EHREvCust WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent

                -- For id_Medicion_Signo_Vital
                LEFT JOIN EHRPatientMeasurements AS EHRPM ON Eve.idEHREvent = EHRPM.idEHREvent
                LEFT JOIN EHRConfMeasurements AS EHRCM ON EHRCM.idMeasurement = EHRPM.idMeasurement

                -- id_Mediciones_Monitoria 
                LEFT JOIN EHREventICUMonitoringMeditions AS EventICUMM ON Eve.idEHREvent = EventICUMM.idEHREvent

                -- idDiagnostic
                LEFT JOIN EHREventMedicalDiagnostics AS EHREvMDiag ON Eve.idEHREvent = EHREvMDiag.idEHREvent
                LEFT JOIN (SELECT DISTINCT idDiagnostic FROM diagnostics) AS Diag ON EHREvMDiag.idDiagnostic = Diag.idDiagnostic

                WHERE Enc.idUserCompany = @idUserCompany
                    EncHc.dateStart >='{last_week}' AND EncHc.dateStart<'{now}'
                    -- AND EncHc.dateStart BETWEEN @dateStart AND (@dateEnd + '23:59:59')
                    AND Enc.idOffice IN (SELECT Value FROM dbo.FnSplit (@OfficeFilter))
                    AND EncR.idPrincipalContractee IN (SELECT Value FROM dbo.FnSplit (@idIns))
                    AND EncR.idPrincipalContract IN (SELECT Value FROM dbo.FnSplit (@idCont))) AS Todo
            
        """
    
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    print(df.columns)
    print(df.dtypes)
    print(df.isna().sum()) # conteo de nulos por campo
    print(df)

    # Si la consulta no es vacía, carga el dataframe a la tabla temporal en BI.
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
    # Se establece la ejecución del dag a las 9:10 am (hora servidor) todos los Jueves
    schedule_interval= None, # '10 9 * * 04', # cron expression
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_factMasiva = PythonOperator(task_id = "get_factMasiva",
                                                                python_callable = func_get_factMasiva,
                                                                #email_on_failure=False, 
                                                                # email='BI@clinicos.com.co',
                                                                dag=dag
                                                                )
    """
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_factMasiva = MsSqlOperator(task_id='Load_factMasiva',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDEsquemasConfigurables",
                                        # email_on_failure=True, 
                                        # email='BI@clinicos.com.co',
                                        dag=dag
                                       )
    """


    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_factMasiva >> task_end