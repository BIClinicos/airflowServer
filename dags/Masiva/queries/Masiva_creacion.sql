	DECLARE 
		@idUserCompany INT= 1,
		@dateStart DATETIME = {last_week},
		@dateEnd DATETIME = convert(date, getdate()),
		@OfficeFilter VARCHAR(MAX) = '1,17',--(SELECT STRING_AGG(idOffice,',') FROM companyOffices WHERE idUserCompany = 352666),
		@idIns VARCHAR(MAX) = '16,33,285991,20,266465,422816,289134,17,150579,358811,39,88813,4,24,22,25,150571,302708,26,289154,365849,266467,7,28,23,420,32,421',
		@idCont VARCHAR(MAX) = '83,81,79,76,84,77,88,82,78,80,92'


	CREATE TABLE #tbProdHTA (
			rnum INT,
			Prod VARCHAR(600),
			idEncounter INT

		)

	CREATE TABLE #tbProdDM (
			rnum INT,
			Prod VARCHAR(600),
			idEncounter INT

		)

	CREATE TABLE #tbActNu (
			rnum INT,
			value VARCHAR(100),
			idEncounter INT,
			idMedition INT
		)

	INSERT INTO #tbActNu	
	SELECT 
	ROW_NUMBER() OVER(PARTITION BY EHREvCust.idMedition, Eve.idEncounter ORDER BY EHREvCust.idEHREvent DESC) rnum,
	EHREvCust.value, 
	Eve.idEncounter, 
	EHREvCust.idMedition
	FROM EHREventICUMonitoringMeditions AS EHREvCust WITH(NOLOCK)
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEHREvent = Eve.idEHREvent
		INNER JOIN EHRConfUCIMonitoringMeditions AS EHREvCUsM WITH(NOLOCK) ON EHREvCust.idMonitoring = EHREvCUsM.idMonitoring
			AND EHREvCUsM.idMedition = EHREvCust.idMedition
	WHERE EHREvCust.idMonitoring IN (1036,1037)
		AND EHREvCUsM.isNumeric = 1



	INSERT INTO #tbProdHTA
	SELECT 
		ROW_NUMBER() OVER(PARTITION BY EV.idEncounter ORDER BY Diag.code) AS rnum,
		ISNULL(Prod.name,ProdG.name) AS Prod,
		EV.idEncounter
	FROM EHREvents AS EV WITH(NOLOCK)
		INNER JOIN EHREventMedicalDiagnostics AS EHREvMDiag WITH(NOLOCK) ON EHREvMDiag.idEHREvent = EV.idEHREvent
		INNER JOIN diagnostics AS Diag WITH(NOLOCK) ON EHREvMDiag.idDiagnostic = Diag.idDiagnostic
		INNER JOIN EHREvents AS EvF WITH(NOLOCK) ON Ev.idEncounter = EvF.idEncounter
		INNER JOIN EHREventFormulation AS EvFo WITH(NOLOCK) ON EvF.idEHREvent = Evfo.idEHREvent
		LEFT JOIN products AS Prod WITH(NOLOCK) ON EvFo.idProduct = Prod.idProduct
		LEFT JOIN productGenerics AS ProdG WITH(NOLOCK) ON EvFo.idProductGeneric = ProdG.idGenericProduct
	WHERE Diag.code IN ('I10X','P000','O16X','O149','O141','O13X','O11X','O109','O104','O103','O102','O101','O100','I701','I674','I270','I159','I158','I152','I151','I150','I139','I132','I131','I130','I129','I120','I119','I110','F453')
		AND EV.idCompany = 1


	INSERT INTO #tbProdDM
	SELECT 
		ROW_NUMBER() OVER(PARTITION BY EV.idEncounter ORDER BY Diag.code) AS rnum,
		ISNULL(Prod.name,ProdG.name) AS Prod,
		EV.idEncounter
	FROM EHREvents AS EV WITH(NOLOCK)
		INNER JOIN EHREventMedicalDiagnostics AS EHREvMDiag WITH(NOLOCK) ON EHREvMDiag.idEHREvent = EV.idEHREvent
		INNER JOIN diagnostics AS Diag WITH(NOLOCK) ON EHREvMDiag.idDiagnostic = Diag.idDiagnostic
		INNER JOIN EHREvents AS EvF WITH(NOLOCK) ON Ev.idEncounter = EvF.idEncounter
		INNER JOIN EHREventFormulation AS EvFo WITH(NOLOCK) ON EvF.idEHREvent = Evfo.idEHREvent
		LEFT JOIN products AS Prod WITH(NOLOCK) ON EvFo.idProduct = Prod.idProduct
		LEFT JOIN productGenerics AS ProdG WITH(NOLOCK) ON EvFo.idProductGeneric = ProdG.idGenericProduct
	WHERE Diag.code IN ('E140','Z713','T380','R730','P702','P701','P700','O249','O244','O243','O242','O241','O240','N251','E833','E748','E232','E144','E130','E126','E110','E100','G632','G633','E139','E123','E122','E121','E119','E118','E117','E116','E115','E114','E112','E111','E109','E108','E107','E106','E104','E102','G590','E105','E149','E129','E101','E103','E145','E148','E146','E136','E113','E128','E360') 
		AND EV.idCompany = 1



CREATE TABLE #tbResult(
	 idPatient INT,
	 idEncounter INT,
	 [TIPO DE IDENTIFICACIÓN] VARCHAR(120),
	 [NÚMERO DE IDENTIFICACIÓN] VARCHAR(120),
	 [INGRESO] INT,
	 [FECHA ASIGNACION PLAN] DATETIME,
	 [NOMBRES COMPLETOS] VARCHAR(120),
	 [CÓDIGO HABILITACIÓN] VARCHAR(120),
	 [NIT IPS] VARCHAR(120),
	 [CÓDIGO SUCURSAL] INT,
	 [FECHA DE INGRESO DEL USUARIO A LA IPS PAD] DATETIME,
	 [MUNICIPIO DE RESIDENCIA] VARCHAR(120),
	 [NÚMERO TELEFÓNICO NO.1 DEL PACIENTE] VARCHAR(120),
	 [NÚMERO TELEFÓNICO NO.2 DEL PACIENTE] VARCHAR(120),
	 [DIRECCIÓN DE RESIDENCIA DEL PACIENTE] VARCHAR(150),
	 [TALLA] VARCHAR(120),
	 [PESO] VARCHAR(120),
	 [TENSIÓN ARTERIAL SISTÓLICA] VARCHAR(120),
	 [TENSIÓN ARTERIAL DIASTÓLICA] VARCHAR(120),
	 [CIRCUNFERENCIA ABDOMINAL] VARCHAR(120),
	 [ASPECTO GENERAL] VARCHAR(120),
	 [INTEGRIDAD DE LA PIEL] VARCHAR(120),
	 [RED DE APOYO] VARCHAR(120),
	 [SOPORTE DE CUIDADOR] VARCHAR(120),
	 [SITUACIÓN ACTUAL DE DISCAPACIDAD] VARCHAR(120),
	 [ALIMENTACIÓN] VARCHAR(120),
	 [ACTIVIDADES EN BAÑO] VARCHAR(120),
	 [VESTIRSE] VARCHAR(120),
	 [ASEO PERSONAL] VARCHAR(120),
	 [DEPOSICIONES-CONTROL ANAL] VARCHAR(120),
	 [MICCION-CONTROL VESICAL] VARCHAR(120),
	 [MANEJO DE INODORO O RETRETE] VARCHAR(120),
	 [TRASLADO SILLA-CAMA] VARCHAR(120),
	 [DEAMBULACIÓN TRASLADO] VARCHAR(120),
	 [SUBIR O BAJAR ESCALONES] VARCHAR(120),
	 [VALORACIÓN BARTHEL] INT,
	 [INDICE KARNOFSKY] VARCHAR(120),
	 [CARACTERÍSTICAS DE LAS PATOLOGÍAS DE INGRESO DEL PACIENTE] VARCHAR(8000),
	 [FASE DE LA ENFERMEDAD DE INGRESO EN LA QUE PRESENTA EL USUARIO(A)] VARCHAR(120),
	 [ACCIONES INSEGURAS] VARCHAR(120),
	 [EVENTOS ADVERSOS PRESENTADOS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO] VARCHAR(120),
	 [DESCRIPCIÓN DE OTROS EVENTOS ADVERSOS] VARCHAR(120),
	 [NIT IPS DE OCURRENCIA DEL EVENTO ADVERSO] VARCHAR(120),
	 [FECHA DE EVENTO ADVERSO] DATETIME,
	 [GRADO DE LESIÓN DEL EVENTO ADVERSO] VARCHAR(120),
	 [PLAN DE INTERVENCIÓN- EVENTOS ADVERSOS] VARCHAR(120),
	 [FALLAS DE CALIDAD PRESENTADAS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO] VARCHAR(120),
	 [PLAN DE INTERVENCIÓN- FALLAS DE CALIDAD] VARCHAR(120),
	 [OBSERVACIÓN] VARCHAR (MAX),
	 [FECHA DE INGRESO AL PROGRAMA PAD] DATETIME,
	 [DIAGNÓSTICO PRINCIPAL CIE 10] VARCHAR(120),
	 [DIAGNÓSTICO NO.02 COMORBILIDAD PRINCIPAL CIE 10] VARCHAR(120),
	 [DIAGNÓSTICO NO.03 OTRAS COMORBILIDADES CIE 10] VARCHAR(120),
	 [CANTIDAD DE SERVICIOS SOLICITADOS] INT,
	 [CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO] VARCHAR(120),
	 [idHCRecordEncounter]INT,
	 [idHCRecordencounterHCActivities]INT,
	 [MEDICINA GENERAL] VARCHAR(120),
	 [MEDICINA ESPECIALIZADA] VARCHAR(120),
	 [ESPECIALIDAD MÉDICA DE INTERVENCIÓN] VARCHAR(120),
	 [ENFERMERIA PROFESIONAL] VARCHAR(120),
	 [NUTRICIÓN Y DIETÉTICA] VARCHAR(120),
	 [PSICOLOGÍA] VARCHAR(120),
	 [TRABAJO SOCIAL] VARCHAR(120),
	 [FONIATRIA Y FONOAUDIOLOGÍA] VARCHAR(120),
	 [FISIOTERAPIA] VARCHAR(120),
	 [TERAPIA RESPIRATORIA] VARCHAR(120),
	 [TERAPIA OCUPACIONAL] VARCHAR(120),
	 [AUXILIAR DE ENFERMERÍA] VARCHAR(120),
	 [CLASIFICACIÓN DE LA HERIDA] VARCHAR(120),
	 [DIMENSIÓN DE LA HERIDA] VARCHAR(120),
	 [PROFUNDIDAD/TEJIDOS AFECTADOS] VARCHAR(MAX),
	 [COMORBILIDAD] VARCHAR(120),
	 [ESTADIO DE LA HERIDA] VARCHAR(120),
	 [INFECCIÓN] VARCHAR(120),
	 [TIEMPO DE EVOLUCIÓN EN TRATAMIENTO CON CLÍNICA DE HERIDAS] VARCHAR(120),
	 [EVOLUCIÓN SOPORTADA EN VISITA MÉDICA O REGISTRO FOTOGRAFICO] VARCHAR(120),
	 [NIVEL ALBUMINA SÉRICA] VARCHAR(120),
	 [FECHA DE REPORTE DE ALBUMINA] VARCHAR(120),
	 [TIPO DE SOPORTE DE OXÍGENO] VARCHAR(120),
	 [CONSUMO DE OXÍGENO EN LITROS/MINUTO] VARCHAR(120),
	 [HORAS DE ADMINISTRACIÓN DE OXÍGENO AL DÍA] VARCHAR(120),
	 [FECHAS DE INICIO DE SOPORTE DE OXÍGENO] VARCHAR(120),
	 [EQUIPO PARA PRESIÓN POSITIVA] VARCHAR(120),
	 [TIEMPO REQUERIDO DE TRATAMIENTO] VARCHAR(120),
	 [FECHA INICIO VENTILACIÓN MÉCANICA CRÓNICA] VARCHAR(120),
	 [MODO DE VENTILACIÓN MÉCANICA] VARCHAR(120),
	 [DESCRIPCIÓN OTRO MODO DE VENTILACIÓN MÉCANICA] VARCHAR(120),
	 [MODO VENTILATORIO] VARCHAR(120),
	 [MODALIDAD VENTILATORIA] VARCHAR(120),
	 [DESCRIPCION MODALIDAD VENTILATORIA] VARCHAR(120),
	 [PEEP] VARCHAR(120),
	 [PEEP ALTO] VARCHAR(120),
	 [PEEP BAJO] VARCHAR(120),
	 [TIEMPO BAJO] VARCHAR(120),
	 [TIEMPO ALTO] VARCHAR(120),
	 [FRECUENCIA RESPIRATORIA TOTAL] VARCHAR(120),
	 [FRECUENCIA RESPIRATORIA PROGRAMADA] VARCHAR(120),
	 [FIO2] VARCHAR(120),
	 [TIPO DE VENTILADOR EN USO POR EL PACIENTE] VARCHAR(120),
	 [DESCRIPCIÓN OTRO TIPO DE VENTILADOR EN USO POR EL PACIENTE] VARCHAR(120),
	 [OBSERVACIONES] VARCHAR(MAX),
	 [FECHA DE CONTROL MÉDICO] VARCHAR(120),
	 [HTA] VARCHAR(120),
	 [FECHA DE DIÁGNOSTICO HTA] DATETIME,
	 [MEDICAMENTO 1  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL] VARCHAR(600),
	 [MEDICAMENTO 2  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL] VARCHAR(600),
	 [MEDICAMENTO 3  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL] VARCHAR(600),
	 [RIESGO DE LA HTA AL INGRESO] VARCHAR(120),
	 [DM] VARCHAR(120),
	 [TIPO DE DIABETES] VARCHAR(120),
	 [FECHA DE DIAGNÓSTICO DM] VARCHAR(120),
	 [MEDICAMENTO 1 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM] VARCHAR(600),
	 [MEDICAMENTO 2 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM] VARCHAR(600),
	 [MEDICAMENTO 3 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM] VARCHAR(600),
	 [TIPO DE INSULINA ADMINISTRADA AL INGRESO DEL PROGRAMA] VARCHAR(120),
	 [TIPO DE INSULINA ADMINISTRADA DURANTE EL CONTROL] VARCHAR(120),
	 [RIESGO DE LA DM AL INGRESO] VARCHAR(120),
	 [ERC] VARCHAR(120),
	 [FECHA DE DIAGNÓSTICO ERC] DATETIME,
	 [TFG INGRESO] VARCHAR(120),
	 [FECHA TFG INGRESO] VARCHAR(120),
	 [TFG ACTUAL] VARCHAR(120),
	 [FECHA TFG ACTUAL ] VARCHAR(120),
	 [MICROALBUMINURIA AL INGRESO DEL PROGRAMA] VARCHAR(120),
	 [FECHA MICROALBUMINURIA AL INGRESO DEL PROGRAMA] VARCHAR(120),
	 [ESTADIO ACTUAL DE LA PATOLOGÌA] VARCHAR(120),
	 [CREATININA SUERO] VARCHAR(120),
	 [FECHA DE CREATININA] VARCHAR(120),
	 [GLICEMIA] VARCHAR(120),
	 [FECHA DE TOMA DE GLICEMIA] VARCHAR(120),
	 [HEMOGLOBINA GLICOSILADA] VARCHAR(120),
	 [FECHA DE TOMA DE HEMOGLOBINA GLICOSILADA] VARCHAR(120),
	 [COLESTEROL TOTAL] VARCHAR(120),
	 [FECHA DE TOMA DE COLESTEROL TOTAL] VARCHAR(120),
	 [COLESTEROL HDL] VARCHAR(120),
	 [FECHA DE TOMA DE COLESTEROL HDL] VARCHAR(120),
	 [COLESTEROL LDL] VARCHAR(120),
	 [FECHA DE TOMA DE COLESTEROL LDL] VARCHAR(120),
	 [TRIGLICERIDOS] VARCHAR(120),
	 [FECHA DE TOMA DE TRIGLICERIDOS] VARCHAR(120),
	 [MICRO ALBUMINURIA] VARCHAR(120),
	 [FECHA DE TOMA DE MICRO ALBUMINURIA] VARCHAR(120),
	 [RELACIÓN MICROALBUMINURIA/CREATINURIA] VARCHAR(120),
	 [FECHA DE RELACIÓN MICROALBUMINURIA/CREATINURIA] VARCHAR(120)
)

DECLARE
	@idAct  VARCHAR(MAX),
	@idActEl VARCHAR(MAX)


INSERT INTO #tbResult
SELECT 
	Pat.idUser,
	Enc.idEncounter,
	Doc.code + ' | ' + Doc.name AS [TIPO DE IDENTIFICACIÓN],
	Pat.documentNumber AS [NÚMERO DE IDENTIFICACIÓN],
	Enc.identifier AS [INGRESO],
	EncHc.dateStart AS [FECHA ASIGNACION PLAN],
	(Pat.firstGivenName+' '+Pat.SecondGiveName+' '+Pat.firstFamilyName+' '+Pat.secondFamilyName) AS [NOMBRES COMPLETOS],
	Office.legalCode AS [CÓDIGO HABILITACIÓN],
	Ucom.documentNumber AS [NIT IPS],
	RIGHT(Office.legalCode,1) AS [CÓDIGO SUCURSAL],
	--Enc.dateStart
	'' AS [FECHA DE INGRESO DEL USUARIO A LA IPS PAD],
	CityD.codeConcatenate AS [MUNICIPIO DE RESIDENCIA],
	PatU.telecom AS [NÚMERO TELEFÓNICO NO.1 DEL PACIENTE],
	PatU.phoneHome AS [NÚMERO TELEFÓNICO NO.2 DEL PACIENTE],
	PatU.homeAddress AS [DIRECCIÓN DE RESIDENCIA DEL PACIENTE],
	'' AS [TALLA],
	'' AS [PESO],
	'' AS [TENSIÓN ARTERIAL SISTÓLICA],
	'' AS [TENSIÓN ARTERIAL DIASTÓLICA],
	'' AS [CIRCUNFERENCIA ABDOMINAL],
	'' AS [ASPECTO GENERAL],
	'' AS [INTEGRIDAD DE LA PIEL],
	'' AS [RED DE APOYO],
	'' AS [SOPORTE DE CUIDADOR],
	'' AS [SITUACIÓN ACTUAL DE DISCAPACIDAD],
	'' AS [ALIMENTACIÓN],
	'' AS [ACTIVIDADES EN BAÑO],
	'' AS [VESTIRSE],
	'' AS [ASEO PERSONAL],
	'' AS [DEPOSICIONES-CONTROL ANAL],
	'' AS [MICCION-CONTROL VESICAL],
	'' AS [MANEJO DE INODORO O RETRETE],
	'' AS [TRASLADO SILLA-CAMA],
	'' AS [DEAMBULACIÓN TRASLADO],
	'' AS [SUBIR O BAJAR ESCALONES],
	'' AS [VALORACIÓN BARTHEL],
	'' AS [INDICE KARNOFSKY],
	'' AS [CARACTERÍSTICAS DE LAS PATOLOGÍAS DE INGRESO DEL PACIENTE],
	'' AS [FASE DE LA ENFERMEDAD DE INGRESO EN LA QUE PRESENTA EL USUARIO(A)],
	'' AS [ACCIONES INSEGURAS],
	'' AS [EVENTOS ADVERSOS PRESENTADOS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO],
	'' AS [DESCRIPCIÓN DE OTROS EVENTOS ADVERSOS],
	Ucom.documentNumber AS [NIT IPS DE OCURRENCIA DEL EVENTO ADVERSO],
	'' AS [FECHA DE EVENTO ADVERSO],
	'' AS [GRADO DE LESIÓN DEL EVENTO ADVERSO],
	'' AS [FALLAS DE CALIDAD PRESENTADAS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO],
	'' AS [PLAN DE INTERVENCIÓN- EVENTOS ADVERSOS],
	'' AS [PLAN DE INTERVENCIÓN- FALLAS DE CALIDAD],
	'' AS [OBSERVACIÓN],
	'' AS [FECHA DE INGRESO AL PROGRAMA PAD],
	'' AS [DIAGNÓSTICO PRINCIPAL CIE 10],
	'' AS [DIAGNÓSTICO NO.02 COMORBILIDAD PRINCIPAL CIE 10],
	'' AS [DIAGNÓSTICO NO.03 OTRAS COMORBILIDADES CIE 10],
	'1'  AS [CANTIDAD DE SERVICIOS SOLICITADOS],
	EHRconfAct.codeActivity AS [CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO],
	EncHc.idHCRecord AS [idHCRecordEncounter],
	EncHc.idHCRecord AS [idHCRecordencounterHCActivities],
	'' AS [MEDICINA GENERAL],
	'' AS [MEDICINA ESPECIALIZADA],
	'' AS [ESPECIALIDAD MÉDICA DE INTERVENCIÓN],
	'' AS [ENFERMERIA PROFESIONAL],
	'' AS [NUTRICIÓN Y DIETÉTICA],
	'' AS [PSICOLOGÍA],
	'' AS [TRABAJO SOCIAL],
	'' AS [FONIATRIA Y FONOAUDIOLOGÍA],
	'' AS [FISIOTERAPIA],
	'' AS [TERAPIA RESPIRATORIA],
	'' AS [TERAPIA OCUPACIONAL],
	'' AS [AUXILIAR DE ENFERMERÍA],
	'' AS [CLASIFICACIÓN DE LA HERIDA],
	'' AS [DIMENSIÓN DE LA HERIDA],
	'' AS [PROFUNDIDAD/TEJIDOS AFECTADOS],
	'' AS [COMORBILIDAD],
	'' AS [ESTADIO DE LA HERIDA],
	'' AS [INFECCIÓN],
	'' AS [TIEMPO DE EVOLUCIÓN EN TRATAMIENTO CON CLÍNICA DE HERIDAS],
	'' AS [EVOLUCIÓN SOPORTADA EN VISITA MÉDICA O REGISTRO FOTOGRAFICO],
	'' AS [NIVEL ALBUMINA SÉRICA],
	'' AS [FECHA DE REPORTE DE ALBUMINA],
	'' AS [TIPO DE SOPORTE DE OXÍGENO],
	'' AS [CONSUMO DE OXÍGENO EN LITROS/MINUTO],
	'' AS [HORAS DE ADMINISTRACIÓN DE OXÍGENO AL DÍA],
	'' AS [FECHAS DE INICIO DE SOPORTE DE OXÍGENO],
	'' AS [EQUIPO PARA PRESIÓN POSITIVA],
	'' AS [TIEMPO REQUERIDO DE TRATAMIENTO],
	'' AS [FECHA INICIO VENTILACIÓN MÉCANICA CRÓNICA],
	'' AS [MODO DE VENTILACIÓN MÉCANICA],
	'' AS [DESCRIPCIÓN OTRO MODO DE VENTILACIÓN MÉCANICA],
	'Seleccione' AS [MODO VENTILATORIO],
	'Seleccione' AS [MODALIDAD VENTILATORIA],
	'' AS [DESCRIPCION MODALIDAD VENTILATORIA],
	'' AS [PEEP],
	'' AS [PEEP ALTO],
	'' AS [PEEP BAJO],
	'' AS [TIEMPO BAJO],
	'' AS [TIEMPO ALTO],
	'' AS [FRECUENCIA RESPIRATORIA TOTAL],
	'' AS [FRECUENCIA RESPIRATORIA PROGRAMADA],
	'' AS [FIO2],
	'Seleccione' AS [TIPO DE VENTILADOR EN USO POR EL PACIENTE],
	'' AS [DESCRIPCIÓN OTRO TIPO DE VENTILADOR EN USO POR EL PACIENTE],
	'' AS [OBSERVACIONES],
	'' AS [FECHA DE CONTROL MÉDICO],
	'' AS [HTA],
	'' AS [FECHA DE DIÁGNOSTICO HTA],
	'' AS [MEDICAMENTO 1  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL],
	'' AS [MEDICAMENTO 2  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL],
	'' AS [MEDICAMENTO 3  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL],
	'' AS [RIESGO DE LA HTA AL INGRESO],
	'' AS [DM],
	'' AS [TIPO DE DIABETES],
	'' AS [FECHA DE DIAGNÓSTICO DM],
	'' AS [MEDICAMENTO 1 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM],
	'' AS [MEDICAMENTO 2 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM],
	'' AS [MEDICAMENTO 3 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM],
	'' AS [TIPO DE INSULINA ADMINISTRADA AL INGRESO DEL PROGRAMA],
	'' AS [TIPO DE INSULINA ADMINISTRADA DURANTE EL CONTROL],
	'' AS [RIESGO DE LA DM AL INGRESO],
	'' AS [ERC],
	'' AS [FECHA DE DIAGNÓSTICO ERC],
	'' AS [TFG INGRESO],
	'' AS [FECHA TFG INGRESO],
	'' AS [TFG ACTUAL],
	'' AS [FECHA TFG ACTUAL ],
	'' AS [MICROALBUMINURIA AL INGRESO DEL PROGRAMA],
	'' AS [FECHA MICROALBUMINURIA AL INGRESO DEL PROGRAMA],
	'' AS [ESTADIO ACTUAL DE LA PATOLOGÌA],
	'' AS [CREATININA SUERO],
	'' AS [FECHA DE CREATININA],
	'' AS [GLICEMIA],
	'' AS [FECHA DE TOMA DE GLICEMIA],
	'' AS [HEMOGLOBINA GLICOSILADA],
	'' AS [FECHA DE TOMA DE HEMOGLOBINA GLICOSILADA],
	'' AS [COLESTEROL TOTAL],
	'' AS [FECHA DE TOMA DE COLESTEROL TOTAL],
	'' AS [COLESTEROL HDL],
	'' AS [FECHA DE TOMA DE COLESTEROL HDL],
	'' AS [COLESTEROL LDL],
	'' AS [FECHA DE TOMA DE COLESTEROL LDL],
	'' AS [TRIGLICERIDOS],
	'' AS [FECHA DE TOMA DE TRIGLICERIDOS],
	'' AS [MICRO ALBUMINURIA],
	'' AS [FECHA DE TOMA DE MICRO ALBUMINURIA],
	'' AS [RELACIÓN MICROALBUMINURIA/CREATINURIA],
	'' AS [FECHA DE RELACIÓN MICROALBUMINURIA/CREATINURIA]
FROM encounters AS Enc WITH(NOLOCK)
	INNER JOIN users AS Pat WITH(NOLOCK) ON Enc.idUserPatient = Pat.idUser
	INNER JOIN userConfTypeDocuments AS Doc WITH(NOLOCK) ON Pat.idDocumentType = Doc.idTypeDocument
	INNER JOIN companyOffices AS Office WITH(NOLOCK) ON Enc.idOffice = Office.idOffice
	INNER JOIN userPeople AS PatU WITH(NOLOCK) ON Pat.idUser = PatU.idUser
	INNER JOIN users AS Ucom WITH(NOLOCK) ON Office.idUserCompany = Ucom.idUser
	INNER JOIN generalPoliticalDivisions AS City WITH(NOLOCK) ON PatU.idHomePlacePoliticalDivision = City.idPoliticalDivision
	INNER JOIN generalPoliticalDivisions AS CityD WITH(NOLOCK) ON City.idParent = CityD.idPoliticalDivision
	INNER JOIN encounterHC AS EncHc WITH(NOLOCK) ON Enc.idEncounter = EncHc.idEncounter
	INNER JOIN ehrconfhcActivity AS EHRconfAct WITH(NOLOCK) ON EncHc.idHCActivity = EHRconfAct.idHCActivity
	AND EHRconfAct.idCompany = @idUserCompany
	INNER JOIN encounterRecords AS EncR WITH(NOLOCK) ON Enc.idEncounter = EncR.idEncounter 
WHERE Enc.idUserCompany = @idUserCompany
	--AND Enc.dateStart BETWEEN @dateStart AND (@dateEnd + '23:59:59')
	AND EncHc.dateStart BETWEEN @dateStart AND (@dateEnd + '23:59:59')
	AND Enc.idStatus = 2 -- The Encounter has begun and the patient is present / the practitioner and the patient are meeting
	AND Enc.idOffice IN (SELECT Value FROM dbo.FnSplit (@OfficeFilter))
	AND EncR.idPrincipalContractee IN (SELECT Value FROM dbo.FnSplit (@idIns))
	AND EncR.idPrincipalContract IN (SELECT Value FROM dbo.FnSplit (@idCont))
	AND EncHc.isActive=1


UPDATE #tbResult SET
	[FECHA DE INGRESO DEL USUARIO A LA IPS PAD]= (SELECT TOP 1
							EHREvCust.valueText
						FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
							INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
						WHERE EHREvCust.idConfigActivity = 375
							AND EHREvCust.idElement = 1
							AND Eve.idPatient = #tbResult.idPatient
							--AND Eve.idEncounter = #tbResult.idEncounter
							order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	TALLA = (SELECT TOP 1 CONVERT(VARCHAR,CONVERT(INT,EHRPatM.recordedValue)) AS TALLA
				FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
					INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
					INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHRPatM.idEHREvent = Eve.idEHREvent
				WHERE EHRPatM.idUserPatient = #tbResult.idPatient
					AND EHRCM.name LIKE '%Talla%'
					AND (Eve.idAction = 1013
								OR Eve.idAction= 1004
								OR Eve.idAction= 1023)
				ORDER BY EHRPatM.recordedDate DESC)

UPDATE #tbResult SET	
	PESO = (SELECT TOP 1 CONVERT(VARCHAR,CONVERT(INT,EHRPatM.recordedValue)) AS PESO
				FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
					INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
					INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHRPatM.idEHREvent = Eve.idEHREvent
				WHERE EHRPatM.idUserPatient = #tbResult.idPatient
					AND EHRCM.name LIKE '%peso%'
					AND (Eve.idAction = 1013
								OR Eve.idAction= 1004
								OR Eve.idAction= 1023)
				ORDER BY EHRPatM.recordedDate DESC)

UPDATE #tbResult SET	
	[TENSIÓN ARTERIAL SISTÓLICA] = (SELECT TOP 1 CONVERT(VARCHAR,CONVERT(INT,EHRPatM.recordedValue)) AS [TENSIÓN ARTERIAL SISTÓLICA]
				FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
					INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
					INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHRPatM.idEHREvent = Eve.idEHREvent
				WHERE EHRPatM.idUserPatient = #tbResult.idPatient
					AND EHRCM.name LIKE '%P.A.%Sist_lica%'
					AND (Eve.idAction = 1013
								OR Eve.idAction= 1004
								OR Eve.idAction= 1023)
				ORDER BY EHRPatM.recordedDate DESC)

UPDATE #tbResult SET	
	[TENSIÓN ARTERIAL DIASTÓLICA] = (SELECT TOP 1 CONVERT(VARCHAR,CONVERT(INT,EHRPatM.recordedValue)) AS [TENSIÓN ARTERIAL DIASTÓLICA]
				FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
					INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
					INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHRPatM.idEHREvent = Eve.idEHREvent
				WHERE EHRPatM.idUserPatient = #tbResult.idPatient
					AND EHRCM.name LIKE '%P.A.%Diast_lica%'
					AND (Eve.idAction = 1013
								OR Eve.idAction= 1004
								OR Eve.idAction= 1023)
				ORDER BY EHRPatM.recordedDate DESC)


UPDATE #tbResult SET	
	[CIRCUNFERENCIA ABDOMINAL] = (SELECT TOP 1 CONVERT(VARCHAR,CONVERT(INT,EHRPatM.recordedValue)) AS [CIRCUNFERENCIA ABDOMINAL]
				FROM EHRPatientMeasurements AS EHRPatM WITH(NOLOCK)
					INNER JOIN EHRConfMeasurements AS EHRCM WITH(NOLOCK) ON EHRCM.idMeasurement = EHRPatM.idMeasurement
					INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHRPatM.idEHREvent = Eve.idEHREvent
				WHERE EHRPatM.idUserPatient = #tbResult.idPatient
					AND EHRCM.name LIKE '%Circunferencia%Abdominal%'
					AND (Eve.idAction = 1013
								OR Eve.idAction= 1004
								OR Eve.idAction= 1023)
				ORDER BY EHRPatM.recordedDate DESC)


SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'AspGen'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'AspGenEle'

UPDATE #tbResult SET	
	[ASPECTO GENERAL] = (SELECT TOP 1
							EHREvCust.valueText
						FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
							INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
						WHERE EHREvCust.idConfigActivity = 301
							AND EHREvCust.idElement = 1
							AND Eve.idPatient = #tbResult.idPatient
							--AND Eve.idEncounter = #tbResult.idEncounter
							order by Eve.actionRecordedDate DESC)



SET @idAct = ''
SET @idActEl = ''


SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'InPiel'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'InPielEle'

UPDATE #tbResult SET	
	[INTEGRIDAD DE LA PIEL] = (SELECT TOP 1
							EHREvCust.valueText
						FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
							INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
						WHERE EHREvCust.idConfigActivity = 302
							AND EHREvCust.idElement = 1
							AND Eve.idPatient = #tbResult.idPatient
							--AND Eve.idEncounter = #tbResult.idEncounter
							order by Eve.actionRecordedDate DESC)

SET @idAct = ''
SET @idActEl = ''


SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'RedApo'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'RedApoEle'

UPDATE #tbResult SET	
	[RED DE APOYO] = (SELECT TOP 1
							EHREvCust.valueText
						FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
							INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
						WHERE EHREvCust.idConfigActivity = @idAct
							AND EHREvCust.idElement = @idActEl
							AND Eve.idPatient = #tbResult.idPatient
							--AND Eve.idEncounter = #tbResult.idEncounter
							order by Eve.actionRecordedDate DESC)

SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'SopCui'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'SopCuiEle'

UPDATE #tbResult SET	
	[SOPORTE DE CUIDADOR] = (SELECT TOP 1
							EHREvCust.valueText
						FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
							INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
						WHERE EHREvCust.idConfigActivity = @idAct
							AND EHREvCust.idElement = @idActEl
							AND Eve.idPatient = #tbResult.idPatient
							--AND Eve.idEncounter = #tbResult.idEncounter
							order by Eve.actionRecordedDate DESC)

SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'SitDes'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'SitDesEle'


UPDATE #tbResult SET	
	[SITUACIÓN ACTUAL DE DISCAPACIDAD] = (SELECT TOP 1 
											EHRconfS.name
										FROM EHREventMedicalScales AS EHREVMS WITH(NOLOCK)
											INNER JOIN EHRConfScaleValorations AS EHRconfS WITH(NOLOCK) ON EHREVMS.idScale = EHRconfS.idScale
												AND EHRconfS.idRecord = EHREVMS.idEvaluation
											INNER JOIN EHREvents AS EV	WITH(NOLOCK) ON EHREVMS.idEHREvent = EV.idEHREvent
										WHERE EHREVMS.idScale = 11
											AND Ev.idPatient = #tbResult.idPatient
											--AND EV.idEncounter = #tbResult.idEncounter
											order by EV.actionRecordedDate DESC )


UPDATE #tbResult SET	
	ALIMENTACIÓN = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 1
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC )

UPDATE #tbResult SET	
	[ACTIVIDADES EN BAÑO] = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 2
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC )

UPDATE #tbResult SET	
	VESTIRSE = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 3
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC )

UPDATE #tbResult SET	
	[ASEO PERSONAL] = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 4
	    AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[DEPOSICIONES-CONTROL ANAL] = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 6
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[MICCION-CONTROL VESICAL] = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 5
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[MANEJO DE INODORO O RETRETE] = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 7
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[TRASLADO SILLA-CAMA] = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 8
	    AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[DEAMBULACIÓN TRASLADO] = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 9
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[SUBIR O BAJAR ESCALONES] = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11 
		AND EHRCQA.idQuestion = 10
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)

--CAMPO OK
UPDATE #tbResult SET
		[VALORACIÓN BARTHEL]=	((SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
		AND EHRCQA.idQuestion = 1
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	)
	+
		(SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
	    AND EHRCQA.idQuestion = 2
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	
	+
		(SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
		AND EHRCQA.idQuestion = 3
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	
	+
		(SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
		AND EHRCQA.idQuestion = 4
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	+
		(SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
		AND EHRCQA.idQuestion = 5
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	+
		(SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
		AND EHRCQA.idQuestion = 6
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	+
		(SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
		AND EHRCQA.idQuestion = 7
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	+
		(SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
		AND EHRCQA.idQuestion = 8
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	+
		(SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
		AND EHRCQA.idQuestion = 9
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	+
		(SELECT TOP 1
		CONVERT(INT,EHRCQA.value)
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 11
		AND EHRCQA.idQuestion = 10
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)
	
	


UPDATE #tbResult SET	
	[INDICE KARNOFSKY] = (SELECT TOP 1
		EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
	FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
		INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
			AND EHRCQA.idQuestion = EHREvMS.idQuestion
			AND EHRCQA.idAnswer = EHREvMS.idAnswer
		INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
	WHERE EHRCQA.idScale = 12
		AND EHRCQA.idQuestion = 1
		AND Eve.idPatient = #tbResult.idPatient
		--AND Eve.idEncounter = #tbResult.idEncounter
		order by Eve.actionRecordedDate DESC)


UPDATE #tbResult SET	
	[CARACTERÍSTICAS DE LAS PATOLOGÍAS DE INGRESO DEL PACIENTE] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 293
									AND EHREvCust.idElement = 3
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[FASE DE LA ENFERMEDAD DE INGRESO EN LA QUE PRESENTA EL USUARIO(A)] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 293
									AND EHREvCust.idElement = 4
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult
SET [ACCIONES INSEGURAS] = ( SELECT TOP 1 EHREvCust.valueText
                                FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
                                INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
                                WHERE EHREvCust.idConfigActivity = 374
                                    AND EHREvCust.idElement = 1
                                    AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
                                ORDER BY Eve.actionRecordedDate DESC)
                       

UPDATE #tbResult SET	
	[EVENTOS ADVERSOS PRESENTADOS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 374
									AND EHREvCust.idElement = 2
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)


UPDATE #tbResult SET	
	[DESCRIPCIÓN DE OTROS EVENTOS ADVERSOS] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 374
									AND EHREvCust.idElement = 3
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)


--UPDATE #tbResult SET		[NIT IPS DE OCURRENCIA DEL EVENTO ADVERSO] = NULL

UPDATE #tbResult SET	
	[FECHA DE EVENTO ADVERSO] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 374
									AND EHREvCust.idElement = 4
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[GRADO DE LESIÓN DEL EVENTO ADVERSO] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 374
									AND EHREvCust.idElement = 5
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[PLAN DE INTERVENCIÓN- EVENTOS ADVERSOS] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 374
									AND EHREvCust.idElement = 6
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET
[FALLAS DE CALIDAD PRESENTADAS EN LA ATENCIÓN DE PACIENTES EN EL DOMICILIO]=(SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 374
									AND EHREvCust.idElement = 9
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[PLAN DE INTERVENCIÓN- FALLAS DE CALIDAD] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 374
									AND EHREvCust.idElement = 7
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[OBSERVACIÓN] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 374
									AND EHREvCust.idElement = 8
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET 
     [FECHA DE INGRESO AL PROGRAMA PAD] =(Select TOP 1 pro.datestart 
											FROM EHRPatientTimeEvents AS PRO
													INNER JOIN EHRConfPrograms AS CONFPRO ON CONFPRO.idProgram = PRO.idProgram
													INNER JOIN EHRConfHCActivity AS CONFI WITH(NOLOCK) ON CONFPRO.code = RIGHT(CONFI.codeactivity,5)
											where PRO.idPatient = #tbResult.idPatient
												AND  CONFPRO.code= RIGHT(#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO],5)
												AND pro.dateend IS NULL)

UPDATE #tbResult SET	
	[DIAGNÓSTICO PRINCIPAL CIE 10] = (SELECT  TOP 1
										Diag.code
									FROM EHREventMedicalDiagnostics AS EHREMDiag WITH(NOLOCK)
										INNER JOIN EHREvents AS EV WITH(NOLOCK) ON EHREMDiag.idEHREvent = EV.idEHREvent
										INNER JOIN diagnostics AS Diag WITH(NOLOCK) ON Diag.idDiagnostic = EHREMDiag.idDiagnostic
									WHERE  EV.idPatient = #tbResult.idPatient
									--EV.idEncounter = #tbResult.idEncounter
										AND EHREMDiag.isPrincipal = 1
										AND (EV.idAction = 1013
								OR EV.idAction= 1004
								OR EV.idAction= 1023))

UPDATE #tbResult SET	
	[DIAGNÓSTICO NO.02 COMORBILIDAD PRINCIPAL CIE 10] =(SELECT  TOP 1
										Diag.code
									FROM EHREventMedicalDiagnostics AS EHREMDiag WITH(NOLOCK)
										INNER JOIN EHREvents AS EV WITH(NOLOCK) ON EHREMDiag.idEHREvent = EV.idEHREvent
										INNER JOIN diagnostics AS Diag WITH(NOLOCK) ON Diag.idDiagnostic = EHREMDiag.idDiagnostic
									WHERE  EV.idPatient = #tbResult.idPatient
									--EV.idEncounter = #tbResult.idEncounter
										AND EHREMDiag.isPrincipal = 0
										AND (EV.idAction = 1013
								OR EV.idAction= 1004
								OR EV.idAction= 1023))

UPDATE #tbResult SET	
	[DIAGNÓSTICO NO.03 OTRAS COMORBILIDADES CIE 10] =(SELECT  TOP 1
										Diag.code
									FROM EHREventMedicalDiagnostics AS EHREMDiag WITH(NOLOCK)
										INNER JOIN EHREvents AS EV WITH(NOLOCK) ON EHREMDiag.idEHREvent = EV.idEHREvent
										INNER JOIN diagnostics AS Diag WITH(NOLOCK) ON Diag.idDiagnostic = EHREMDiag.idDiagnostic
									WHERE  EV.idPatient = #tbResult.idPatient
									--EV.idEncounter = #tbResult.idEncounter
										AND EHREMDiag.isPrincipal = 0
										AND (EV.idAction = 1013
								OR EV.idAction= 1004
								OR EV.idAction= 1023)
										order by Diag.code DESC )

	--[CANTIDAD DE SERVICIOS SOLICITADOS]

UPDATE #tbResult SET	
	[MEDICINA GENERAL] = (SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 4984
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)



UPDATE #tbResult SET	
	[MEDICINA ESPECIALIZADA] = (SELECT TOP 1 EveA.value 
							FROM #tbActNu AS EveA 
							WHERE EveA.idEncounter = #tbResult.idEncounter
								AND EveA.rnum = 1
								AND EveA.idMedition = 2650)

UPDATE #tbResult SET	
	[ESPECIALIDAD MÉDICA DE INTERVENCIÓN] = (SELECT TOP 1 EveA.value 
							FROM #tbActNu AS EveA 
							WHERE EveA.idEncounter = #tbResult.idEncounter
								AND EveA.rnum = 1
								AND EveA.idMedition = 2666)

UPDATE #tbResult SET	
	[ENFERMERIA PROFESIONAL] = (SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 4987
										AND Act.idRol =139
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)
	
	

UPDATE #tbResult
SET [NUTRICIÓN Y DIETÉTICA] =   (SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 40496
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)
										
									 
									 
UPDATE #tbResult SET	
	PSICOLOGÍA = (SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 4989
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)

UPDATE #tbResult SET	
	[TRABAJO SOCIAL] = (SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 4990
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)

UPDATE #tbResult SET	
	[FONIATRIA Y FONOAUDIOLOGÍA] = (SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 40497
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)

UPDATE #tbResult SET	
	FISIOTERAPIA = (SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 4992
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)

UPDATE #tbResult SET	
	[TERAPIA RESPIRATORIA] = (SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 4993
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)

UPDATE #tbResult SET	
	[TERAPIA OCUPACIONAL] = (SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 4994
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)

UPDATE #tbResult SET	
	[AUXILIAR DE ENFERMERÍA] = ((SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 4987
										AND Act.idRol =141
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC)
										+
										(SELECT TOP 1 TRY_CAST(Act.quantityTODO AS INT)  -- or use CAST(Act.quantityTODO AS INT) if using older SQL Server versions
									    FROM encounterHC AS planus
										INNER JOIN encounters AS INGR WITH(NOLOCK) ON planus.idEncounter = INGR.idEncounter
										INNER JOIN encounterHCActivities AS Act WITH(NOLOCK) ON planus.idEncounter = Act.idEncounter 
										INNER JOIN EHRConfHCActivity AS CONF WITH(NOLOCK) ON planus.idHCActivity = CONF.idHCActivity
										WHERE planus.isActive = 1
										AND Act.idProduct = 4525
										AND Act.idRol =141
										AND planus.idHCActivity = CONF.idHCActivity
										AND planus.idHCRecord=#tbResult.idHCRecordEncounter
										AND INGR.idEncounter = #tbResult.idEncounter
										AND planus.idHCRecord = Act.idHCRecord
										AND CONF.codeActivity=#tbResult.[CÓDIGO SERVICIO DE ATENCIÓN REQUERIDA POR EL USUARIO]
										ORDER BY Act.dateRegister DESC))


SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ClaHer'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ClaHerEle'


UPDATE #tbResult SET	
	[CLASIFICACIÓN DE LA HERIDA] = (SELECT TOP 1
										EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
									FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
										INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
											AND EHRCQA.idQuestion = EHREvMS.idQuestion
											AND EHRCQA.idAnswer = EHREvMS.idAnswer
										INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
									WHERE EHRCQA.idScale = @idAct
										AND EHRCQA.idQuestion = @idActEl
										 AND Eve.idPatient = #tbResult.idPatient
										--Eve.idEncounter = #tbResult.idEncounter
										order by Eve.actionRecordedDate DESC) 

SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'DimHer'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'DimHerEle'

UPDATE #tbResult SET	
	[DIMENSIÓN DE LA HERIDA] = (SELECT TOP 1
										EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
									FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
										INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
											AND EHRCQA.idQuestion = EHREvMS.idQuestion
											AND EHRCQA.idAnswer = EHREvMS.idAnswer
										INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
									WHERE EHRCQA.idScale = @idAct 
										AND EHRCQA.idQuestion = @idActEl
										AND Eve.idPatient = #tbResult.idPatient
										--Eve.idEncounter = #tbResult.idEncounter
										order by Eve.actionRecordedDate DESC) 

SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ProHer'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ProHerEle'


UPDATE #tbResult SET	
	[PROFUNDIDAD/TEJIDOS AFECTADOS] = (SELECT TOP 1
										EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
									FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
										INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
											AND EHRCQA.idQuestion = EHREvMS.idQuestion
											AND EHRCQA.idAnswer = EHREvMS.idAnswer
										INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
									WHERE EHRCQA.idScale = @idAct
										AND EHRCQA.idQuestion = @idActEl
										AND Eve.idPatient = #tbResult.idPatient
										--AND Eve.idEncounter = #tbResult.idEncounter
										order by Eve.actionRecordedDate DESC)

SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'Comor'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'ComorEle'

UPDATE #tbResult SET	
	COMORBILIDAD = (SELECT TOP 1
										EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
									FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
										INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
											AND EHRCQA.idQuestion = EHREvMS.idQuestion
											AND EHRCQA.idAnswer = EHREvMS.idAnswer
										INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
									WHERE EHRCQA.idScale = @idAct
										AND EHRCQA.idQuestion = @idActEl
										AND Eve.idPatient = #tbResult.idPatient
										--AND Eve.idEncounter = #tbResult.idEncounter
										order by Eve.actionRecordedDate DESC)

SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'EstHer'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'EstHerEle'

UPDATE #tbResult SET	
	[ESTADIO DE LA HERIDA] = (SELECT TOP 1
										EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
									FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
										INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
											AND EHRCQA.idQuestion = EHREvMS.idQuestion
											AND EHRCQA.idAnswer = EHREvMS.idAnswer
										INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
									WHERE EHRCQA.idScale = @idAct
										AND EHRCQA.idQuestion = @idActEl
										AND Eve.idPatient = #tbResult.idPatient
										--AND Eve.idEncounter = #tbResult.idEncounter
										order by Eve.actionRecordedDate DESC)

SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'Infecc'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'InfeccEle'

UPDATE #tbResult SET	
	INFECCIÓN = (SELECT TOP 1
					EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
				FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
					INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
						AND EHRCQA.idQuestion = EHREvMS.idQuestion
						AND EHRCQA.idAnswer = EHREvMS.idAnswer
					INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
				WHERE EHRCQA.idScale = @idAct
					AND EHRCQA.idQuestion = @idActEl
					AND Eve.idPatient = #tbResult.idPatient
					--AND Eve.idEncounter = #tbResult.idEncounter
					order by Eve.actionRecordedDate DESC)

SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'TEvoTr'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'TEvoTrEle'

UPDATE #tbResult SET	
	[TIEMPO DE EVOLUCIÓN EN TRATAMIENTO CON CLÍNICA DE HERIDAS] = (SELECT TOP 1
					EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
				FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
					INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
						AND EHRCQA.idQuestion = EHREvMS.idQuestion
						AND EHRCQA.idAnswer = EHREvMS.idAnswer
					INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
				WHERE EHRCQA.idScale = @idAct
					AND EHRCQA.idQuestion = @idActEl
					AND Eve.idPatient = #tbResult.idPatient
					--AND Eve.idEncounter = #tbResult.idEncounter
					order by Eve.actionRecordedDate DESC)

SET @idAct = ''
SET @idActEl = ''

SELECT @idAct = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'EvSopo'

SELECT @idActEl = Com.valueField FROM companyReportParams AS Com WITH(NOLOCK) WHERE Com.codeReport = 'rptEPS' AND Com.codeField = 'EvSopoEle'

UPDATE #tbResult SET	
	[EVOLUCIÓN SOPORTADA EN VISITA MÉDICA O REGISTRO FOTOGRAFICO] = (SELECT TOP 1
					EHRCQA.description + ' - ' + CONVERT(VARCHAR,CONVERT(INT,EHRCQA.value))
				FROM EHRConfScaleQuestionAnswers AS EHRCQA WITH(NOLOCK)
					INNER JOIN EHREventMedicalScaleQuestions AS EHREvMS WITH(NOLOCK) ON EHREvMS.idScale = EHRCQA.idScale
						AND EHRCQA.idQuestion = EHREvMS.idQuestion
						AND EHRCQA.idAnswer = EHREvMS.idAnswer
					INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvMS.idEHREvent = Eve.idEHREvent
				WHERE EHRCQA.idScale = @idAct
					AND EHRCQA.idQuestion = @idActEl
					AND Eve.idPatient = #tbResult.idPatient
					--AND Eve.idEncounter = #tbResult.idEncounter
					order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[NIVEL ALBUMINA SÉRICA] = (SELECT TOP 1
							EHREvCust.valueText
						FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
							INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
						WHERE EHREvCust.idConfigActivity = 303
							AND EHREvCust.idElement = 1
							AND Eve.idPatient = #tbResult.idPatient
							--AND Eve.idEncounter = #tbResult.idEncounter
							order by Eve.actionRecordedDate DESC)
	
UPDATE #tbResult SET	
	[FECHA DE REPORTE DE ALBUMINA] = (SELECT TOP 1
							EHREvCust.valueText
						FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
							INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
						WHERE EHREvCust.idConfigActivity = 303
							AND EHREvCust.idElement = 2
							AND Eve.idPatient = #tbResult.idPatient
							--AND Eve.idEncounter = #tbResult.idEncounter
							order by Eve.actionRecordedDate DESC)


UPDATE #tbResult SET	
	[TIPO DE SOPORTE DE OXÍGENO] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 290
									AND EHREvCust.idElement = 13
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[CONSUMO DE OXÍGENO EN LITROS/MINUTO] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 290
									AND EHREvCust.idElement = 14
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[HORAS DE ADMINISTRACIÓN DE OXÍGENO AL DÍA] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 290
									AND EHREvCust.idElement = 15
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[FECHAS DE INICIO DE SOPORTE DE OXÍGENO] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 290
									AND EHREvCust.idElement = 16
									AND Eve.idPatient = #tbResult.idPatient 
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[EQUIPO PARA PRESIÓN POSITIVA] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 290
									AND EHREvCust.idElement = 17
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

--[TIEMPO REQUERIDO DE TRATAMIENTO]	
UPDATE #tbResult SET [FECHA INICIO VENTILACIÓN MÉCANICA CRÓNICA] = NULL

UPDATE #tbResult SET	
	[MODO DE VENTILACIÓN MÉCANICA] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 290
									AND EHREvCust.idElement = 12
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

UPDATE #tbResult SET	
	[DESCRIPCIÓN OTRO MODO DE VENTILACIÓN MÉCANICA] = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
										INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 290
									AND EHREvCust.idElement = 18
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

--[MODO VENTILATORIO]
--[MODALIDAD VENTILATORIA]
--[DESCRIPCION MODALIDAD VENTILATORIA]
--[PEEP]
--[PEEP ALTO]
--[PEEP BAJO]
--[TIEMPO BAJO]
--[TIEMPO ALTO]
--[FRECUENCIA RESPIRATORIA TOTAL]
--[FRECUENCIA RESPIRATORIA PROGRAMADA]
--[FIO2]
--[TIPO DE VENTILADOR EN USO POR EL PACIENTE]
--[DESCRIPCIÓN OTRO TIPO DE VENTILADOR EN USO POR EL PACIENTE]


UPDATE #tbResult SET	
	OBSERVACIONES = (SELECT TOP 1
									EHREvCust.valueText
								FROM EHREventCustomActivities AS EHREvCust WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREvCust.idEvent = Eve.idEHREvent
								WHERE EHREvCust.idConfigActivity = 304
									AND EHREvCust.idElement = 1
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
/* CAMPO OK*/									order by Eve.actionRecordedDate DESC)
UPDATE #tbResult SET	
	[FECHA DE CONTROL MÉDICO] = (SELECT TOP 1 EV.actionRecordedDate 
								FROM EHREvents AS EV WITH(NOLOCK) 
								WHERE (EV.idAction = 1013
								OR EV.idAction= 1004
								OR EV.idAction= 1023)
								AND EV.idPatient = #tbResult.idPatient
								--AND Ev.idEncounter = #tbResult.idEncounter
								ORDER BY EV.idEHREvent DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	HTA = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2780
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE DIÁGNOSTICO HTA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2781
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

/* CAMPO OK*/
UPDATE #tbResult SET	
	[MEDICAMENTO 1  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2782
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[MEDICAMENTO 2  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2783
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
	/* CAMPO OK*/
UPDATE #tbResult SET	
	[MEDICAMENTO 3  QUE ESTA FORMULADO PARA MANEJO DE LA HTA ACTUAL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2784
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

	/* CAMPO OK*/
UPDATE #tbResult SET
[RIESGO DE LA HTA AL INGRESO] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2787
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

/* CAMPO OK*/
UPDATE #tbResult SET	
	DM = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2792
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[TIPO DE DIABETES] =	(SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2793
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE DIAGNÓSTICO DM] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2794
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

/* CAMPO OK*/
UPDATE #tbResult SET	
	[MEDICAMENTO 1 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2795
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

/* CAMPO OK*/
UPDATE #tbResult SET	
	[MEDICAMENTO 2 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2796
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

	/* CAMPO OK*/
UPDATE #tbResult SET	
	[MEDICAMENTO 3 QUE ESTA FORMULADO PARA EL MANEJO DE LA DM ACTUALM] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2797
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

	/* CAMPO OK*/
UPDATE #tbResult SET	
	[TIPO DE INSULINA ADMINISTRADA AL INGRESO DEL PROGRAMA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2798
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[TIPO DE INSULINA ADMINISTRADA DURANTE EL CONTROL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2799
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[RIESGO DE LA DM AL INGRESO] =  (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2801
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	ERC = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2804
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE DIAGNÓSTICO ERC] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2805
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[TFG INGRESO] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2813
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA TFG INGRESO] =  (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2814
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[TFG ACTUAL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2824
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)

/* CAMPO OK*/
UPDATE #tbResult SET 
	[FECHA TFG ACTUAL ] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2826
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[MICROALBUMINURIA AL INGRESO DEL PROGRAMA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2828
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA MICROALBUMINURIA AL INGRESO DEL PROGRAMA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2829
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[ESTADIO ACTUAL DE LA PATOLOGÌA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2830
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[CREATININA SUERO] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2819
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE CREATININA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2825
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	GLICEMIA = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2831
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE TOMA DE GLICEMIA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2832
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[HEMOGLOBINA GLICOSILADA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2815
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE TOMA DE HEMOGLOBINA GLICOSILADA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2816
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[COLESTEROL TOTAL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2833
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE TOMA DE COLESTEROL TOTAL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2834
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[COLESTEROL HDL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2835
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE TOMA DE COLESTEROL HDL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2836
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[COLESTEROL LDL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2837
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE TOMA DE COLESTEROL LDL] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2838
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	TRIGLICERIDOS = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2839
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE TOMA DE TRIGLICERIDOS] =(SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2840
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[MICRO ALBUMINURIA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2841
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE TOMA DE MICRO ALBUMINURIA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2842
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[RELACIÓN MICROALBUMINURIA/CREATINURIA] = (SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2843
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
/* CAMPO OK*/
UPDATE #tbResult SET	
	[FECHA DE RELACIÓN MICROALBUMINURIA/CREATINURIA] =	(SELECT TOP 1
									value
								FROM EHREventICUMonitoringMeditions AS EHREUCI WITH(NOLOCK)
									INNER JOIN EHREvents AS Eve WITH(NOLOCK) ON EHREUCI.idEHREvent = Eve.idEHREvent
								WHERE EHREUCI.idMonitoring = '1044'
									AND EHREUCI.idMedition = 2844
									AND Eve.idPatient = #tbResult.idPatient
									--AND Eve.idEncounter = #tbResult.idEncounter
									order by Eve.actionRecordedDate DESC)
