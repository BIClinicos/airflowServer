SELECT
		EV.idEHREvent as IdEvento,
		ENC.idEncounter as IdEncounter,
		ga.idAction as idAction,
		EV.actionRecordedDate AS FechaActividad,
		EV.isActive as Asistida,
		USR2.documentNumber AS DocumentoPaciente,
		USR2.idUser AS IdPaciente,
		EVMCP.carePlan AS PlanTratamiento,
		ENC.dateStart as Fecha_Atencion,
		ENCR.idPrincipalContract AS Contrato_Id,
		ENCR.idPrincipalPlan as Plan_Id,
		CONT.name AS Contrato,
		HR.name AS Regimen,
		GS.name as Especialidades,
		ga.name Action,
		EVMD.presentIllness AS EnfermedadActual
		, ISNULL(ISNULL(EVCA.valueText,EVMC.medicalConcept),EVSW.socialDiagnosis) AS Analisis 
	FROM
		dbo.encounters AS ENC
		INNER JOIN dbo.encounterRecords AS ENCR WITH(NOLOCK) ON ENC.idEncounter = ENCR.idEncounter --Contrato
			AND ENCR.idPrincipalContract IN (44, 45, 46, 47, 8) --Código del contrato de Compensar-Domiciliaria y Nueva EPS
		INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EV.idEncounter = ENC.idEncounter --Evento
		AND CONVERT(date, EV.actionRecordedDate) BETWEEN {last_week} AND convert(date, getdate()) and ev.idPatient not in (14)

		

		INNER JOIN dbo.generalActions ga on EV.idAction = ga.idAction
		INNER JOIN dbo.generalSpecialties AS GS WITH(NOLOCK) ON EV.idSpeciality = GS.idSpecialty --Especialidad
		INNER JOIN dbo.users AS USR2 ON EV.idPatient = USR2.idUser --DocumentoPaciente
		INNER JOIN dbo.encounterConfClass AS ENCC WITH(NOLOCK) ON ENC.idEncounterClass = ENCC.idEncounterClass --TipoIngreso
		INNER JOIN dbo.contracts AS CONT WITH(NOLOCK) ON ENCR.idPrincipalContract = CONT.idContract --Contrato
		INNER JOIN dbo.contractPlans AS CONTP WITH(NOLOCK) ON CONT.idcontract = CONTP.idcontract AND ENCR.idPrincipalPlan = CONTP.idplan --Contrato Plan
		INNER JOIN dbo.healthRegimes AS HR WITH(NOLOCK) ON CONTP.idHealthRegime = HR.idHealthRegime --Regimen
		LEFT JOIN dbo.EHREventMedicalCarePlan AS EVMCP WITH(NOLOCK) ON EV.idEHREvent = EVMCP.idEHREvent --PlanTratamiento

		LEFT JOIN dbo.EHREventMedicalDescription AS EVMD ON EV.idEHREvent = EVMD.idEHREvent

		LEFT JOIN dbo.EHREventCustomActivities AS EVCA ON EV.idEHREvent = EVCA.idEvent		--Analisis
            AND ( (EVCA.idConfigActivity IN(70,53,146,110)	AND EVCA.idElement=1) OR (EVCA.idConfigActivity IN(293)	AND EVCA.idElement=5))
		LEFT JOIN EHREventMedicalConcept EVMC on EV.idEHREvent = EVMC.idEHREvent
		LEFT JOIN EHREventSocialWork EVSW on EVSW.idEHREvent = EV.idEHREvent

	GROUP BY
		EV.idEHREvent,
		ENC.idEncounter,
		ga.idAction,
		EV.actionRecordedDate,
		EV.isActive,
		USR2.documentNumber,
		USR2.idUser,
		EVMCP.carePlan,
		ENC.dateStart,
		ENCR.idPrincipalContract,
		ENCR.idPrincipalPlan,
		CONT.name,
		HR.name,
		GS.name,
		ga.name,
		EVMD.presentIllness
		, ISNULL(ISNULL(EVCA.valueText,EVMC.medicalConcept),EVSW.socialDiagnosis)