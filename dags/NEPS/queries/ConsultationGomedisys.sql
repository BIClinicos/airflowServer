 SELECT
            EV.idEHREvent as IdEvento,
            ENC.idEncounter as IdEncounter,
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
            GS.name as Especialidades
        FROM
            dbo.encounters AS ENC
            INNER JOIN dbo.encounterRecords AS ENCR WITH(NOLOCK) ON ENC.idEncounter = ENCR.idEncounter --Contrato
                AND ENCR.idPrincipalContract IN (44, 45, 46, 47, 8) --Código del contrato de Compensar-Domiciliaria y Nueva EPS
            INNER JOIN dbo.EHREvents AS EV WITH(NOLOCK) ON EV.idEncounter = ENC.idEncounter --Evento
            AND CONVERT(date, EV.actionRecordedDate) BETWEEN {last_week} AND convert(date, getdate()) and ev.idPatient not in (14)
            INNER JOIN dbo.generalSpecialties AS GS WITH(NOLOCK) ON EV.idSpeciality = GS.idSpecialty --Especialidad
            INNER JOIN dbo.users AS USR2 ON EV.idPatient = USR2.idUser --DocumentoPaciente
            INNER JOIN dbo.encounterConfClass AS ENCC WITH(NOLOCK) ON ENC.idEncounterClass = ENCC.idEncounterClass --TipoIngreso
            INNER JOIN dbo.contracts AS CONT WITH(NOLOCK) ON ENCR.idPrincipalContract = CONT.idContract --Contrato
            INNER JOIN dbo.contractPlans AS CONTP WITH(NOLOCK) ON CONT.idcontract = CONTP.idcontract AND ENCR.idPrincipalPlan = CONTP.idplan --Contrato Plan
            INNER JOIN dbo.healthRegimes AS HR WITH(NOLOCK) ON CONTP.idHealthRegime = HR.idHealthRegime --Regimen
            LEFT JOIN dbo.EHREventMedicalCarePlan AS EVMCP WITH(NOLOCK) ON EV.idEHREvent = EVMCP.idEHREvent --PlanTratamiento
        GROUP BY
            EV.idEHREvent,
            ENC.idEncounter,
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
            GS.name
        ORDER BY
            USR2.idUser,
            EV.actionRecordedDate;