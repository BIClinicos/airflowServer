-- ESTE CTE ES PARA FILTRAL TODO LO QUE NECESITAMOS PARA LOS EVENTOS DE TRATAMIENTOS, TENIENDO EN CUENTA LOS EVENTOS, CONTRATO, 
-- ENCOUNTER, UNIDOS CON LOS USUARIOS

with main as (
    select e.idEHREvent,
        e.idAction,
        GA.name as Accion,
        e.idSpeciality,
        e.actionRecordedDate,
        ec.idOffice,
        u.documentNumber DocumentoPaciente,
        uctd.name tipo_doc,
        CONCAT(u.givenName, ' ', u.familyName) as NombrePaciente,
        concat(um.givenName, ' ', um.familyName) NombreMedico,
        e.idPractitioner,
        er.idPrincipalContract,
        ec.idUserPatient,
        er.idEncounter,
        ec.idDischargePatientOutcomeStatus,
        ec.idDischargeDestination,
        c.name as Contrato,
        ec.dateDischarge
    from EHREvents e
        INNER join encounterRecords er on er.idEncounter = e.idEncounter
        AND er.idPrincipalContract in (92,76)
        and e.idPatient <> 14
        AND e.idAction in (932, 933, 965, 984, 1004, 1013, 1023)
        AND e.actionRecordedDate BETWEEN '2023-07-01' AND {end_date}
        INNER join contracts c on c.idContract = er.idPrincipalContract
        INNER JOIN encounters ec on e.idEncounter = ec.idEncounter
        INNER JOIN dbo.generalActions GA on GA.idAction = e.idAction
        INNER join users u on e.idPatient = u.idUser
        INNER JOIN users um on e.idPractitioner = um.idUser
        INNER JOIN userConfTypeDocuments uctd on uctd.idTypeDocument = u.idDocumentType -- 9181 DATOS, 14-07-2023
),
factMedSca as (
    select main.idEHREvent,
        EMS.idEvaluation,
        main.idAction,
        GS.idSpecialty,
        CSV.[idScale],
        sum(EMSQ.[value]) value
    from main
        INNER JOIN dbo.generalSpecialties GS ON GS.idSpecialty = main.idSpeciality
        INNER JOIN dbo.EHREventMedicalScales EMS ON main.idEHREvent = EMS.idEHREvent
        INNER JOIN dbo.EHRConfScaleValorations CSV ON CSV.idScale = EMS.idScale
        AND EMS.idEvaluation = CSV.[idRecord]
        and CSV.[idScale] = 11
        INNER JOIN dbo.EHREventMedicalScaleQuestions EMSQ ON main.idEHREvent = EMSQ.idEHREvent
        and EMS.idScale = EMSQ.idScale
        LEFT JOIN dbo.EHREventMedicalDiagnostics EMD ON main.idEHREvent = EMD.idEHREvent
        and EMD.isPrincipal = 1
    group by EMS.idEvaluation,
        main.idEHREvent,
        main.idAction,
        GS.idSpecialty,
        CSV.idScale
)

SELECT main.idEHREvent,
    convert(date, main.actionRecordedDate) FechaCita,
    main.idAction,
    main.Accion,
    -- PACIENTE
    main.idUserPatient idPaciente,
    main.DocumentoPaciente DocumentoPaciente,
    main.tipo_doc Tipo_Documento,
    main.NombrePaciente as NombrePaciente,
    up.deathDate FechaMuerte,
    gp.name Ciudad,
    case
        when gpn.name is not null
        AND CHARINDEX('-', gpn.name) > 0 then TRIM(LEFT(gpn.name, CHARINDEX('-', gpn.name) - 1))
        else gpn.name
    end AS BarrioPaciente,
    case
        when gpn.name is not null
        AND CHARINDEX('-', gpn.name) > 0 then TRIM(
            LTRIM(
                RIGHT(
                    gpn.name,
                    LEN(gpn.name) - CHARINDEX('-', gpn.name)
                )
            )
        )
        else gpn.name
    end AS LocalidadPaciente,
    up.homeAddress DireccionPaciente,
    up.telecom CelularPaciente,
    up.phoneHome TelefonoPaciente,
    co.name Sede,
    -- CONTRATO
    main.idEncounter,
    main.idPrincipalContract idContrato,
    main.Contrato Contrato,
    ced.name DestinoEgresoAsistencial,
    gld.value DestinoEgresoAdministrativo,
    ecpos.name DetalleDestinoEgresoAdministrativo,
    main.dateDischarge FechaEgresoAdministrativo,
    -- ORDENAMIENTO
    main.idPractitioner idProfesional,
    main.NombreMedico NombreProfesional,
    emd.idDiagnostic idDiagnostico,
    d.code CodigoDiagnosticoPrincipal,
    d.name DiagnosticoPrincipal,
    -- tf.[value], tl.[value], tor.[value], tr.[value], tro.[value]
    -- AUTORIZACION
    a.idAuthorization idAutorizacion,
    pr.name PaqueteEvento,
    pr.legalCode CUPS,
    ar.authorizationCode Autorizacion,
    a.quantityAuthorized CantidadServiciosAutorizados,
    ar.authorizationDate VigenciaAutorizacion,
    ar.authorizedValidSince FechaInicio,
    ar.authorizedValidUntil FechaFin,
    -- MONITOREOS
    tf.value TFOrdenadas,
    tl.value TLOrdenadas,
    too.value TOOrdenadas,
    tr.value TROrdenadas,
    tro.value TRPrioritariasOrdenadas,
    fms.value Barthel
FROM main
    LEFT JOIN Authorizations a on a.idEncounter = main.idEncounter
    AND main.idUserPatient = a.idUserPatient
    LEFT JOIN products pr on pr.idProduct = a.idProduct
    LEFT JOIN AuthorizationResponses ar on ar.idAuthorization = a.idAuthorization
    LEFT JOIN EHREventMedicalDiagnostics emd on emd.idEHREvent = main.idEHREvent
    and emd.isPrincipal = 1
    LEFT JOIN diagnostics d on d.idDiagnostic = emd.idDiagnostic
    LEFT JOIN encounterConfPatientOutcomeStatus ecpos on ecpos.idPatientOutcomeStatus = main.idDischargePatientOutcomeStatus
    LEFT JOIN generalListDetails gld on gld.idListDetail = main.idDischargeDestination
    LEFT JOIN userPeople up on up.idUser = main.idUserPatient
    LEFT JOIN companyOffices co on co.idOffice = main.idOffice
    LEFT JOIN generalPoliticalDivisions gp on up.idHomePlacePoliticalDivision = gp.idPoliticalDivision
    LEFT JOIN generalPoliticalDivisions gpn on up.idHomePoliticalDivisionNeighborhood = gpn.idPoliticalDivision
    LEFT JOIN EHREventMedicalCarePlan emcp on emcp.idEHREvent = main.idEHREvent
    LEFT JOIN EHRConfEventDestination ced on ced.idEventDestination = emcp.idEventDestination
    LEFT JOIN EHREventICUMonitoringMeditions tf on main.idEHREvent = tf.idEHREvent
    AND tf.idMonitoring = 1036
    AND tf.idMedition = 2674
    LEFT JOIN EHREventICUMonitoringMeditions tl on main.idEHREvent = tl.idEHREvent
    AND tl.idMonitoring = 1036
    AND tl.idMedition = 2678
    LEFT JOIN EHREventICUMonitoringMeditions too on main.idEHREvent = too.idEHREvent
    AND too.idMonitoring = 1036
    AND too.idMedition = 2670
    LEFT JOIN EHREventICUMonitoringMeditions tr on main.idEHREvent = tr.idEHREvent
    AND tr.idMonitoring = 1036
    AND tr.idMedition = 2662
    LEFT JOIN EHREventICUMonitoringMeditions tro on main.idEHREvent = tro.idEHREvent
    AND tro.idMonitoring = 1036
    AND tro.idMedition = 2666
    LEFT JOIN factMedSca fms on fms.idEHREvent = main.idEHREvent
WHERE tf.[value] is not null
    or tl.[value] is not null
    or too.[value] is not null
    or tr.[value] is not null
    or tro.[value] is not null -- 1013
GROUP BY main.idEHREvent,
    convert(date, main.actionRecordedDate),
    main.idAction,
    main.Accion,
    -- PACIENTE
    main.idUserPatient,
    main.DocumentoPaciente,
    main.tipo_doc,
    main.NombrePaciente,
    up.deathDate,
    gp.name,
    case
        when gpn.name is not null
        AND CHARINDEX('-', gpn.name) > 0 then TRIM(LEFT(gpn.name, CHARINDEX('-', gpn.name) - 1))
        else gpn.name
    end,
    case
        when gpn.name is not null
        AND CHARINDEX('-', gpn.name) > 0 then TRIM(
            LTRIM(
                RIGHT(
                    gpn.name,
                    LEN(gpn.name) - CHARINDEX('-', gpn.name)
                )
            )
        )
        else gpn.name
    end,
    up.homeAddress,
    up.telecom,
    up.phoneHome,
    co.name,
    -- CONTRATO
    main.idEncounter,
    main.idPrincipalContract,
    main.Contrato,
    ced.name,
    gld.value,
    ecpos.name,
    main.dateDischarge,
    -- ORDENAMIENTO
    main.idPractitioner,
    main.NombreMedico,
    emd.idDiagnostic,
    d.code,
    d.name,
    -- AUTORIZACION
    a.idAuthorization,
    pr.name,
    pr.legalCode,
    ar.authorizationCode,
    a.quantityAuthorized,
    ar.authorizationDate,
    ar.authorizedValidSince,
    ar.authorizedValidUntil,
    tf.value,
    tl.value,
    too.value,
    tr.value,
    tro.value,
    fms.value