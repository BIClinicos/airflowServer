-- Active: 1692042194923@@goreplica.database.windows.net@1433@goMedisysCo_clinicos
-- ESTE CTE ES PARA FILTRAL TODO LO QUE NECESITAMOS PARA LOS EVENTOS DE TRATAMIENTOS, TENIENDO EN CUENTA LOS EVENTOS, CONTRATO, 
-- ENCOUNTER, UNIDOS CON LOS USUARIOS
with main as (

    select e.idEHREvent, 
    e.idAction, 
    GA.name Accion,
	e.idSpeciality, 
    e.actionRecordedDate, 
    ec.idOffice,
	u.documentNumber DocumentoPaciente, 
    uctd.name tipo_doc,
	CONCAT(u.givenName,' ', u.familyName) as NombrePaciente,
    concat(um.givenName,' ', um.familyName) NombreMedico, 
	e.idPractitioner,
    er.idPrincipalContract, ec.idUserPatient, er.idEncounter,
    ec.idDischargePatientOutcomeStatus, ec.idDischargeDestination, 
    c.name as Contrato, ec.dateDischarge
    from 
     EHREvents e
    INNER join encounterRecords er on er.idEncounter = e.idEncounter
    AND er.idPrincipalContract in (92,76) AND e.idAction in (127,373,614,776,1009,1066,1090,1092,
		271,419,681,813,1015,255,665,1069,1087,1093,
		126,367,613,770,1001,1068,1088,1094,
		124,256,429,611,666,822,1014,1067,1089,1095) and e.idPatient <> 14 
        AND e.actionRecordedDate BETWEEN '2023-07-01' AND {end_date}
    INNER join contracts c on c.idContract = er.idPrincipalContract
    INNER JOIN encounters ec on e.idEncounter = ec.idEncounter
    INNER JOIN dbo.generalActions GA on  GA.idAction = e.idAction
    INNER join users u on e.idPatient = u.idUser
    INNER JOIN users um on e.idPractitioner = um.idUser
    INNER JOIN userConfTypeDocuments uctd on uctd.idTypeDocument = u.idDocumentType
    -- 9181 DATOS, 14-07-2023
)
SELECT
    FORMAT(main.actionRecordedDate,'yyyy-MM') FechaCita,
	main.idUserPatient idPaciente,
    pr.name PaqueteEvento,

    -- bs.transactionOriginNumber,
    -- STRING_AGG(bs.saleNumber,'-') NumFactura,
    -- STRING_AGG(bsd.value, '-') ValorFactura,

    count(tfe.idEHREvent) TFEjecutadas,
	case when count(tfe.idEHREvent) > 0 then 
		STRING_AGG(concat(tfu.firstGivenName, ' ', tfu.familyName), '-')
	else NULL end ProfesionalTF,
    count(tle.idEHREvent) TLEjecutadas,
	case when count(tle.idEHREvent) > 0 then 
		STRING_AGG(concat(tlu.givenName, ' ', tlu.familyName),'-')
	else NULL end ProfesionalTL,
    count(toe.idEHREvent) TOEjecutadas,
	case when count(toe.idEHREvent) > 0 then 
		STRING_AGG(concat(tou.givenName, ' ', tou.familyName),'-')
	else NULL end ProfesionalTO,
    count(tre.idEHREvent) TREjecutadas,
	case when count(tre.idEHREvent) > 0 then 
		STRING_AGG(concat(tru.givenName, ' ', tru.familyName),'-')
	else NULL end ProfesionalTR
    -- Into #tempTratamientosEjecutadas
FROM
    main
	LEFT JOIN Authorizations a on a.idEncounter = main.idEncounter
    AND main.idUserPatient = a.idUserPatient
    LEFT JOIN products pr on pr.idProduct = a.idProduct

    -- Facturacion
    -- LEFT JOIN billSales bs on bs.idEncounter = main.idEncounter AND main.idPrincipalContract = bs.idContract AND bs.idUserPatient = main.idUserPatient
    -- LEFT JOIN billSaleDetails bsd on bsd.idSale = bs.idSale 

    LEFT JOIN main tfe on main.idEHREvent = tfe.idEHREvent AND tfe.idAction in (127,373,614,776,1009,1066,1090,1092)
    LEFT JOIN users tfu on tfe.idPractitioner = tfu.idUser
    LEFT JOIN main tle on main.idEHREvent = tle.idEHREvent AND tle.idAction in (271,419,681,813,1015,255,665,1069,1087,1093)
    LEFT JOIN users tlu on tle.idPractitioner = tlu.idUser
    LEFT JOIN main toe on main.idEHREvent = toe.idEHREvent AND toe.idAction in (126,367,613,770,1001,1068,1088,1094)
    LEFT JOIN users tou on toe.idPractitioner = tou.idUser
    LEFT JOIN main tre on main.idEHREvent = tre.idEHREvent AND tre.idAction in (124,256,429,611,666,822,1014,1067,1089,1095)
    LEFT JOIN users tru on tre.idPractitioner = tru.idUser --OR tu.idUser = tle.idPractitioner OR tu.idUser = toe.idPractitioner OR tu.idUser = tre.idPractitioner

GROUP BY
    FORMAT(main.actionRecordedDate,'yyyy-MM'),
	main.idUserPatient,
    pr.name

ORDER BY
    main.idUserPatient