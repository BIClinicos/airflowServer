with dxv as (
	SELECT 
		FORMAT(FechaActividad, 'yyyy-MM') date_control
		,ci.IdPaciente
		,d.diagnosticDescription
		,d.code
		,ed.isPrincipal
	FROM TblHGomedisysConsultation ci 
		INNER JOIN tblDeventosDiagnosticos ed on ci.idEncounter = ed.idEncounter AND ci.IdPaciente = ed.idUserPatient
		AND ci.contrato_id = 8 and ci.plan_id = 10
		AND	CONVERT(date,ci.FechaActividad) BETWEEN {last_week} and convert(date, getdate())
		INNER JOIN dimDiagnostics d on d.idDiagnostic = ed.idDiagnostic AND 
				d.code in ('G473','J459','J451','J458','J46X','J450','B342','U089','U099','I829','I269','I260','I828','I748','I749',
				'J449','J441','J448','J440','I270','I272','I500','I255','I509','I110','I429','I428','I438','I420','R060','J61X','J628',
				'J60X','J65X','J64X','J189','J129','J128','J159','J188','J180','J158','J679','J678','J680','R931','I279','R91X','J47X',
				'J42X','J410','J439','I278','J82X','I289','J209','S202','J984','J960','R848','J81X','E849','Q210','C348','I379','A159',
				'A158','A188','B909','A161','A153','A168','J849','J841','J848','J982','J969')
		INNER JOIN tblHComorbilidad c on d.code = c.cie10
		--INNER JOIN tmpConsultationGomedisysNEPS cg on ci.idEncuentro = cg.idEncounter
	GROUP BY 
		FORMAT(FechaActividad, 'yyyy-MM'),
		ci.IdPaciente
		,d.diagnosticDescription
		,d.code
		,ed.isPrincipal
)
SELECT
    date_control,
    IdPaciente as idUser,
    COALESCE(MAX(CASE WHEN isPrincipal = 1 THEN code END), MAX(code)) AS code,
    count(distinct diagnosticDescription) as totalDx
FROM dxv
GROUP BY date_control, IdPaciente;