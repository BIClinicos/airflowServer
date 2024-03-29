with dxv as (
	SELECT 
		FORMAT([Fecha cita], 'yyyy-MM') date_control
		,ci.paciente_id
		,d.diagnosticDescription
	FROM tblHcitasinasistencia ci 
		INNER JOIN tblDeventosDiagnosticos ed on ci.idEncuentro = ed.idEncounter AND ci.paciente_id = ed.idUserPatient
		AND ci.contrato_id = 8 and ci.plan_id = 10  and ci.[Estado Cita] = 'Asistida'
		INNER JOIN dimDiagnostics d on d.idDiagnostic = ed.idDiagnostic AND 
				d.code in ('G473','J459','J451','J458','J46X','J450','B342','U089','U099','I829','I269','I260','I828','I748','I749',
				'J449','J441','J448','J440','I270','I272','I500','I255','I509','I110','I429','I428','I438','I420','R060','J61X','J628',
				'J60X','J65X','J64X','J189','J129','J128','J159','J188','J180','J158','J679','J678','J680','R931','I279','R91X','J47X',
				'J42X','J410','J439','I278','J82X','I289','J209','S202','J984','J960','R848','J81X','E849','Q210','C348','I379','A159',
				'A158','A188','B909','A161','A153','A168','J849','J841','J848','J982','J969')
		INNER JOIN tblHComorbilidad c on d.code = c.cie10
		--INNER JOIN tmpConsultationGomedisysNEPS cg on ci.idEncuentro = cg.idEncounter
	WHERE RESPIRATORIOS = 'Respiratorios'
	GROUP BY 
		FORMAT([Fecha cita], 'yyyy-MM'),
		ci.paciente_id
		,d.diagnosticDescription
)
insert into tmpDiagnosticosNEPS
SELECT
    date_control, paciente_id, COUNT(1) AS [# Of dx], STRING_AGG(diagnosticDescription,'~') AS dx
FROM dxv
GROUP BY date_control, paciente_id
ORDER BY paciente_id;


select * from tmpDiagnosticosNEPS a inner join tbldUsuarios b on a.paciente_id = b.idUser order by b.Documento;