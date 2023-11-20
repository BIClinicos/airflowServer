
with cte as (
	select
			u.idUser
			,u.Documento
			,u.fecha_nacimiento
			,CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,FechaActividad)/8766.0,0)) Edad
			,u.[Género]
			,format(FechaActividad, 'yyyy-MM-01') date_control
			,u.fecha_registro
			,sum(coalesce(c.puntaje, 0))+coalesce(
				case 
				when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,FechaActividad)/8766.0,0)) < 50 then 0
				when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,FechaActividad)/8766.0,0)) between 50 and 59 then 1
				when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,FechaActividad)/8766.0,0)) between 60 and 69 then 2
				when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,FechaActividad)/8766.0,0)) between 70 and 79 then 3
				when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,FechaActividad)/8766.0,0)) between 80 and 89 then 4
				when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,FechaActividad)/8766.0,0)) >=90 then 5
				else 0 end, 0) Indice_Comorbilidad
			,tmpDiag.code as Diagnostico
			,tmpDiag.totalDx total_diagnosticos
			,ox.Horas_Oxigeno
			,ox.Dispositivo
			,ox.CPAP_BPAP
			,up.valorTexto [EpocGOLD]
			,cg.Analisis
			,cg.EnfermedadActual
			,cg.PlanTratamiento
			,case when cg.idAction = 330 then cg.FechaActividad end as fecha_rehabilitacion_ingreso
			,case when cg.idAction = 370 then cg.FechaActividad end as fecha_rehabilitacion_egreso
	from 
		TblHGomedisysConsultation cg
		INNER JOIN TblDUsuarios u on u.idUser = cg.IdPaciente 
		AND contrato_id = 8 and plan_id = 10 
		AND cg.idAction not in (347,399,343,987,986,70,350
						,394,90,430,393,1051,382,91,464,94,459,95,380,98,1097,286)
		LEFT JOIN tblDeventosDiagnosticos ed on cg.idEncounter = ed.idEncounter 
			and convert(date, ed.actionRecordedDate) = CONVERT(date,cg.FechaActividad)
		LEFT JOIN dimDiagnostics d on d.idDiagnostic = ed.idDiagnostic 
		LEFT JOIN tblHComorbilidad c on d.code = c.cie10
		LEFT JOIN tblHDiagnosticNEPS tmpDiag on u.idUser = tmpDiag.idUser 
			and FORMAT(tmpDiag.date_control,'yyyy-MM-01') = FORMAT(cg.FechaActividad, 'yyyy-MM-01')
		LEFT JOIN tblhFormulacionOxigeno ox on u.idUser = ox.IdUser 
			and FORMAT(ox.date_control, 'yyyy-MM-01') = FORMAT(cg.FechaActividad, 'yyyy-MM-01')
		LEFT JOIN TblHActividadesPersonalizadas up on up.idEncuentro = cg.idEncounter

		Where cg.FechaActividad between {last_week} and CONVERT(date, getdate())  
	GROUP BY 
	u.idUser
	,u.Documento
	,u.fecha_nacimiento
	,CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,cg.FechaActividad)/8766.0,0))
	,u.[Género]
	,format(cg.FechaActividad, 'yyyy-MM-01')
	,u.fecha_registro
	,tmpDiag.code
	,tmpDiag.totalDx
	,ox.Horas_Oxigeno
	,ox.Dispositivo
	,ox.CPAP_BPAP
	,up.valorTexto
	,cg.Analisis
	,cg.EnfermedadActual
	,cg.PlanTratamiento
	,cg.FechaActividad
	,case when cg.idAction = 330 then cg.FechaActividad end
	,case when cg.idAction = 370 then cg.FechaActividad end
)
SELECT idUser
		,Documento
		,fecha_nacimiento
		, Edad
		,[Género]
		,date_control
		,fecha_registro 
		,max(Indice_Comorbilidad) Indice_Comorbilidad
		,Diagnostico
		,total_diagnosticos
		,Horas_Oxigeno
		,Dispositivo
		,CPAP_BPAP
		,max([EpocGOLD]) EpocGOLD
		,max(Analisis) Analisis
		,max(EnfermedadActual) EnfermedadActual
		,max(PlanTratamiento) PlanTratamiento
		,min(fecha_rehabilitacion_ingreso) fecha_rehabilitacion_ingreso
		,min(fecha_rehabilitacion_egreso) fecha_rehabilitacion_egreso
FROM cte
Group by 
idUser
,Documento
,fecha_nacimiento
, Edad
,[Género]
,date_control
,fecha_registro 
,Diagnostico
,total_diagnosticos
,Horas_Oxigeno
,Dispositivo
,CPAP_BPAP


order by idUser