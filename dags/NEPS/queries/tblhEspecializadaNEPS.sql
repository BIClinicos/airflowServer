
select
		u.idUser
        ,u.Documento
        ,u.fecha_nacimiento
        ,CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,[Fecha Cita])/8766.0,0)) Edad
        ,u.[Género]
        ,format([Fecha Cita], 'yyyy-MM-01') date_control
		,u.fecha_registro
        ,sum(coalesce(c.puntaje, 0))+sum(coalesce(
            case 
            when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,[Fecha Cita])/8766.0,0)) < 50 then 0
            when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,[Fecha Cita])/8766.0,0)) between 50 and 59 then 1
            when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,[Fecha Cita])/8766.0,0)) between 60 and 69 then 2
            when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,[Fecha Cita])/8766.0,0)) between 70 and 79 then 3
            when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,[Fecha Cita])/8766.0,0)) between 80 and 89 then 4
            when CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,[Fecha Cita])/8766.0,0)) >=90 then 5
            else 0 end, 0)) Indice_Comorbilidad
        ,tmpDiag.code as Diagnostico
		,tmpDiag.totalDx total_diagnosticos
        ,ox.Horas_Oxigeno
		,ox.Dispositivo
        ,ox.CPAP_BPAP
        ,up.valorTexto [EpocGOLD]
from 
		tblHcitasinasistencia ci 
	INNER JOIN TblDUsuarios u on u.idUser = ci.paciente_id 
    AND contrato_id = 8 and plan_id = 10 and ci.[Estado Cita] = 'Asistida'   
	LEFT JOIN tblDeventosDiagnosticos ed on ci.idEncuentro = ed.idEncounter 
    LEFT JOIN dimDiagnostics d on d.idDiagnostic = ed.idDiagnostic 
    LEFT JOIN tblHComorbilidad c on d.code = c.cie10
    LEFT JOIN tblHDiagnosticNEPS tmpDiag on u.idUser = tmpDiag.idUser and FORMAT(tmpDiag.date_control,'yyyy-MM-01') = FORMAT(ci.[Fecha Cita], 'yyyy-MM-01')
    LEFT JOIN tblhFormulacionOxigeno ox on u.idUser = ox.IdUser and FORMAT(ox.date_control, 'yyyy-MM-01') = FORMAT(ci.[Fecha Cita], 'yyyy-MM-01')
    LEFT JOIN TblHActividadesPersonalizadas up on up.idEncuentro = ci.idEncuentro
    Where ci.[Fecha Cita] between {last_week} and CONVERT(date, getdate())  
GROUP BY 
u.idUser
,u.Documento
,u.fecha_nacimiento
,CONVERT(int,ROUND(DATEDIFF(hour,u.fecha_nacimiento,[Fecha Cita])/8766.0,0))
,u.[Género]
,format([Fecha Cita], 'yyyy-MM-01')
,u.fecha_registro
,tmpDiag.code
,tmpDiag.totalDx
,ox.Horas_Oxigeno
,ox.Dispositivo
,ox.CPAP_BPAP
,up.valorTexto
ORDER BY u.Documento;