select
    u.idUser
    ,u.Documento
    ,format(ci.FechaActividad, 'yyyy-MM') date_control
    ,Recomendaciones
from 
    TblDusuarios u 
    inner join TblHGomedisysConsultation ci on u.idUser = ci.IdPaciente AND ci.contrato_id = 8 and ci.plan_id = 10
        AND convert(date, ci.FechaActividad) between {last_week} AND convert(date, GETDATE())
    INNER JOIN tblDeventosDiagnosticos ed on ci.IdEncounter = ed.idEncounter 
    INNER JOIN dimDiagnostics d on d.idDiagnostic = ed.idDiagnostic AND 
            d.idDiagnostic in (226,231,232,234,240,254,669,904,1087,3218,3742,3987,4031,4034,4182,8189,
            8777,9207,9251,9255,9256,9257,9259,9260,9264,9299,9315,9323,9324,9328,9362,9364,9474,9475,
            9504,9505,9587,9588,9599,9600,9608,9611,9612,9622,9675,9678,9683,9684,9685,9686,9687,9688,
            9689,9690,9691,9692,9693,9694,9695,9697,9705,9706,9719,9720,9721,9739,9740,9742,9743,9744,
            9772,9774,9777,9779,12668,12669,12670)
    INNER JOIN tblHComorbilidad c on d.code = c.cie10
    INNER JOIN TblHFormulacionMedicamentos fm on ci.IdEncounter = fm.id_cita
    where recomendaciones IS NOT NULL 
	and (lower(recomendaciones) like '%o2%' or 
                                    lower(recomendaciones) like '% ox%' or 
                                    lower(recomendaciones) like 'ox%' or 
                                    lower(recomendaciones) like '%[0-9]_l%[i-tro-s]%[^0-9]%[0-9]_h[o-s]%')

GROUP BY
u.idUser
,u.Documento
,Recomendaciones
,format(ci.FechaActividad, 'yyyy-MM')
ORDER BY u.idUser; 