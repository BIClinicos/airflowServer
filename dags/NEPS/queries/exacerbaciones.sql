SELECT
	  a.idPatient idUser,
	  u.documentNumber Documento,
	  case when try_convert(int, b.valueText) is not null and try_convert(int, b.valueText) > 0 then 1 else 0 end exacerbacion,
	format(a.actionRecordedDate, 'yyyy-MM-01') date_control
FROM
   dbo.EHREvents a 
   INNER JOIN users u on a.idPatient = u.idUser
   INNER JOIN encounterRecords e on a.idEncounter = e.idEncounter and idPrincipalContract = 8 and idPrincipalPlan = 10
    inner join EHREventCustomActivities b  on  a.idEHREvent = b.idEvent and 
	    b.idConfigActivity = 168 AND b.idElement in (9) 
        AND convert(date, a.actionRecordedDate) between {last_week} AND convert(date, GETDATE())
GROUP BY 
	a.idPatient,
    u.documentNumber,
    case when try_convert(int, b.valueText) is not null and try_convert(int, b.valueText) > 0 then 1 else 0 end,
    format(a.actionRecordedDate, 'yyyy-MM-01')

ORDER BY a.idPatient,format(a.actionRecordedDate, 'yyyy-MM-01') desc