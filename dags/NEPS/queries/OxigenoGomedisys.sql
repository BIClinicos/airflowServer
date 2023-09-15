SELECT
  a.idPatient idUser,
  u.documentNumber Documento,
  b.value AS Horas_Oxigeno,
  c.value AS Dispositivo,
	format(a.actionRecordedDate, 'yyyy-MM') date_control
FROM
   dbo.EHREvents a 
    inner join EHREventICUMonitoringMeditions b  on  a.idEHREvent = b.idEHREvent and 
	    b.idMonitoring = 1043 AND b.idMedition in (2822) AND a.isActive = 1
        AND convert(date, a.actionRecordedDate) between {last_week} AND convert(date, GETDATE())
	INNER JOIN encounterRecords e on a.idEncounter = e.idEncounter and idPrincipalContract = 8 and idPrincipalPlan = 10
    INNER JOIN users u on a.idPatient = u.idUser
	LEFT join EHREventICUMonitoringMeditions c  on  c.idEHREvent = b.idEHREvent and 
	    c.idMonitoring = 1043 AND c.idMedition in (2777)

GROUP BY 
	a.idPatient,
    u.documentNumber,
    b.value,
	c.value,
    format(a.actionRecordedDate, 'yyyy-MM')

ORDER BY a.idPatient,format(a.actionRecordedDate, 'yyyy-MM') desc