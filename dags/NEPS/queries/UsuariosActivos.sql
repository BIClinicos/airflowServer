WITH fechas AS (
    SELECT FORMAT(DATEADD(MONTH, n.n, MinFecha), 'yyyy-MM-01') AS Fecha
    FROM (
        SELECT
            {last_week} AS MinFecha,
            MAX(ci.[Fecha Cita]) AS MaxFecha
        FROM tblHcitasinasistencia ci
        WHERE contrato_id = 8 AND plan_id = 10 AND ci.[Estado Cita] = 'Asistida'
    ) AS fecha_range
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.columns
    ) n
    WHERE DATEADD(MONTH, n.n, MinFecha) <= MaxFecha
),
pacientes AS (
    SELECT
        u.idUser,
        u.Documento
    FROM tblHcitasinasistencia ci
    INNER JOIN TblDusuarios u ON u.idUser = ci.Paciente_id
    WHERE contrato_id = 8 AND plan_id = 10 AND ci.[Estado Cita] = 'Asistida' AND idUser <> 14
    GROUP BY u.idUser, u.Documento
)
--insert into tmpUsuariosActivosNEPS
SELECT
    p.idUser,
    p.Documento,
    f.Fecha,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM tblHcitasinasistencia ci
            WHERE ci.Paciente_id = p.idUser AND ci.[Estado Cita] = 'Asistida' 
			AND ci.[Fecha Cita] between DATEADD(MONTH, -2, f.Fecha) AND EOMONTH(f.Fecha)
        ) THEN 1
        ELSE 0
    END AS activo
FROM pacientes p
CROSS JOIN fechas f
ORDER BY p.idUser, f.Fecha;