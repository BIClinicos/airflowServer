WITH fechas AS (
    SELECT FORMAT(DATEADD(MONTH, n.n, MinFecha), 'yyyy-MM-01') AS Fecha
    FROM (
        SELECT
            {last_week} AS MinFecha,
            MAX(ci.FechaActividad) AS MaxFecha
        FROM TblHGomedisysConsultation ci
        WHERE contrato_id = 8 AND plan_id = 10
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
    FROM TblHGomedisysConsultation ci
    INNER JOIN TblDusuarios u ON u.idUser = ci.IdPaciente
    WHERE contrato_id = 8 AND plan_id = 10 AND idUser <> 14
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
            FROM TblHGomedisysConsultation ci
            WHERE ci.IdPaciente = p.idUser AND idAction not in (347,399,343,987,986,70,350
					,394,90,430,393,1051,382,91,464,94,459,95,380,98,1097,286)
			AND ci.FechaActividad between DATEADD(MONTH, -2, f.Fecha) AND EOMONTH(f.Fecha)
        ) THEN 1
        ELSE 0
    END AS activo
FROM pacientes p
CROSS JOIN fechas f
ORDER BY p.idUser, f.Fecha;