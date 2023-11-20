with cte as (
    select  id DocumentoProfesional,fecha_ingreso_clinicos,valor_contrato,case when unidad_de_negocio like '%domiciliaria%' then 'Valor' else 'Minutos' end Tipo_Pago_BI_OPS, ROW_NUMBER() OVER (PARTITION BY id ORDER BY fecha_ingreso_clinicos DESC) AS RowNum  from THM_VYD_OPS
    where id in ({professional})

)
select  DocumentoProfesional,Tipo_Pago_BI_OPS,case when Tipo_Pago_BI_OPS = 'Minutos' then valor_contrato/60 else valor_contrato end Ulima_Tarifa_BI_OPS
from cte where RowNum = 1;