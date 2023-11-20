select f.*,u.idUser as idProfesional, u.Documento as DocumentoProfesional, u.Tipo_Documento TipoDocumentoProfesional
from fact_appointments_bookings f LEFT join TblDUsuarios u
on f.professional  = Nombres_Completos where appointment_start_date >= '2023-07-01'