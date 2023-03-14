SELECT idUser,Profesional,Tipo_Documento,Documento,Especialidad FROM (SELECT DISTINCT
USR.idUser,CONCAT(USR.firstGivenName,' ',USR.secondGiveName ,' ', USR.firstFamilyName,' ',USR.secondFamilyName) as Profesional,USRCD.code as Tipo_Documento,USR.documentNumber as Documento, GS.name AS "Especialidad",
ROW_NUMBER() over( partition by USR.documentNumber order by GS.name desc) as Indicador
FROM dbo.users USR WITH (NOLOCK) 
INNER JOIN dbo.userConfTypeDocuments USRCD WITH (NOLOCK) ON USR.idDocumentType=USRCD.idTypeDocument
LEFT JOIN dbo.userSystemSpecialities USS WITH (NOLOCK) ON USR.idUser=USS.idUser 
LEFT JOIN (SELECT DISTINCT idUser, idSpeciality FROM dbo.userSystemSpecialities USS WITH (NOLOCK) WHERE USS.isPrincipal=1 AND USS.isActive=1) AS Specialities ON USS.idUser=Specialities.idUser AND USS.idSpeciality=Specialities.idSpeciality
LEFT JOIN dbo.generalSpecialties GS WITH (NOLOCK) ON  Specialities.idSpeciality=GS.idSpecialty
WHERE USRCD.code <> 'NIT' ) AS TODO
WHERE TODO.Indicador=%(indicador)s