import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,load_df_to_sql

#  Se nombran las variables a utilizar en el dag

db_table = "TblDUsuarios"
db_tmp_table = "TmpUsuarios"
dag_name = 'dag_' + db_table

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def get_data_users():

    query = f"""
        SELECT idUser,Tipo_Documento,Documento,nombres_completos,[Género],[Celular],[Teléfono],[Email],Dirección,Municipio,idSpecialty,Especialidad,isPrincipal,isActive,[Fecha_Registro],[Fecha_Nacimiento],[Fecha_Fallecido] FROM (SELECT DISTINCT
        USR.idUser,CONCAT(USR.firstGivenName,' ',USR.secondGiveName ,' ', USR.firstFamilyName,' ',USR.secondFamilyName) as nombres_completos,USRCD.code as Tipo_Documento,USR.documentNumber as Documento,Gender.name AS [Género],UP.telecom AS [Celular],
		UP.phoneHome AS [Teléfono],	UP.Email AS [Email],Up.homeAddress AS Dirección,PLD.name As Municipio,GS.idSpecialty,GS.name AS "Especialidad",USS.isPrincipal,USS.isActive,UP.dateEntry as "Fecha_Registro",UP.birthDate as "Fecha_Nacimiento",UP.deathDate as "Fecha_Fallecido",
        ROW_NUMBER() over( partition by USR.documentNumber order by GS.name desc) as Indicador
        FROM dbo.users USR WITH (NOLOCK)
        INNER JOIN dbo.userConfTypeDocuments USRCD WITH (NOLOCK) ON USR.idDocumentType=USRCD.idTypeDocument
        LEFT JOIN dbo.userSystemSpecialities USS WITH (NOLOCK) ON USR.idUser=USS.idUser 
        LEFT JOIN (SELECT DISTINCT idUser, idSpeciality FROM dbo.userSystemSpecialities USS WITH (NOLOCK) WHERE USS.isPrincipal=1 AND USS.isActive=1) AS Specialities ON USS.idUser=Specialities.idUser AND USS.idSpeciality=Specialities.idSpeciality
        LEFT JOIN dbo.generalSpecialties GS WITH (NOLOCK) ON  Specialities.idSpeciality=GS.idSpecialty
        LEFT JOIN userPeople UP WITH (NOLOCK) ON UP.idUser = USR.idUser
        LEFT JOIN userConfAdministrativeSex AS Gender WITH(NOLOCK) ON UP.idAdministrativeSex = Gender.idAdministrativeSex
        LEFT JOIN dbo.generalPoliticalDivisions AS PLD WITH (NOLOCK) ON PLD.idPoliticalDivision = UP.idHomePlacePoliticalDivision
        WHERE USRCD.code <> 'NIT' ) AS TODO
        WHERE TODO.Indicador=1
        """
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    cols_int=['idSpecialty']
    for i in cols_int:
     df.loc[df[i].isnull(),i]=0

    df['idSpecialty']=df['idSpecialty'].astype(int)
        
                  
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['Fecha_Registro','Fecha_Nacimiento','Fecha_Fallecido']
    for col in cols_dates:
        df[col] = df[col].astype(str)
    
    print(df.columns)
    print(df.dtypes)
    print(df)

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)

# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}

with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '15 6 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_data_users_task = PythonOperator(
                                        task_id = "get_data_users_task",
                                        python_callable = get_data_users,
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_data_users = MsSqlOperator(task_id='load_data_users',
                                    mssql_conn_id=sql_connid,
                                    autocommit=True,
                                    sql="EXECUTE uspCarga_TblDUsuarios",
                                    email_on_failure=True, 
                                    email='BI@clinicos.com.co',
                                    dag=dag
                                    )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_data_users_task >> load_data_users >> task_end
    
