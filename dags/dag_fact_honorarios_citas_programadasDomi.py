import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta,date

import pandas as pd
from variables import sql_connid,sql_connid_gomedisys
from utils import load_df_to_sql, sql_2_df

#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'TmpHonorariosCitasModeloDomiciliaria'
db_table = "TblHHonorariosCitasModeloDomiciliaria"
dag_name = 'dag_' + db_table

# Para correr manualmente las fechas
#fecha_texto = '2023-04-21 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

# Para correr fechas con delta
now = datetime.now()
# last_week = now - timedelta(weeks=1)
last_week = datetime(2023,1,1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')

def func_get_honorarios_stating ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)
    
    domiConsultas_query = f"""
        with Todo as (
            SELECT 
                HRE.idEHREvent,
                HRE.idPractitioner,
                HRE.idPatient,
                BMFD.dateRecord as Fecha_Tarifa_Gomedisys,
                CASE 
                    WHEN BMFD.calculateForm='T' THEN 'Minutos'
                    WHEN BMFD.calculateForm='V' THEN 'Valor' 
                    WHEN BMFD.calculateForm='P' THEN 'Porcentaje' 
                    ELSE '-1'
                END AS Tipo_Pago_Gomedisys,
                BMFD.value AS Tarifa_Gomedisys, 
                ROW_NUMBER() over( partition by HRE.idEHREvent,HRE.idPractitioner order by BMFD.dateRecord desc) as Indicador
            FROM  dbo.EHREvents HRE WITH (NOLOCK)
                INNER JOIN dbo.billMedicalFeeUsers BMFU WITH (NOLOCK) ON HRE.idPractitioner=BMFU.idUserMedical 
                    AND HRE.idPatientLocation IN (5,77,79,81) AND HRE.actionRecordedDate BETWEEN '{last_week}' AND '{now}' 
                INNER JOIN dbo.billMedicalFees BMF WITH (NOLOCK) ON BMFU.idMedicalFee=BMF.idMedicalFee
                    AND BMF.isActive = 1
                INNER JOIN  dbo.billMedicalFeeDetails BMFD WITH (NOLOCK) ON BMF.idMedicalFee=BMFD.idMedicalFee
                    AND HRE.actionRecordedDate BETWEEN BMFD.dateBegin  and BMFD.dateEnd
        ),
        Professional as (
            SELECT idEHREvent,idPractitioner,idPatient,
            Tipo_Pago_Gomedisys,Tarifa_Gomedisys,Fecha_Tarifa_Gomedisys from Todo where Indicador=1
        )
        SELECT 
            HRE.idEHREvent, 
            HRE.idPractitioner as User_id,
            HRE.idEncounter,
            ENC.idOffice,
            HRE.actionRecordedDate as Fecha_Cita,
            HRE.isActive as Agenda_Activa,
            HRE.idAction as action_id,
            HRE.idPatientLocation,
            ENCR.idPrincipalContract as contract_id,
            Professional.Tipo_Pago_Gomedisys,
            Professional.Tarifa_Gomedisys,
            Professional.Fecha_Tarifa_Gomedisys,
            HRE.idPatient as Patient_id
        FROM dbo.EHREvents HRE WITH (NOLOCK)
        INNER JOIN dbo.encounters  ENC WITH (NOLOCK) on HRE.idEncounter=ENC.idEncounter 
            AND HRE.idPatientLocation IN (5,77,79,81) AND HRE.actionRecordedDate BETWEEN '{last_week}' AND '{now}' 
        INNER JOIN dbo.encounterRecords ENCR  WITH (NOLOCK) on ENC.idEncounter=ENCR.idEncounter
        LEFT JOIN Professional WITH (NOLOCK) ON HRE.idEHREvent=Professional.idEHREvent AND HRE.idPractitioner=Professional.idPractitioner
            AND HRE.idPatient = Professional.idPatient
        GROUP BY
        HRE.idEHREvent, 
            HRE.idPractitioner,
            HRE.idPatient,
            HRE.idEncounter,
            ENC.idOffice,
            HRE.actionRecordedDate,
            HRE.isActive,
            HRE.idAction ,
            HRE.idPatientLocation,
            ENCR.idPrincipalContract,
            Professional.Tipo_Pago_Gomedisys,
            Professional.Tarifa_Gomedisys,
            Professional.Fecha_Tarifa_Gomedisys"""
            
    # Ejecutar la consulta capturandola en un dataframe
    df = sql_2_df(domiConsultas_query, sql_conn_id=sql_connid_gomedisys)
   
    cols_int=['idEncounter', 'idOffice','contract_id']
    for i in cols_int:
     df.loc[df[i].isnull(),i]=0

    df['idEncounter']=df['idEncounter'].astype(int)
    df['idOffice']=df['idOffice'].astype(int) 
    df['contract_id']=df['contract_id'].astype(int) 
      
                  
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['Fecha_Cita','Fecha_Tarifa_Gomedisys']
    for col in cols_dates:
        df[col] = df[col].astype(str)

    print(df.columns)
    print(df.dtypes)
    print(df.head())

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)
        


def execute_Sql():
     query = f"""
     delete from TmpHonorariosCitasModeloDomiciliaria where Fecha_Cita >='{last_week}' AND Fecha_Cita <'{now}'
     """
     hook = MsSqlHook(sql_connid)
     hook.run(query)


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
    schedule_interval='15 7 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    
    extract_honoraries_stating= PythonOperator(
                                     task_id = "extract_honoraries_stating",
                                     python_callable = execute_Sql,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    get_honoraries_stating= PythonOperator(
                                     task_id = "get_honoraries_stating",
                                     python_callable = func_get_honorarios_stating,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_honoraries_scheduled_appointments = MsSqlOperator(task_id='load_fact_honoraries_scheduled_appointments',
                                          mssql_conn_id=sql_connid,
                                          autocommit=True,
                                          sql="EXECUTE uspCarga_TblHHonorariosCitasModeloDomiciliaria",
                                          email_on_failure=True, 
                                          email='BI@clinicos.com.co',
                                          dag=dag
                                         )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> extract_honoraries_stating >>get_honoraries_stating>> load_fact_honoraries_scheduled_appointments>>task_end
