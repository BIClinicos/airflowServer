import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta,date

import pandas as pd
from variables import sql_connid,sql_connid_gomedisys, sql_connid_domi
from utils import load_df_to_sql, sql_2_df

#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'TmpHonorariosCitasModeloDomiciliaria'
db_tmp_table_domi = 'TmpCitasModeloDomiciliaria'
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
last_week = now - timedelta(weeks=1)
#last_week = datetime(2023,6,1)
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

    ###Fuente de informacion alterna que se almacena en BD de DOMI
    #Filtro de contrato y actividades
    #act_list = [124,126,127,256,271,367,373,419,429,611,613,614,666,681,770,776,813,822,1001,1009,1014,1015]
    #df2 = df.loc[df['contract_id'] == 57 and df['action_id'].isin(act_list)]

    #if ~df2.empty and len(df2.columns) >0:
    #    load_df_to_sql(df2, db_tmp_table, sql_connid_domi)    


def execute_Sql():
     query = f"""
     delete from TmpHonorariosCitasModeloDomiciliaria where Fecha_Cita >='{last_week}' AND Fecha_Cita <'{now}'
     """
     hook = MsSqlHook(sql_connid)
     hook.run(query)


def load_copy_domi():
    query = f"""
    select * 
    from TblHHonorariosCitasModeloDomiciliaria 
    where Fecha_Cita >='{last_week}' AND Fecha_Cita <'{now}'
    and action_id in (124,126,127,256,271,367,373,419,429,611,613,614,666,681,770,776,813,822,1001,1009,1014,1015,1066,1067,1068,1069,1087,1088,1089,1090,1092,1093,1094,1095)
    and contract_id = 57
    """
    df = sql_2_df(query, sql_conn_id = sql_connid)

    #Convertir a str los campos de tipo fecha 
    cols_dates = ['Fecha_Cita','Fecha_Tarifa_Gomedisys','created_at','calendario_id']
    for col in cols_dates:
        df[col] = df[col].astype(str)

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table_domi, sql_connid_domi)    



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
    
    # Se declara y se llama la función encargada de subir copia de la información a la base db-domi-001
    copy_honoraries_domi = PythonOperator(
                                     task_id = "copy_honoraries_domi",
                                     python_callable = load_copy_domi,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure" de copia a la base db-domi-001
    load_copy_honoraries_domi = MsSqlOperator(task_id='load_copy_honoraries_domi',
                                          mssql_conn_id=sql_connid_domi,
                                          autocommit=True,
                                          sql="EXECUTE uspCarga_TblHCitasModeloDomiciliaria",
                                          email_on_failure=True, 
                                          email='BI@clinicos.com.co',
                                          dag=dag
                                         )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> extract_honoraries_stating >>get_honoraries_stating>> load_fact_honoraries_scheduled_appointments>>copy_honoraries_domi>>load_copy_honoraries_domi>>task_end
