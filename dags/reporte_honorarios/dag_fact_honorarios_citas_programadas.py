import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,load_df_to_sql



#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'TmpHonorariosCitasModeloEspecializada'
db_table = "TblHHonorariosCitasModeloEspecializada"
dag_name = 'dag_' + db_table

# Para correr fechas con delta
now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')

# Para correr manualmente las fechas
#fecha_texto = '2023-04-19 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

def func_get_honoraries_stating ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)
    
    domiConsultas_query = f"""
                    DECLARE 
                    @is_profesional varchar(60),
                    @is_asistente varchar(60)

                    set @is_profesional ='Principal'
                    set @is_asistente ='Especialista Asistente'
                SELECT DISTINCT TODOX.* FROM (
	                (
	                SELECT DISTINCT ASS.idAppointmentSchedulerSlots,ASCH.idAppointmenScheduler,ASS.idAppointment, ASCH.idOffice as Office_id,ASCH.isActive AS Agenda_Activa,ASCH.isDeleted AS Agenda_Eliminada,ASCH.dateBegin AS Fecha_Inicio, ASCH.dateEnd AS Fecha_Fin,ASS.dateAppointment AS Fecha_Cita ,ASCH.durationTimeSlots AS Duracion,ASS.indEstado AS Espacio_Activo,ASCH.idUserProfessional AS User_id,@is_profesional as "Tipo Profesional",Professional.Tipo_Pago_Gomedisys,Professional.Tarifa_Gomedisys,Professional.Fecha_Tarifa_Gomedisys,ASS.isDeleted AS Espacio_Eliminado
                    FROM dbo.appointmentSchedulers ASCH  WITH (NOLOCK)
                    INNER JOIN dbo.appointmentSchedulerSlots ASS WITH (NOLOCK) ON ASCH.idAppointmenScheduler=ASS.idAppointmentScheduler
                    LEFT JOIN (SELECT idAppointmenScheduler,idUserProfessional,Tipo_Pago_Gomedisys,Tarifa_Gomedisys,Fecha_Tarifa_Gomedisys from (
                    SELECT ASCH.idAppointmenScheduler,ASCH.idUserProfessional,BMFD.dateRecord as Fecha_Tarifa_Gomedisys,ASCH.dateBegin,ASCH.dateEnd,
                    CASE WHEN BMFD.calculateForm='T' THEN 'Minutos' 
                        WHEN BMFD.calculateForm='V' THEN 'Valor' 
                        WHEN BMFD.calculateForm='P' THEN 'Porcentaje' 
                        ELSE '-1'
                    END AS Tipo_Pago_Gomedisys,BMFD.value AS Tarifa_Gomedisys, 
                    ROW_NUMBER() over( partition by ASCH.idAppointmenScheduler,ASCH.idUserProfessional order by BMFD.dateRecord desc) as Indicador
                    FROM dbo.appointmentSchedulers ASCH 
                    LEFT JOIN dbo.billMedicalFeeUsers BMFU WITH (NOLOCK) ON ASCH.idUserProfessional=BMFU.idUserMedical 
                    INNER JOIN dbo.billMedicalFees BMF WITH (NOLOCK) ON BMFU.idMedicalFee=BMF.idMedicalFee
                    INNER JOIN  dbo.billMedicalFeeDetails BMFD WITH (NOLOCK) ON BMF.idMedicalFee=BMFD.idMedicalFee
                    WHERE ASCH.dateBegin BETWEEN BMFD.dateBegin  and BMFD.dateEnd
                    ) AS Todo where Indicador=1) AS Professional ON ASCH.idAppointmenScheduler=Professional.idAppointmenScheduler AND ASCH.idUserProfessional=Professional.idUserProfessional
                    )
                UNION ALL
                (
                    SELECT DISTINCT ASS.idAppointmentSchedulerSlots,ASCH.idAppointmenScheduler,ASS.idAppointment, ASCH.idOffice as Office_id,ASCH.isActive AS Agenda_Activa,ASCH.isDeleted AS Agenda_Eliminada,ASCH.dateBegin AS Fecha_Inicio, ASCH.dateEnd AS Fecha_Fin,ASS.dateAppointment AS Fecha_Cita ,ASCH.durationTimeSlots AS Duracion,ASS.indEstado AS Espacio_Activo,ASCH.idAssistantSpecialist AS User_id,@is_asistente as "Tipo Profesional",Professional.Tipo_Pago_Gomedisys,Professional.Tarifa_Gomedisys,Professional.Fecha_Tarifa_Gomedisys,ASS.isDeleted AS Espacio_Eliminado
                    FROM dbo.appointmentSchedulers ASCH  WITH (NOLOCK)
                    INNER JOIN dbo.appointmentSchedulerSlots ASS WITH (NOLOCK) ON ASCH.idAppointmenScheduler=ASS.idAppointmentScheduler
                    LEFT JOIN (SELECT idAppointmenScheduler,idAssistantSpecialist,Tipo_Pago_Gomedisys,Tarifa_Gomedisys,Fecha_Tarifa_Gomedisys from (
                    SELECT ASCH.idAppointmenScheduler,ASCH.idAssistantSpecialist,BMFD.dateRecord as Fecha_Tarifa_Gomedisys,ASCH.dateBegin,ASCH.dateEnd,
                    CASE WHEN BMFD.calculateForm='T' THEN 'Minutos'
                        WHEN BMFD.calculateForm='V' THEN 'Valor' 
                        WHEN BMFD.calculateForm='P' THEN 'Porcentaje' 
                        ELSE '-1'
                    END AS Tipo_Pago_Gomedisys,BMFD.value AS Tarifa_Gomedisys, 
                    ROW_NUMBER() over( partition by ASCH.idAppointmenScheduler,ASCH.idAssistantSpecialist order by BMFD.dateRecord desc) as Indicador
                    FROM dbo.appointmentSchedulers ASCH 
                    LEFT JOIN dbo.billMedicalFeeUsers BMFU WITH (NOLOCK) ON ASCH.idAssistantSpecialist=BMFU.idUserMedical 
                    INNER JOIN dbo.billMedicalFees BMF WITH (NOLOCK) ON BMFU.idMedicalFee=BMF.idMedicalFee
                    INNER JOIN  dbo.billMedicalFeeDetails BMFD WITH (NOLOCK) ON BMF.idMedicalFee=BMFD.idMedicalFee
                    WHERE ASCH.dateBegin BETWEEN BMFD.dateBegin  and BMFD.dateEnd
                    ) AS Todo where Indicador=1) AS Professional ON ASCH.idAppointmenScheduler=Professional.idAppointmenScheduler AND ASCH.idAssistantSpecialist=Professional.idAssistantSpecialist
                    WHERE ASCH.idAssistantSpecialist IS NOT NULL
                ) ) AS TODOX
                    WHERE Fecha_Inicio >='{last_week}' AND Fecha_Inicio<'{now}'
                    """
    # Ejecutar la consulta capturandola en un dataframe
    df = sql_2_df(domiConsultas_query, sql_conn_id=sql_connid_gomedisys)
      
                  
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['Fecha_Inicio','Fecha_Fin','Fecha_Cita','Fecha_Tarifa_Gomedisys']
    for col in cols_dates:
        df[col] = df[col].astype(str)

    print(df.columns)
    print(df.dtypes)
    print(df.head())

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)


def execute_Sql(): # Se utiliza para elilminar registros duplicados en cargas adicionales.
     query = f"""
     delete from TmpHonorariosCitasModeloEspecializada where Fecha_Inicio >='{last_week}' AND Fecha_Inicio <'{now}'
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
    schedule_interval='55 6 * * *',
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
                                     python_callable = func_get_honoraries_stating,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_honoraries_scheduled_appointments = MsSqlOperator(task_id='load_fact_honoraries_scheduled_appointments',
                                          mssql_conn_id=sql_connid,
                                          autocommit=True,
                                          sql="EXECUTE uspCarga_TblHHonorariosCitasModeloEspecializada",
                                          email_on_failure=True, 
                                          email='BI@clinicos.com.co',
                                          dag=dag
                                         )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> extract_honoraries_stating >>get_honoraries_stating>> load_fact_honoraries_scheduled_appointments>>task_end
#start_task >> extract_cita_detail >>get_cita_detail>>task_end
