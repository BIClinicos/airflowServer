import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,load_df_to_sql



#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'TpmCitasInasistencia'
db_table = "TblHCitasInasistencia"
dag_name = 'dag_' + db_table

# Para correr manualmente las fechas
#fecha_texto = '2023-01-01'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d')


# Para correr fechas con delta
now = datetime.now()
now = now - timedelta(days=1)
now = now.strftime('%Y-%m-%d')


def func_get_appointment_inacistances_stating():

    print('Fecha inicio ', now)
    
    
    domiConsultas_query = f"""
                    SELECT DISTINCT appointment.idAppointment AS [Identificador],enc.idEncounter as [idEncuentro], appointment.idEvent as [idEvento],CompanyOff.idOffice as [oficina_id],appstate.itemName AS [Estado Cita],appointment.idContract as [contrato_id],appointment.idPlan as [plan_id],AppS.idUserProfessional as [profesional_id],appointment.idUserPerson AS [paciente_id],HealthR.name AS [Régimen],AppSd.dateAppointment as [Fecha Cita],CONVERT(DATE,AppSd.dateAppointment) as [calendario_id],
                    CONCAT(RIGHT('00' + Ltrim(Rtrim(DATEPART(HOUR,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(MINUTE,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(SECOND,AppSd.dateAppointment))),2)) AS [HORA DE CITA],
                    CASE 
                    WHEN CONVERT(TIME, CONCAT(RIGHT('00' + Ltrim(Rtrim(DATEPART(HOUR,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(MINUTE,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(SECOND,AppSd.dateAppointment))),2))) > CONVERT(TIME,'05:59') AND CONVERT(TIME, CONCAT(RIGHT('00' + Ltrim(Rtrim(DATEPART(HOUR,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(MINUTE,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(SECOND,AppSd.dateAppointment))),2))) < CONVERT(TIME,'09:00') THEN 'A'
                    WHEN CONVERT(TIME, CONCAT(RIGHT('00' + Ltrim(Rtrim(DATEPART(HOUR,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(MINUTE,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(SECOND,AppSd.dateAppointment))),2))) > CONVERT(TIME,'08:59') AND CONVERT(TIME, CONCAT(RIGHT('00' + Ltrim(Rtrim(DATEPART(HOUR,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(MINUTE,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(SECOND,AppSd.dateAppointment))),2))) < CONVERT(TIME,'15:00') THEN 'B'
                    WHEN CONVERT(TIME, CONCAT(RIGHT('00' + Ltrim(Rtrim(DATEPART(HOUR,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(MINUTE,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(SECOND,AppSd.dateAppointment))),2))) > CONVERT(TIME,'14:59') AND CONVERT(TIME, CONCAT(RIGHT('00' + Ltrim(Rtrim(DATEPART(HOUR,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(MINUTE,AppSd.dateAppointment))),2),':',RIGHT('00' + Ltrim(Rtrim(DATEPART(SECOND,AppSd.dateAppointment))),2))) < CONVERT(TIME,'20:00') THEN 'C'
                    ELSE '-1'
                    END AS [FRANJA HORARIA],
                    DATEDIFF(DAY, appointment.dateRecord, AppSd.dateAppointment) AS [Diferencia Fecha cita - Asign (días)],
                    AppT.itemName AS [Tipo Cita],
                    (AppS.durationTimeSlots * (SELECT COUNT(*) FROM appointmentSchedulerSlots AS Slot WHERE appointment.idAppointment = Slot.idAppointment)) AS [Tiempo de cita],
                    CE.name AS [Examen],
                    ISNULL(appointment.dateRecord, '-1') AS [Fecha Asignación Cita],
                    CASE
                    WHEN appointment.IsForTelemedicine = 0 THEN 'PRESENCIAL'
                    WHEN appointment.IsForTelemedicine = 1 THEN 'TELECONSULTA'
                    ELSE '-1'
                    END AS [TELECONSULTA O PRESENCIAL],
                    ISNULL(apCan.note, '-1') AS [Motivo cancelación 1],
                    ISNULL(genCau.descripcion, '-1') AS [Motivo cancelación 2]
                    FROM appointmentSchedulers AS AppS WITH (NOLOCK)
                    INNER JOIN appointmentSchedulerSlots AS AppSd WITH (NOLOCK) ON AppS.idAppointmenScheduler = AppSd.idAppointmentScheduler
                    INNER JOIN appointments AS appointment ON appointment.idAppointmentSchedulerSlots = AppSd.idAppointmentSchedulerSlots
                    INNER JOIN companyOffices AS CompanyOff WITH (NOLOCK) ON CompanyOff.idOffice = AppS.idOffice
                    LEFT JOIN (select APJ1.idCausalList,APJ1.idAppointment,APJ1.note from appointmentJournals APJ1
                    inner join (select idAppointment,max(dateRecord) as dateRecord  from appointmentJournals group by idAppointment) as APJ2 on APJ1.idAppointment=APJ2.idAppointment and APJ1.dateRecord=APJ2.dateRecord) AS apCan ON  appointment.idAppointment = apCan.idAppointment
                    LEFT JOIN generalCausalLists AS genCau WITH (NOLOCK) ON apCan.idCausalList = genCau.idCausalList
                    INNER JOIN generalInternalLists AS appState WITH(NOLOCK) ON appointment.state = appState.itemValue AND appState.groupCode = 'appState'
                    LEFT OUTER JOIN physicalLocationRooms AS Rooms WITH (NOLOCK) ON Rooms.idRoom = AppS.idRoom
                    INNER JOIN generalInternalLists AS AppT WITH (NOLOCK) ON appointment.idAppointmentExamType = AppT.idGeneralInternalList
                    INNER JOIN appointmentExams CE WITH (NOLOCK) ON appointment.idAppointmentExam = CE.idAppointmentExam
                    INNER JOIN healthRegimes AS HealthR WITH(NOLOCK) ON appointment.idHealthRegime = HealthR.idHealthRegime
                    LEFT OUTER JOIN encounters AS enc WITH(NOLOCK) ON appointment.idAdmission = enc.idEncounter
                    LEFT OUTER JOIN encounterConfAdmitSource AS AdmitS WITH(NOLOCK) ON enc.idAdmitSource = AdmitS.idAdmitSource
                    LEFT OUTER JOIN encounterRecords AS EncRecord WITH(NOLOCK) ON enc.idEncounter = EncRecord.idEncounter
                    WHERE CONVERT(DATE,AppSd.dateAppointment) BETWEEN '{now}' AND (select  CONVERT(DATE,max(AppSd.dateAppointment)) from appointmentSchedulerSlots AppSd
                    INNER JOIN appointments AS appointment ON appointment.idAppointmentSchedulerSlots = AppSd.idAppointmentSchedulerSlots
                    inner join appointmentSchedulers AS AppS on AppSd.idAppointmentScheduler=AppS.idAppointmenScheduler) """
    
    sql3= f"""select CI.paciente_id,[Estado Cita],UltimaAsistida,UltimaCanceladaUsuario,cumAsistida,cumCancelada,cantidadCitas
              from  TblHCitasInasistencia as CI
              inner join (select paciente_id, max([Fecha Asignación Cita]) as max_fecha from TblHCitasInasistencia group by paciente_id) as CI2
              on CI.paciente_id=CI2.paciente_id and CI.[Fecha Asignación Cita]=CI2.max_fecha
            """
    # Ejecutar la consulta capturandola en un dataframe
    df = sql_2_df(domiConsultas_query, sql_conn_id=sql_connid_gomedisys)
    df_ultimo = sql_2_df(sql3, sql_conn_id=sql_connid)
      
                  
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['Fecha Cita','calendario_id','HORA DE CITA','Fecha Asignación Cita']
    for col in cols_dates:
        df[col] = df[col].astype(str)
    
    cols_int=['idEncuentro', 'idEvento','oficina_id','contrato_id','plan_id','profesional_id']
    for i in cols_int:
     df.loc[df[i].isnull(),i]=0

    df['idEncuentro']=df['idEncuentro'].astype(int)
    
 
    
    #Tranformación del campo Estado cita segun el motivo de cancelación
    for i in range(len(df)):
        if(df.iloc[i]['Estado Cita']=='Cancelada' and df.iloc[i]['Motivo cancelación 2']=='Por Solicitud del Usuario' ):
            df.at[i,"Estado Cita"]="Cancelada por usuario"
        elif(df.iloc[i]['Estado Cita']=='Cancelada'):
            df.at[i,"Estado Cita"]="Cancelada por Clínicos"
    

    # Se crea una etiqueta de ultimo estado
    df['lag_citas'] = df.groupby(['paciente_id'])['Estado Cita'].shift(1)
    
    # Se crean las variables nuevas para el DF
    df["UltimaAsistida"] = np.nan
    df["UltimaCanceladaUsuario"] = np.nan
    df["cumAsistida"] = np.nan
    df["cumCancelada"] = np.nan
    df["cantidadCitas"] = np.nan

    # Función que verfica si el usuario tiene citas cargadas anteriormente
    def check_last(x):
        dato = df_ultimo.loc[df_ultimo['paciente_id'] == x]
        return not dato.empty

   
    #Función que calcula la condición de asistida 
    def condition_assisted(x,idUserPerson):
        if not df_ultimo.empty:
            if(isinstance(x, float) and not check_last(idUserPerson)):         
                return 1
            elif (isinstance(x, float) and check_last(idUserPerson)): 
                if((df_ultimo.loc[df_ultimo['paciente_id'] == idUserPerson]['Estado Cita']=="Asistida").iloc[0]):                      
                    return 1 
                else:
                    return 0                    
            elif x=="Asistida" or isinstance(x, float):               
                return 1        
            else:              
                return 0
        else:
            if x=="Asistida" or isinstance(x, float):
                return 1
            else:
                return 0
    
    #Función que calcula la condición de cancelada    
    def condition_canceld(x,idUserPerson):
        if not df_ultimo.empty:
            if(x=="Cancelada por usuario" and isinstance(x, float) and not check_last(idUserPerson)):         
                return 1
            elif (isinstance(x, float) and check_last(idUserPerson)): 
                if((df_ultimo.loc[df_ultimo['paciente_id'] == idUserPerson]['Estado Cita']=="Cancelada por usuario").iloc[0]):                      
                    return 1 
                else:
                    return 0                    
            elif x=="Cancelada por usuario":               
                return 1        
            else:              
                return 0
        else:
            if x=="Cancelada por usuario":
                return 1
            else:
                return 0
            
    # Función que recalcula la cantidad de citas asistidas si el usuario tiene citas cargadas anteriormente
    def sum_cum_assisted(x,idUserPerson,id):
        if not df_ultimo.empty:
            if(check_last(idUserPerson)):
                return df_ultimo.loc[df_ultimo['paciente_id'] == idUserPerson]['cumAsistida'].iloc[0]+df.loc[df['Identificador'] == id]['cumAsistida'].iloc[0]
            else:
                return x
        else:
            return x
    # Función que recalcula la cantidad de citas canceladas si el usuario tiene citas cargadas anteriormente
    def sum_cum_canceld(x,idUserPerson,id):
        if not df_ultimo.empty:
            if(check_last(idUserPerson)):
                return df_ultimo.loc[df_ultimo['paciente_id'] == idUserPerson]['cumCancelada'].iloc[0]+df.loc[df['Identificador'] == id]['cumCancelada'].iloc[0]
            else:
                return x
        else:
            return x
    # Función que recalcula el total de citas si el usuario tiene citas cargadas anteriormente
    def sum_cum_quantity(x,idUserPerson,id):
        if not df_ultimo.empty:
            if(check_last(idUserPerson)):
                return df_ultimo.loc[df_ultimo['paciente_id'] == idUserPerson]['cantidadCitas'].iloc[0]+df.loc[df['Identificador'] == id]['cantidadCitas'].iloc[0]
            else:
                return x+1
        else:
            return x+1
    
    # Se calcula la cuenta de las canceladas y asistidad
    df['UltimaAsistida'] = df.apply(lambda x: condition_assisted(x.lag_citas, x.paciente_id), axis=1)
    df['UltimaCanceladaUsuario'] = df.apply(lambda x: condition_canceld(x.lag_citas, x.paciente_id), axis=1)
    
    
    df['cumAsistida'] = df.groupby(['paciente_id'])['UltimaAsistida'].cumsum()
    df['cumCancelada'] = df.groupby(['paciente_id'])['UltimaCanceladaUsuario'].cumsum()
    df['cantidadCitas'] = df.groupby(['paciente_id'])['paciente_id'].cumcount()+1

    if not df_ultimo.empty: 
        df['cumAsistida'] = df.apply(lambda x: sum_cum_assisted(x.cumAsistida, x.paciente_id,x.Identificador), axis=1)
        df['cumCancelada'] = df.apply(lambda x: sum_cum_canceld(x.cumCancelada, x.paciente_id,x.Identificador), axis=1)
        df['cantidadCitas'] = df.apply(lambda x: sum_cum_quantity(x.cantidadCitas, x.paciente_id,x.Identificador), axis=1)
    
    # Se calcula los Porcentajes de canceladas y asistidas
    df['porcAsistida'] = df.apply(lambda row: (row.cumAsistida/row.cantidadCitas), axis=1)
    df['porcCanceladaUsuario'] = df.apply(lambda row: (row.cumCancelada/row.cantidadCitas), axis=1)

    # Se seleccionan los campos que se van a guardar en la tabla
    df=df[['Identificador','idEncuentro','idEvento','oficina_id','Estado Cita','contrato_id','plan_id','profesional_id','paciente_id','Régimen',
           'Fecha Cita','calendario_id','HORA DE CITA','FRANJA HORARIA','Diferencia Fecha cita - Asign (días)','Tipo Cita',
           'Tiempo de cita','Examen','Fecha Asignación Cita','TELECONSULTA O PRESENCIAL','Motivo cancelación 1','Motivo cancelación 2',
           'UltimaAsistida','UltimaCanceladaUsuario','cumAsistida','cumCancelada','cantidadCitas','porcAsistida','porcCanceladaUsuario']]
    
    print(df.columns)
    print(df.dtypes)
    print(df.head())
    

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)


def execute_Sql():
     query = f"""
     delete from TblHCitasInasistencia where CONVERT(DATE,[Fecha Cita]) >='{now}'
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
    schedule_interval="15 8 * * *",
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    
    extract_appointment_inacistances_stating= PythonOperator(
                                     task_id = "extract_appointment_inacistances_stating",
                                     python_callable = execute_Sql,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )

    
    get_appointment_inacistances_stating= PythonOperator(
                                     task_id = "get_appointment_inacistances_stating",
                                     python_callable = func_get_appointment_inacistances_stating,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_appointment_inacistances_stating = MsSqlOperator(task_id='load_fact_appointment_inacistances_stating',
                                          mssql_conn_id=sql_connid,
                                          autocommit=True,
                                          sql="EXECUTE uspCarga_TblHCitasInasistencia",
                                          email_on_failure=True, 
                                          email='BI@clinicos.com.co',
                                          dag=dag
                                         )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task>>extract_appointment_inacistances_stating>>get_appointment_inacistances_stating>>load_fact_appointment_inacistances_stating>>task_end

