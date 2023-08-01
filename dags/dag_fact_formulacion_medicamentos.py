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
db_tmp_table = 'TmpFormulacionMedicamentos'
db_table = "TblHFormulacionMedicamentos"
dag_name = 'dag_' + db_table


# Para correr manualmente las fechas
#fecha_texto = '2023-06-01 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2021-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

# Para correr fechas con delta
now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')

def func_get_formulation_medicines_stating ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)
    
    query = f"""
                    select EHRE.idEHREvent as Id_Evento
                    ,EHREF.idProductGeneric AS MedicamentoGenerico_Id
                    ,ENC.idEncounter as Id_Cita
                    ---,EHREF.idProductType as [Tipo_Producto]                    
                    ,EHREF.dateRecorded as [Fecha_Formulación_Medicamento]
                    ,EHREF.isActive as [Formulación_Activa]
                    ,EHREF.isSuspended as [Formulación_Suspendida]
                    ,EHREF.dateSuspended as [Fecha_Suspensión_Medicamento]
                    ,CONCAT(CAST(round(EHREF.doseValue, 0, 0)AS INT), ' ',  EFP.name) as Periodicidad
                    ,EHREF.valueAdministrationTime as [Tiempo_Administración]
                    , EHREF.formulatedAmount as [Cantidad_Formulada]
                    ,PAR.name as [Vía_Administración]
                    ,EHRE.idAction as Id_Accion
                    ,GAC.code as Codigo_Accion,GAC.name as [Nombre_Acción]
                    ,GAC.isActive as [Acción_Activa],ENC.idUserPatient AS Paciente_Id
                    ,EHREF.idUserPractitioner as Medico_id
                    , ENC.dateStart as [Fecha_Atención]
                    ,EHRE.startRecordedDate as [Fecha_Evento]
                    , ENCR.idPrincipalContract AS Contrato_Id
                    ,  ENCR.idPrincipalPlan as Plan_Id
                    ,ENCR.idFirstDiagnosis as Diagnostico_Id
                    , EHREF.idProductType as [Tipo_Producto]
                    , EHREF.annotations as [Recomendaciones]
                    from dbo.encounters ENC
                    inner join dbo.encounterRecords ENCR on ENC.idEncounter=ENCR.idEncounter
                    inner join dbo.EHREvents EHRE on ENC.idEncounter = EHRE.idEncounter
                    inner join dbo.generalActions GAC on EHRE.idAction=GAC.idAction
                    inner join dbo.EHREventFormulation EHREF on EHRE.idEHREvent=EHREF.idEHREvent
                    left join dbo.EHRConfFormulationPeriodicity EFP on EHREF.idPeriodicity= EFP.idPeriodicity
                    left join dbo.productConfAdministrationRoute PAR ON EHREF.idAdministrationRoute = PAR.idAdministrationRoute
                    WHERE EHREF.dateRecorded >='{last_week}' AND EHREF.dateRecorded<'{now}' and EHREF.idProductGeneric<>0
                    """
    # Ejecutar la consulta capturandola en un dataframe
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    cols_int=['MedicamentoGenerico_Id','Tiempo_Administración','Diagnostico_Id']
    for i in cols_int:
     df.loc[df[i].isnull(),i]=0

    df['MedicamentoGenerico_Id']=df['MedicamentoGenerico_Id'].astype(int)
    df['Tiempo_Administración']=df['Tiempo_Administración'].astype(int)
    df['Diagnostico_Id']=df['Diagnostico_Id'].astype(int)
                  
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['Fecha_Formulación_Medicamento','Fecha_Suspensión_Medicamento','Fecha_Atención','Fecha_Evento']
    for col in cols_dates:
        df[col] = df[col].astype(str)

    print(df.columns)
    print(df.dtypes)
    print(df.head())

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
    schedule_interval="30 8 * * *",
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    

    
    get_formulation_medicines_stating= PythonOperator(
                                     task_id = "get_formulation_medicines_stating",
                                     python_callable = func_get_formulation_medicines_stating,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_formulation_medicines_scheduled_appointments = MsSqlOperator(task_id='load_fact_formulation_medicines_scheduled_appointments',
                                          mssql_conn_id=sql_connid,
                                          autocommit=True,
                                          sql="EXECUTE uspCarga_TblHFormulacionMedicamentos",
                                          email_on_failure=True, 
                                          email='BI@clinicos.com.co',
                                          dag=dag
                                         )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>get_formulation_medicines_stating>> load_fact_formulation_medicines_scheduled_appointments>>task_end

