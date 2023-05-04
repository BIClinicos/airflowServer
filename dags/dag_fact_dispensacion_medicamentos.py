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
db_tmp_table = 'TmpDispensacionMedicamentos'
db_table = "TblHDispensacionMedicamentos"
dag_name = 'dag_' + db_table


# Para correr manualmente las fechas
#fecha_texto = '2023-05-02 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

# Para correr fechas con delta
now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')

def func_get_dispensation_medicines_stating ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)
    
    query = f"""
            select distinct PRD.idPharmacyRequestDetail as Id_EntregaFarmacia,EHRE.idEHREvent as Id_Evento,PRD.idGenericProduct as MedicamentoGenerico_Id,ENC.idEncounter as Id_Cita,ENC.identifier as Formulación,PRQ.dateRecord as [Fecha_Pedido],PRQ.isAuthorized as [Formulación Autorizada],PRQ.note as Notas,EHRE.idAction as id_accion,GAC.code as Codigo_Accion,GAC.name as [Nombre Acción],GAC.isActive as [Acción Activa],ENC.idUserPatient AS Paciente_id,PRQ.idUserAuthorized as Usuario_Autoriza_Id,ENCR.idActualPractitioner as Medico_Id,ENC.dateStart as [Fecha_Atencion],EHRE.startRecordedDate as [Fecha_Evento], ENCR.idPrincipalContract AS Contrato_id,  ENCR.idPrincipalPlan as Plan_id,
            /*solicitada (RE), cancelada (CA), anulada (AN), atendida total (DT) o parcialmente (DP)*/
            CASE 
            WHEN PRQ.state = 'RE' THEN 'solicitud'
            WHEN PRQ.state = 'DT' THEN 'despacho'
            WHEN PRQ.state = 'CA' THEN 'cancelada'
            WHEN PRQ.state = 'AN' THEN 'anulada'
            WHEN PRQ.state = 'DP' THEN 'parcialmente'
            ELSE '-1'
            END AS Estado_Solicitud,PRD.requestedQuantity as Cantidad_Solicitada, PRD.authorizedQuantity as Cantidad_Autorizada, PRD.dispensedQuantity as Cantidad_Entregada,ENCR.idFirstDiagnosis as Diagnostico_Id
            from dbo.encounters ENC
            inner join dbo.encounterRecords ENCR on ENC.idEncounter=ENCR.idEncounter
            inner join dbo.EHREvents EHRE on ENC.idEncounter = EHRE.idEncounter
            inner join dbo.pharmacyRequests PRQ on PRQ.EHREvent = EHRE.idEHREvent
            inner join dbo.pharmacyRequestDetails PRD on PRQ.idPharmacyRequest = PRD.idPharmacyRequest
            inner join dbo.generalActions GAC on EHRE.idAction=GAC.idAction
            WHERE PRQ.dateRecord>='{last_week}' AND PRQ.dateRecord<'{now}' """
    # Ejecutar la consulta capturandola en un dataframe
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    cols_int=['MedicamentoGenerico_Id','Diagnostico_Id']
    for i in cols_int:
     df.loc[df[i].isnull(),i]=0
    
    df['MedicamentoGenerico_Id']=df['MedicamentoGenerico_Id'].astype(int) 
    df['Diagnostico_Id']=df['Diagnostico_Id'].astype(int)                 
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['Fecha_Pedido','Fecha_Atencion','Fecha_Evento']
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
    schedule_interval="35 8 * * *",
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    

    
    get_dispensation_medicines_stating= PythonOperator(
                                     task_id = "get_dispensation_medicines_stating",
                                     python_callable = func_get_dispensation_medicines_stating,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_dispensation_medicines_scheduled = MsSqlOperator(task_id='load_fact_dispensation_medicines_scheduled',
                                          mssql_conn_id=sql_connid,
                                          autocommit=True,
                                          sql="EXECUTE uspCarga_TblHDispensacionMedicamentos",
                                          email_on_failure=True, 
                                          email='BI@clinicos.com.co',
                                          dag=dag
                                         )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>get_dispensation_medicines_stating>> load_fact_dispensation_medicines_scheduled>>task_end

