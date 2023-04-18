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

db_table = "TblDEventosDiagnosticos"
db_tmp_table = "TmpEventosDiagnosticos"
dag_name = 'dag_' + db_table

#Se halla las fechas de cargue de la data 

# Para correr manualmente las fechas
#fecha_texto = '2023-01-31 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

# Para correr fechas con delta
now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')




# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def get_data_events_diagnostics():

    query = f"""
        SELECT Todo.idDiagnostic,Todo.idEncounter,Todo.idUserPatient,Todo.name,Todo.isActive,Todo.isPrincipal,Todo.actionRecordedDate,Todo.recordNoValid 
        FROM (
        select distinct EVD.idDiagnostic, EVEN.idEncounter,ENC.idUserPatient, dx.name,EVEN.isActive,EVD.isPrincipal,EVEN.actionRecordedDate as actionRecordedDate,EVEN.recordNoValid
        ,ROW_NUMBER() over( partition by EVD.idDiagnostic,EVEN.idEncounter,ENC.idUserPatient order by EVEN.actionRecordedDate desc) as Indicador
        FROM dbo.EHREvents AS EVEN WITH(NOLOCK)
        INNER JOIN dbo.EHREventMedicalDiagnostics AS EVD WITH(NOLOCK) ON EVD.idEHREvent = EVEN.idEHREvent
        INNER JOIN dbo.diagnostics AS Dx WITH(NOLOCK) ON EVD.idDiagnostic = Dx.idDiagnostic
        INNER JOIN dbo.users AS USR WITH (NOLOCK) ON USR.idUser = EVEN.idPatient
        INNER JOIN dbo.encounters AS ENC WITH (NOLOCK) ON USR.idUser = ENC.idUserPatient
        INNER JOIN dbo.encounterRecords AS ENR WITH (NOLOCK) ON ENC.idEncounter = ENR.idEncounter AND EVEN.idEncounter IS NOT NULL
        WHERE EVEN.actionRecordedDate >='{last_week}' AND EVEN.actionRecordedDate<'{now}') AS Todo 
        WHERE Todo.Indicador=1
        """
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)

    #Convertir a str los campos de tipo fecha 
    cols_dates = ['actionRecordedDate']
    for col in cols_dates:
        df[col] = df[col].astype(str)

    #Se crea la columna  NomCap del cruce de Cie10
    path='/opt/airflow/dags/files_clasificador_grupo_cie10/clasificador de grupo cie10.xlsx'
    df_cie10 = pd.read_excel(path,sheet_name='Hoja1',header=None,names=['Cod3Car','Desc3Car','Cod4Car','Desc4Car','Cap','NomCap'])
    df_cie10=df_cie10.drop([0],axis=0).reset_index()
    df_cie10['Desc4Car']=df_cie10['Desc4Car'].apply(lambda x :x.upper())
    df_cie10 = df_cie10[["Desc4Car", "NomCap"]]
    resultado=df.merge(df_cie10,left_on='name', right_on='Desc4Car')
    resultado=resultado[["idDiagnostic","idEncounter","idUserPatient","name","isActive","isPrincipal","NomCap","actionRecordedDate","recordNoValid"]]
    #resultado.loc[resultado.isPrincipal==0,'NomCap']=="NA"    
    for i in range(len(resultado)):
        if(resultado.iloc[i]['isPrincipal']==0):
            resultado.at[i,"NomCap"]="NA"

    print(resultado.columns)
    print(resultado.dtypes)
    print(resultado.head())
 
    if ~resultado.empty and len(resultado.columns) >0:
        load_df_to_sql(resultado, db_tmp_table, sql_connid)

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
    schedule_interval= "00 8 * * *",
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_data_events_diagnostics_python_task = PythonOperator(
                                             task_id = "get_data_events_diagnostics_python_task",
                                             python_callable = get_data_events_diagnostics,
                                             email_on_failure=True, 
                                             email='BI@clinicos.com.co',
                                             dag=dag
                                                    )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_data_events_diagnostics = MsSqlOperator(task_id='load_data_events_diagnostics',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDEventosDiagnosticos",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_data_events_diagnostics_python_task >> load_data_events_diagnostics >> task_end