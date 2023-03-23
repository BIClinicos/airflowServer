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

db_table = "TblDCitas"
db_tmp_table = "TmpCitas"
dag_name = 'dag_' + db_table

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
#Se halla las fechas de cargue de la data 
now = datetime.now()
#fecha_texto = '2023-03-14 04:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-01-01 04:00:00', '%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

def get_data_citas():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)

    query = f"""
        SELECT distinct AP.idAppointment,AP.idContract,APRO.idProduct,CON.name as Contrato,PRO.name AS Producto,PRO.legalCode AS CUPS, AP.dateAppointment, AP.dateRecord
        FROM  dbo.appointments AP WITH (NOLOCK)
        INNER JOIN  dbo.contracts CON  WITH (NOLOCK) ON AP.idContract=CON.idContract
        LEFT JOIN  dbo.appointmentProducts APRO WITH (NOLOCK) ON AP.idAppointment=APRO.idAppointment
        LEFT JOIN  dbo.products PRO WITH (NOLOCK) ON APRO.idProduct=PRO.idProduct   
        WHERE AP.dateAppointment >='{last_week}' AND AP.dateAppointment<'{now}'  
        """
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    df= df.fillna(0)
    df['idContract']=df['idContract'].astype(int)
    df['idProduct']=df['idProduct'].astype(int)  

    print(df.columns)
    print(df.dtypes)
    print(df.head())

    #Convertir a str los campos de tipo fecha 
    cols_dates = ['dateAppointment','dateRecord']
    for col in cols_dates:
        df[col] = df[col].astype(str)
     
     
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
    schedule_interval= '35 6 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_data_citas_python_task = PythonOperator(
                                             task_id = "get_data_citas_python_task",
                                             python_callable = get_data_citas,
                                             email_on_failure=True, 
                                             email='BI@clinicos.com.co',
                                             dag=dag
                                                    )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_data_citas = MsSqlOperator(task_id='load_data_citas',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDCitas",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_data_citas_python_task >> load_data_citas >> task_end