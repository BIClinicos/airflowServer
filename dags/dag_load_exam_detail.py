import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.sensors.external_task_sensor  import ExternalTaskSensor
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,load_df_to_sql_2



#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'TmpExamenesDetalles'
db_table = "TblHExamanesDetalles"
dag_name = 'dag_' + db_table


#Se halla las fechas de cargue de la data 
now = datetime.now()
#fecha_texto = '2023-03-14 04:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-01-01 04:00:00', '%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

#year = last_week.year
#month = last_week.month

def func_get_examen_detail ():

    print('Fecha inicio ', last_week)
    print('Fecha fin ', now)
    
    domiConsultas_query = f"""
    SELECT ASS.idAppointmentSchedulerSlots,ASCH.idAppointmenScheduler,AES.idAppointmentExam,ASCH.dateBegin
    FROM dbo.appointmentSchedulers ASCH  WITH (NOLOCK)
    INNER JOIN dbo.appointmentSchedulerSlots ASS WITH (NOLOCK) ON ASCH.idAppointmenScheduler=ASS.idAppointmentScheduler
    INNER JOIN dbo.appointmentExamSchedulers AES WITH (NOLOCK) ON ASCH.idAppointmenScheduler=AES.idAppointmentScheduler
    WHERE ASCH.dateBegin >='{last_week}' AND ASCH.dateBegin<'{now}'
    """
    # Ejecutar la consulta capturandola en un dataframe
    df = sql_2_df(domiConsultas_query, sql_conn_id=sql_connid_gomedisys)
    
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['dateBegin']
    for col in cols_dates:
        df[col] = df[col].astype(str)

    print(df.columns)
    print(df.dtypes)
    print(df)

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql_2(df, db_tmp_table, sql_connid)


def execute_Sql():
     query = f"""
     delete from TmpExamenesDetalles where dateBegin >='{last_week}' AND dateBegin<'{now}'
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
    schedule_interval= '25 6 * * *',
    max_active_runs=1
    ) as dag:

    #wait_for_exam_staging = ExternalTaskSensor(
    #task_id='wait_for_exam_staging',
    #external_dag_id='dag_TblDExamenes',
    #external_task_id='task_end',
    #execution_delta = timedelta(minutes=5),
    #dag=dag)s

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    
    extract_examen_detail= PythonOperator(
                                     task_id = "extract_examen_detail",
                                     python_callable = execute_Sql,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    get_examen_detail= PythonOperator(
                                     task_id = "get_examen_detail",
                                     python_callable = func_get_examen_detail,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_fact_examen_detail = MsSqlOperator(task_id='load_fact_examen_detail',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql="EXECUTE uspCarga_TblHExamanesDetalles",
                                            email_on_failure=True, 
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                            )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

#start_task >> get_examen_detail >> load_fact_examen_detail >> task_end
start_task >> extract_examen_detail >>get_examen_detail>> load_fact_examen_detail>>task_end
