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
from utils import sql_2_df,load_df_to_sql_2


#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'TmpTblHActividadesPersonalizadas'
db_table = "TblHActividadesPersonalizadas"
dag_name = 'dag_' + db_table

# Para correr manualmente las fechas
#fecha_texto = '2021-01-01'
#last_week = datetime.strptime(fecha_texto, '%Y-%m-%d')

# Para correr fechas con delta
now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

# Rezago de una semana



def func_get_custom_activities():

    print('Fecha inicio: ', last_week)
    
    custom_activities_query = f"""
	select
		CUAC.idEvent	[idEvento]
		,CUAC.idConfigActivity	[idCon]
		,CUAC.idElement	[idElemento]
		,CUAC.laterality	[lateralidad]
		,CUAC.valueNumeric	[valorNumero]
		,CUAC.valueText	[valorTexto]
		,EVEN.actionRecordedDate  [fechaRegistro]
		,EVEN.idPatient	[idPaciente]
	from EHREventCustomActivities CUAC
	inner join EHREvents EVEN on CUAC.idEvent = EVEN.idEHREvent
	where CUAC.idConfigActivity = 179 and
	EVEN.actionRecordedDate >= '{last_week}'
    """
    # Ejecutar la consulta capturandola en un dataframe
    df = sql_2_df(custom_activities_query, sql_conn_id=sql_connid_gomedisys)

    #Convertir a str los campos de tipo fecha 
    cols_dates = ['fechaRegistro']
    for col in cols_dates:
        df[col] = df[col].astype(str)

    print(df.columns)
    print(df.dtypes)
    print(df.head())

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql_2(df, db_tmp_table, sql_connid)


# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}

with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag todos los PENDIENTE
    schedule_interval= '45 7 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    

    get_custom_activities = PythonOperator(
                                     task_id = "get_custom_activities",
                                     python_callable = func_get_custom_activities,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
 
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_custom_activities = MsSqlOperator(task_id='load_custom_activities',
                                          mssql_conn_id=sql_connid,
                                          autocommit=True,
                                          sql="EXECUTE uspCarga_TblHActividadesPersonalizadas",
                                          email_on_failure=True, 
                                          email='BI@clinicos.com.co',
                                          dag=dag
                                         )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')


start_task >> get_custom_activities >> load_custom_activities >> task_end
