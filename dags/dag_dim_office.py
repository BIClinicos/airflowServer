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

db_table = "TblDOficinas"
db_tmp_table = "TmpOficinas"
dag_name = 'dag_' + db_table
### mensual 40 2 6 * *
# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def get_data_officies():

    query = f"""
        SELECT idOffice,name,director,directorIdentification,isActive,valueLocation,registrationDate
        FROM dbo.companyOffices COF WITH (NOLOCK) 
        """
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    #Convertir a str los campos de tipo fecha 
    cols_dates = ['registrationDate']
    for col in cols_dates:
        df[col] = df[col].astype(str)
    
    print(df.columns)
    print(df.dtypes)
    print(df)

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
    schedule_interval= '@monthly',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_data_officies_python_task = PythonOperator(
                                                        task_id = "get_data_officies_python_task",
                                                        python_callable = get_data_officies,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_data_officies = MsSqlOperator(task_id='load_data_officies',
                                        mssql_conn_id=sql_connid,
                                        autocommit=True,
                                        sql="EXECUTE uspCarga_TblDOficinas",
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_data_officies_python_task >> load_data_officies >> task_end