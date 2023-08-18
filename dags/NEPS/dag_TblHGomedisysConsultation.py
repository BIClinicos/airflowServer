import os
import pandas as pd
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta

from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,load_df_to_sql_pandas,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name

#  Se nombran las variables a utilizar en el dag

db_table = "TblHGomedisysConsultation"
db_tmp_table = "tmpConsultationGomedisys"
dag_name = 'dag_' + db_table

now = datetime.now()
last_week_date = now - timedelta(weeks=1)
# last_week_date = datetime(2023,7,1)
last_week = last_week_date.strftime('%Y-%m-%d %H:%M:%S')

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_ConsultationGomedisys ():
    # LECTURA DE DATOS  
    with open("dags/NEPS/queries/ConsultationGomedisys.sql") as fp:
        query = fp.read().replace("{last_week}", f"{last_week_date.strftime('%Y-%m-%d')!r}")
    df:pd.DataFrame = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    # CARGA A BASE DE DATOS
    if ~df.empty and len(df.columns) >0:
        df["FechaActividad"] = df["FechaActividad"].astype(str)
        df["Fecha_Atencion"] = df["Fecha_Atencion"].astype(str)
        
        load_df_to_sql_pandas(df, db_tmp_table, sql_connid,["date_control","idUser"])


def delete_temp_range():
     query = f"""
     delete from {db_tmp_table} where convert(date, FechaActividad) >='{last_week_date.date()}' AND convert(date, FechaActividad) <'{now.date()}'
     """
     hook = MsSqlHook(sql_connid)
     hook.run(query)



# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
}

# Se declara el DAG con sus respectivos parámetros
with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag a las 12:00 am (hora servidor) todos los Domingos
    schedule_interval= '0 0 * * 0',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')
    
    delete_temp_range_python= PythonOperator(
                                     task_id = "delete_temp_range_python",
                                     python_callable = delete_temp_range,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_ConsultationGomedisys_python_task = PythonOperator(task_id = "get_ConsultationGomedisys",
                                                        python_callable = func_get_ConsultationGomedisys,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_ConsultationGomedisys = MsSqlOperator(task_id='Load_ConsultationGomedisys',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql=f"EXECUTE SP_TblHGomedisysConsultation @date_start = {last_week_date.strftime('%Y-%m-%d')!r}",
                                            email_on_failure=True, 
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> delete_temp_range_python >> get_ConsultationGomedisys_python_task >> load_ConsultationGomedisys >> task_end