import os
import xlrd
import re
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from utils import remove_accents_cols, regular_snake_case, remove_special_chars, normalize_str_categorical
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

dag_name = 'dag_fact_appointments_bw_compensar_repair_cups'

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
    # Se establece la ejecución del dag todos los viernes a las 10:00 am(Hora servidor)
    schedule_interval= '@once',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_repair_general_medicine = MsSqlOperator(task_id='load_repair_general_medicine',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_fact_appointments_bw_compensar_repair_general_medicine",
                                    #    email_on_failure=True, 
                                    #    email='BI@clinicos.com.co'
                                       )

    load_repair_general_surgery = MsSqlOperator(task_id='load_repair_general_surgery',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_fact_appointments_bw_compensar_repair_general_surgery",
                                    #    email_on_failure=True, 
                                    #    email='BI@clinicos.com.co'
                                       )

    load_repair_gynecology = MsSqlOperator(task_id='load_repair_gynecology',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_fact_appointments_bw_compensar_repair_gynecology",
                                    #    email_on_failure=True, 
                                    #    email='BI@clinicos.com.co'
                                       )

    load_repair_gynecology_obstetrics = MsSqlOperator(task_id='load_repair_gynecology_obstetrics',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_fact_appointments_bw_compensar_repair_gynecology_obstetrics",
                                    #    email_on_failure=True, 
                                    #    email='BI@clinicos.com.co'
                                       )

    load_repair_internal_medicine = MsSqlOperator(task_id='load_repair_internal_medicine',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_fact_appointments_bw_compensar_repair_internal_medicine",
                                    #    email_on_failure=True, 
                                    #    email='BI@clinicos.com.co'
                                       )

    load_repair_odontology = MsSqlOperator(task_id='load_repair_odontology',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_fact_appointments_bw_compensar_repair_odontology",
                                    #    email_on_failure=True, 
                                    #    email='BI@clinicos.com.co'
                                       )

    load_repair_pediatrics = MsSqlOperator(task_id='load_repair_pediatrics',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_fact_appointments_bw_compensar_repair_pediatrics",
                                    #    email_on_failure=True, 
                                    #    email='BI@clinicos.com.co'
                                       )

    load_fact_appointments_bw_compensar_cups_repaired = MsSqlOperator(task_id='load_fact_appointments_bw_compensar_cups_repaired',
                                       mssql_conn_id=sql_connid,
                                       autocommit=True,
                                       sql="EXECUTE sp_load_fact_appointments_bw_compensar_cups_repaired",
                                    #    email_on_failure=True, 
                                    #    email='BI@clinicos.com.co'
                                       )
    

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> load_repair_general_medicine >> load_repair_general_surgery >> load_repair_gynecology >> load_repair_gynecology_obstetrics >> load_repair_internal_medicine >> load_repair_odontology >> load_repair_pediatrics >> load_fact_appointments_bw_compensar_cups_repaired >> task_end