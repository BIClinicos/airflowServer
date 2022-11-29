from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime
import pandas as pd

from utils import load_df_to_sql
from variables import sql_connid
from airflow.hooks.mssql_hook import MsSqlHook


default_args = {
    'owner': 'Clinicos',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

def mssql_func(**kwargs):
    conn = MsSqlHook.get_connection(sql_connid)
    hook = conn.get_hook()
    print('connection OK')
    df = hook.get_pandas_df(sql="SELECT * FROM [dbo].[BI_Ecopetrol_Queues]")
    print(df)
    print('termine')



with DAG('dag_test_db_clinicos',
         default_args=default_args,
         schedule_interval=None,
         max_active_runs=1) as dag:

    start_task = DummyOperator(task_id='dummy_start2')

    reading_db = PythonOperator(
        task_id='reading_db2',
        python_callable=mssql_func,
    )


start_task >> reading_db