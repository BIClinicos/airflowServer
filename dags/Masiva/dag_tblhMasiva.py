import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta
from NEPS.utils.utils import generar_rango_fechas
from variables import sql_connid,sql_connid_gomedisys
from utils import load_df_to_sql_pandas,get_engine
from sqlalchemy.orm import Session
#  Se nombran las variables a utilizar en el dag

db_table = "tblhMasivaNEPS"
db_tmp_table = "tmpMasivaNEPS"
dag_name = 'dag_' + db_table

now = datetime.now()
last_week_date = now - timedelta(days=1)
# last_week_date = datetime(2023,6,1)
last_week = last_week_date.strftime('%Y-%m-%d %H:%M:%S')


def func_get_tblhMasivaNEPS():
    # LECTURA DE DATOS  
    with open("dags/Masiva/queries/Masiva_creacion.sql") as fp:
        query = fp.read().replace("{last_week}", f"{last_week_date.strftime('%Y-%m-%d')!r}")
    engine = get_engine(sql_connid_gomedisys)
    session = Session(engine)
    session.execute(query)
    session.commit()
    with open("dags/Masiva/queries/Masiva.sql") as fp:
        query = fp.read()
    df = pd.read_sql_query(query, session.bind)
    session.close()
    engine.dispose()
    print(df.info())
    # CARGA A BASE DE DATOS
    if ~df.empty and len(df.columns) >0:
        load_df_to_sql_pandas(df,db_tmp_table, sql_connid, truncate=False)
    

def delete_temp_range():
     query = f"""
     delete from {db_tmp_table} where date_control >='{last_week_date.strftime("%Y-%m-%d")}' AND date_control <'{now.strftime("%Y-%m-%d")}'
     """
     hook = MsSqlHook(sql_connid)
     hook.run(query)



# Se declara un objeto con los parámetros del DAG
default_args = {
    'owner': 'clinicos',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 1),
}

# Se declara el DAG con sus respectivos parámetros
with DAG(dag_name,
    catchup=False,
    default_args=default_args,
    # Se establece la ejecución del dag a las 1:00 am (hora servidor) todos los Domingos
    schedule_interval= '0 2 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')
    
    # delete_temp_range_python= PythonOperator(
    #                                  task_id = "delete_temp_range_python",
    #                                  python_callable = delete_temp_range,
    #                                  email_on_failure=True, 
    #                                  email='BI@clinicos.com.co',
    #                                  dag=dag
    #                                  )

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    func_get_tblhMasivaNEPS_python_task = PythonOperator(task_id = "func_get_tblhMasivaNEPS",
                                                        python_callable = func_get_tblhMasivaNEPS,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # # Se declara la función encargada de ejecutar el "Stored Procedure"
    # load_tblhEspecializadaNEPS = MsSqlOperator(task_id='Load_tblhEspecializadaNEPS',
    #                                         mssql_conn_id=sql_connid,
    #                                         autocommit=True,
    #                                         sql=f"EXECUTE SP_tblhEspecializadaNEPS",
    #                                         email_on_failure=True, 
    #                                         email='BI@clinicos.com.co',
    #                                         dag=dag
    #                                    )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> func_get_tblhMasivaNEPS_python_task >> task_end