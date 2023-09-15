import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime
from variables import sql_connid,sql_connid_gomedisys
from utils import load_df_to_sql_pandas,sql_2_df,professional_names

import re

#  Se nombran las variables a utilizar en el dag

db_table = "tblhRehabilitacionInnovar"
db_tmp_table = "tmpRehabilitacionInnovar"
dag_name = 'dag_' + db_table

now = datetime.now()


def func_get_tblhRehabilitacionInnovar():
    # LECTURA DE DATOS  
    with open("dags/Innovar/queries/tratamientos_ordenadas.sql") as fp:
        query = fp.read().replace("{end_date}", f"{now.strftime('%Y-%m-%d')!r}")
    df_ordenadas:pd.DataFrame = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    with open("dags/Innovar/queries/tratamientos_ejecutadas.sql") as fp:
        query = fp.read().replace("{end_date}", f"{now.strftime('%Y-%m-%d')!r}")
    df_ejecutadas:pd.DataFrame = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    df_ordenadas.drop_duplicates(["idPaciente","idEHREvent"],  inplace=True)
    
    ordenamientos_pacientes = df_ordenadas["idPaciente"].unique()
    df_ejecutadas_filtradas = df_ejecutadas.loc[df_ejecutadas["idPaciente"].isin(ordenamientos_pacientes)]
    
    df_ejecutadas_filtradas.rename(columns={"FechaCita":"FechaCitaEjecutada","PaqueteEvento":"PaqueteEventoEjecutada"}, inplace=True)
    
    df_ejecutadas_filtradas.rename(columns={"idPaciente_ejecutadas":"idPaciente"}, inplace=True)
    
    df_ordenadas["FechaCita"] = pd.to_datetime(df_ordenadas["FechaCita"])
    
    df_ejecutadas_filtradas["FechaCitaEjecutada"] = pd.to_datetime(df_ejecutadas_filtradas["FechaCitaEjecutada"])
    df_ejecutadas_filtradas["date_control"] = df_ejecutadas_filtradas["FechaCitaEjecutada"] - pd.DateOffset(months=1)
    df_ejecutadas_filtradas["date_control"] = df_ejecutadas_filtradas["date_control"].dt.to_period('M')
    df_ordenadas["date_control"] = df_ordenadas["FechaCita"].dt.to_period('M')
    df_final = pd.merge(df_ordenadas,df_ejecutadas_filtradas, 'left', ['idPaciente',"date_control"])
    
    prof_col = df_final.filter(regex='^Profesional.+', axis=1).columns.to_list()
    for col in prof_col:
        df_final[col] = df_final[col].apply(professional_names)
        
    df_final.drop(["date_control"], axis=1, inplace=True)
    
    # CARGA A BASE DE DATOS
    if ~df_final.empty and len(df_final.columns) >0:
        df_final.drop_duplicates(["idEHREvent","FechaCita","idAction","idPaciente","idEncounter",
        "idContrato","idDiagnostico","idProfesional","FechaCitaEjecutada","idAutorizacion"], keep='first', inplace=True)
        load_df_to_sql_pandas(df_final,db_tmp_table, sql_connid)



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
    # Se establece la ejecución del dag a las 4:00 am (hora servidor) todos los dias
    schedule_interval= '0 4 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')
    

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    func_get_tblhRehabilitacionInnovar_python_task = PythonOperator(task_id = "get_tblhRehabilitacionInnovar",
                                                        python_callable = func_get_tblhRehabilitacionInnovar,
                                                        # email_on_failure=True, 
                                                        # email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_tblhRehabilitacionInnovar = MsSqlOperator(task_id='Load_tblhRehabilitacionInnovar',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql=f"EXECUTE SP_tblhRehabilitacionInnovar",
                                            email_on_failure=True, 
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>  func_get_tblhRehabilitacionInnovar_python_task >> load_tblhRehabilitacionInnovar >> task_end