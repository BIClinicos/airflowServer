import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta
from NEPS.utils.utils import generar_rango_fechas, get_hours, other_hours
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,load_df_to_sql,update_to_sql

#  Se nombran las variables a utilizar en el dag

db_table = "TblHFormulacionOxigeno"
db_tmp_table = "tmpFormulacionOxigeno"
dag_name = 'dag_' + db_table

now = datetime.now()
# last_week_date = now - timedelta(weeks=1)
last_week_date = datetime(2023,6,1)
last_week = last_week_date.strftime('%Y-%m-%d %H:%M:%S')

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_FormulacionOxigeno ():
    # LECTURA DE DATOS  
    with open("dags/NEPS/queries/OxigenoNEPS.sql") as fp:
        query = fp.read().replace("{last_week}", f"{last_week_date.strftime('%Y-%m-%d')!r}")
    df:pd.DataFrame = sql_2_df(query, sql_conn_id=sql_connid)

    return df

def func_get_FormulacionOxigenoPlan(pacientes):
    # LECTURA DE DATOS  
    with open("dags/NEPS/queries/OxigenoNEPSPlan.sql") as fp:
        query = fp.read().replace("{last_week}", f"{last_week_date.strftime('%Y-%m-%d')!r}")
        query = query.replace("{pacientes}",",".join(map(str,pacientes)))
    df:pd.DataFrame = sql_2_df(query, sql_conn_id=sql_connid)
    df.rename(columns={"PlanTratamiento":"Recomendaciones"}, inplace=True)

    return df

def func_get_FormulacionOxigenoGom():
    # LECTURA DE DATOS  
    with open("dags/NEPS/queries/OxigenoGomedisys.sql") as fp:
        query = fp.read().replace("{last_week}", f"{last_week_date.strftime('%Y-%m-%d')!r}")
    df:pd.DataFrame = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    return df

def func_get_FormulacionOxigenoFinal():
    # LECTURA DE DATOS  
    df_main = func_get_FormulacionOxigeno()
    df_gom = func_get_FormulacionOxigenoGom()
    df_main = pd.merge(df_main, df_gom,'outer', ["idUser", "Documento", "date_control"] )
    
    #PlanTratamiento
    df_plan = func_get_FormulacionOxigenoPlan(df_main["idUser"].unique())
    df_plan.replace('"',"", inplace=True)
    df_plan.rename(columns={"PlanTratamiento":"Recomendaciones"}, inplace=True)
    
    df_main['date_control'] = pd.to_datetime(df_main['date_control'])
    df_plan['date_control'] = pd.to_datetime(df_plan['date_control'])
    
    df_main = df_main.sort_values(by=['idUser', 'date_control'])
    df_main = df_main.groupby('idUser').apply(generar_rango_fechas)
    
    df_plan = df_plan.sort_values(by=['idUser', 'date_control'])
    df_plan = df_plan.groupby('idUser').apply(generar_rango_fechas)
    
    df_last = pd.concat([df_main.reindex(),df_plan.reindex()],ignore_index=True)

    df_last[pd.isna(df_last["Horas_Oxigeno"])]["Horas_Oxigeno"] = df_last[pd.isna(df_last["Horas_Oxigeno"])].apply(get_hours, axis = 1)
    if df_last[pd.isna(df_last["Horas_Oxigeno"])].size != 0:
        mask = pd.isna(df_last["Horas_Oxigeno"])
        if mask.any():
            df_last.loc[mask, "Horas_Oxigeno"] = df_last.loc[mask, "Recomendaciones"].apply(other_hours)
    
    # CARGA A BASE DE DATOS
    if ~df_last.empty and len(df_last.columns) >0:
        for column, dtype in df_last.dtypes.iteritems():
            if isinstance(dtype,datetime):
                df_last[column] = df_last[column].astype(str)
        df_last.drop_duplicates(["idUser","date_control"], keep='last', inplace=True)
        df_last["date_control"] = df_last["date_control"].astype(str)
        print(df_last.info())
        load_df_to_sql(df_last, db_tmp_table, sql_connid)
        
        
def update_FormulacionOxigenoFinal():
    data:pd.DataFrame = sql_2_df("select * from tmpFormulacionOxigeno where horas_oxigeno is null", sql_conn_id=sql_connid)
    
    # Aplicar la función other_hours si la columna "Horas_Oxigeno" está nula
    mask = pd.isna(data["Horas_Oxigeno"])
    if mask.any():
        data.loc[mask, "Horas_Oxigeno"] = data.loc[mask, "Recomendaciones"].apply(other_hours)

    # Aplicar la función custom_hours con un patrón específico si la columna "Horas_Oxigeno" sigue estando nula
    mask = pd.isna(data["Horas_Oxigeno"])
    if mask.any():
        data.loc[mask, "Horas_Oxigeno"] = data.loc[mask, "Recomendaciones"].apply(
            other_hours, pather=(r"(?:^| )(?:(?:o2|ox)i?g?e?n?o?)[^\d\n]*(\d+\s*(?:hrs?|h))\b"
        ))
        
    query = "UPDATE tmpFormulacionOxigeno SET Horas_Oxigeno = %s WHERE idUser = %s AND date_control = %s;"
    params = [(row["Horas_Oxigeno"], row["idUser"], row["date_control"].strftime('%Y-%m-%d')) 
                    for _, row in data.iterrows() if pd.notnull(row["Horas_Oxigeno"])]
    update_to_sql(params,sql_connid, query)
    


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
    schedule_interval= '0 1 * * 0',
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
    get_FormulacionOxigeno_python_task = PythonOperator(task_id = "get_FormulacionOxigeno",
                                                        python_callable = func_get_FormulacionOxigenoFinal,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    update_FormulacionOxigeno_python_task = PythonOperator(task_id = "update_FormulacionOxigenoFinal",
                                                        python_callable = update_FormulacionOxigenoFinal,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_ConsultationGomedisys = MsSqlOperator(task_id='Load_ConsultationGomedisys',
                                            mssql_conn_id=sql_connid,
                                            autocommit=True,
                                            sql=f"EXECUTE SP_TblHFormulacionOxigeno @date_start = {last_week_date.strftime('%Y-%m-%d')!r}",
                                            email_on_failure=True, 
                                            email='BI@clinicos.com.co',
                                            dag=dag
                                       )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> delete_temp_range_python >> get_FormulacionOxigeno_python_task >> update_FormulacionOxigeno_python_task >> load_ConsultationGomedisys >> task_end