import os
import re
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta
from datetime import date
import numpy as np
import pandas as pd
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df,load_df_to_sql_pandas,normalize_strings



#  Se nombran las variables a utilizar en el dag
db_tmp_table = 'TmpHonorariosCitasModeloPrimaria'
db_table = "TblHHonorariosCitasModeloPrimaria"
dag_name = 'dag_' + db_table

QUERY_PATH = "dags/reporte_honorarios/queries/Honorarios"

# Para correr fechas con delta
now = datetime.now()
last_week = now - timedelta(weeks=1)
last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')
now = now.strftime('%Y-%m-%d %H:%M:%S')

# Para correr manualmente las fechas
#fecha_texto = '2023-04-19 00:00:00'
#now = datetime.strptime(fecha_texto, '%Y-%m-%d %H:%M:%S')
#last_week=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#now = now.strftime('%Y-%m-%d %H:%M:%S')
#last_week = last_week.strftime('%Y-%m-%d %H:%M:%S')

def func_get_honoraries_stating ():
    with open(os.path.join(QUERY_PATH,"Fact_appontments.sql")) as fp:
            query = fp.read()
                    
    df_fact:pd.DataFrame = sql_2_df(query, sql_conn_id=sql_connid)
      
                  
    profesionales = df_fact[pd.isnull(df_fact["DocumentoProfesional"])]["professional"]
    
    df_cons = profesionalesdf(profesionales)
    df_fact = pd.merge(df_fact, df_cons, 'left', ['professional'])
    
    df_fact["DocumentoProfesional"] = df_fact["DocumentoProfesional_x"].fillna(df_fact["DocumentoProfesional_y"])
    df_fact["idProfesional"] = df_fact["idProfesional_x"].fillna(df_fact["idProfesional_y"])
    df_fact["TipoDocumentoProfesional"] = df_fact["TipoDocumentoProfesional_x"].fillna(df_fact["TipoDocumentoProfesional_y"])
    
    df_fact.drop(["DocumentoProfesional_x","DocumentoProfesional_y","idProfesional_x","idProfesional_y","TipoDocumentoProfesional_x","TipoDocumentoProfesional_y"], axis=1, inplace=True)
    
    professional = ",".join([f"{val!r}" for val in df_fact["DocumentoProfesional"].unique() if not pd.isnull(val) ])
    with open(os.path.join(QUERY_PATH,"THM_VYD_OPS.sql")) as fp:
            query = fp.read().replace("{professional}",professional)
            
    df_ops = sql_2_df(query, sql_conn_id=sql_connid)
    df_fact = pd.merge(df_fact, df_ops, 'left', ["DocumentoProfesional"])
    
    professional = ",".join([f"{val!r}" for val in df_fact["DocumentoProfesional"].unique() if not pd.isnull(val) ])
    with open(os.path.join(QUERY_PATH,"THM_VYD_Nomina.sql")) as fp:
            query = fp.read().replace("{professional}",professional)
    
    df_nom = sql_2_df(query, sql_conn_id=sql_connid)
    
    df_nom["DocumentoProfesional"] = df_nom["DocumentoProfesional"].astype(str)
    
    df_fact = pd.merge(df_fact, df_nom, 'left', ["DocumentoProfesional"])
    
    df_fact["fecha_ingreso"] = pd.to_datetime(df_fact["fecha_ingreso"])
    df_fact["fecha_retiro"] = pd.to_datetime(df_fact["fecha_retiro"])
    
    df_fact["Nomina"] = df_fact.apply(nomina, axis=1)
    

    query = "select idContract, name as headquarter from contracts c where idContract in (52,53,54);"
    
    df_contract = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    # Inicializa la columna Contrato con NaN
    df_fact["Contrato"] = np.nan

    # Itera a través de las filas de df_fact
    for idx, row in df_fact.iterrows():
        headquarter = row["headquarter"]
        
        # Verifica si headquarter está contenido en alguna fila de df_contract["headquarter"]
        condition = df_contract["headquarter"].str.contains(headquarter, case=False, na=False)
        
        # Si hay al menos una coincidencia, asigna el valor correspondiente desde df_contract
        if condition.any():
            df_fact.at[idx, "Contrato"] = df_contract.loc[condition, "headquarter"].values[0]
    
    
    query = "select idOffice, name headquarter from companyOffices where idOffice in (2,3,12)"
    
    df_offices = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    df_fact["Sede"] = np.nan

    # Itera a través de las filas de df_fact
    for idx, row in df_fact.iterrows():
        headquarter = row["headquarter"]
        
        # Verifica si headquarter está contenido en alguna fila de df_contract["headquarter"]
        condition = df_offices["headquarter"].str.contains(headquarter, case=False, na=False)
        
        # Si hay al menos una coincidencia, asigna el valor correspondiente desde df_contract
        if condition.any():
            df_fact.at[idx, "Sede"] = df_offices.loc[condition, "headquarter"].values[0]
            
    cups = [f"{val!r}" for val in df_fact['cups'].unique().tolist() if val]
    query = f"SELECT idProduct,name as Producto, legalCode as cups from products where legalCode in ({','.join(cups)})"
    
    df_products = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    df_fact = pd.merge(df_fact, df_products, 'left', ["cups"])
    
    
    df_fact.loc[df_fact["service"].str.contains("FISIOTERAPEUTAS", na=False), "service"] = "TERAPIA FÍSICA"
    df_fact["service"] = df_fact["service"].apply(lambda x: re.sub(r"CONTOL\s*DE\s*TERAPIA|TERAPEUTA|CONTROL DE TERAPIA", "TERAPIA", str(x), flags=re.IGNORECASE))
    df_fact.loc[df_fact["service"].str.contains("ECOCARDIOGRAMA", na=False), "service"] = "CARDIOLOGÍA"
    df_fact.loc[df_fact["service"].str.contains("MEDICINA FISICA Y REHABILITACION", na=False), "service"] = "FISIATRÍA"
    df_fact.loc[df_fact["service"].str.contains("NUTRICION", na=False), "service"] = "NUTRICIÓN"
    df_fact.loc[df_fact["service"].str.contains("OPTÓMETRA", na=False), "service"] = "OPTOMETRÍA"
    
    df_fact.loc[pd.isnull(df_fact["cups"]),"cups"]= df_fact[pd.isnull(df_fact["cups"])]["service"].apply(func_cups_service)
    
    df_service = df_fact.loc[pd.isnull(df_fact["Producto"]),["service","consultation_type"]].drop_duplicates()
    
    df_cons = func_df_cons(df_service)
    
    df_fact = pd.merge(df_fact, df_cons, 'left',["service","consultation_type"])
    
    df_fact["cups"] = df_fact["cups_x"].fillna(df_fact["cups_y"])
    df_fact["Producto"] = df_fact["Producto_x"].fillna(df_fact["Producto_y"])
    
    df_fact.drop(["cups_x","cups_y","Producto_x","Producto_y"], axis=1, inplace=True)
    
    cups = [f"{val!r}" for val in df_fact[pd.isnull(df_fact["Producto"])]['cups'].unique().tolist() if not pd.isnull(val)]
    query = f"SELECT idProduct,name as Producto, legalCode as cups from products where legalCode in ({','.join(cups)})"
    df_products = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
    df_fact = pd.merge(df_fact, df_products, 'left', ["cups"])
    
    df_fact["idProduct"] = df_fact["idProduct_x"].fillna(df_fact["idProduct_x"])
    df_fact["Producto"] = df_fact["Producto_x"].fillna(df_fact["Producto_y"])
    
    df_fact.drop(["idProduct_x","idProduct_y","Producto_x","Producto_y"],axis=1, inplace=True)
    
    df_service = df_fact.loc[pd.isnull(df_fact["Producto"]),["service","consultation_type"]].drop_duplicates()
    
    new_df_prod = pd.merge(df_fact[pd.isnull(df_fact["Producto"])], df_cons, 'left',["service"])
    
    new_df_prod["Producto"] = new_df_prod["Producto_x"].fillna(new_df_prod["Producto_y"])
    new_df_prod["cups"] = new_df_prod["cups_x"].fillna(new_df_prod["cups_y"])
    
    new_df_prod.drop(["Producto_x","Producto_y","cups_x","cups_y"], axis=1, inplace=True)
    
    df_fact = pd.merge(df_fact, new_df_prod[["id","Producto","cups"]], "left","id")
    
    df_fact["Producto"] = df_fact["Producto_x"].fillna(df_fact["Producto_y"])
    df_fact["cups"] = df_fact["cups_x"].fillna(df_fact["cups_y"])
    
    df_fact.drop(["Producto_x","Producto_y","cups_x","cups_y"], axis=1, inplace=True)
    
    load_df_to_sql_pandas(df_fact, db_tmp_table, sql_connid, truncate = False)
    
def func_df_cons_no_consultation(df_service:pd.DataFrame) -> pd.DataFrame:
    query = "WITH CTE AS ("
    df_service["service_replace"] = df_service['service'].str.replace('[ÁÉÍÓÚáéíóú]', '_', regex=True)
    for _,row in df_service.iterrows():
        if (row["service"]):
            if query.endswith("AS ("):
                query += f"""
                SELECT TOP 1 WITH TIES
                        {row['service']!r} AS service,
                        name AS Producto,
                        MIN(legalCode) AS cups
                    FROM products
                WHERE name LIKE '%{row['service_replace']}%' 
                    AND idProductType = 4
                GROUP BY name, legalCode
                ORDER BY ROW_NUMBER() OVER ( ORDER BY legalCode)"""
                
            else:
                query += f"""
                \nUNION ALL\n
                SELECT TOP 1 WITH TIES
                        {row['service']!r} AS service,
                        name AS Producto,
                        MIN(legalCode) AS cups
                    FROM products
                WHERE name LIKE '%{row['service_replace']}%' 
                    AND idProductType = 4
                GROUP BY name, legalCode
                ORDER BY ROW_NUMBER() OVER ( ORDER BY legalCode)"""
    query += ")\n SELECT * FROM CTE"
    return sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
    
def func_df_cons(df_service:pd.DataFrame) -> pd.DataFrame:
    query = ""
    df_service["service_replace"] = df_service['service'].str.replace('[ÁÉÍÓÚáéíóú]', '_', regex=True)
    for _,row in df_service.iterrows():
        if (row["service"] and row["consultation_type"]):
            
            if query:
                query += f"\nUNION ALL\n(select top 1 {row['service']!r} service, {row['consultation_type']!r} consultation_type,name Producto, min(legalCode) cups from products where name like '%{row['service_replace']}%' AND name like '%{row['consultation_type']}%' and idProductType = 4 group by name ) "
            else:
                query += f"(select top 1 {row['service']!r} service, {row['consultation_type']!r} consultation_type,name Producto, min(legalCode) cups from products where name like '%{row['service_replace']}%' AND name like '%{row['consultation_type']}%' AND idProductType = 4 group by name)"
        
    return sql_2_df(query, sql_conn_id=sql_connid_gomedisys)
        
    
def func_cups_service(x:str):
    enf = set(map(str,map(normalize_strings, ["VEJEZ", "ADULTEZ", "JUVENTUD","JOVEN","JEFE", "INFANCIA", "GESTIÓN DEL RIESGO","GESTION DEL RIESGO","ENFERMERÍA","ENFERMERIA"])))

    if pd.isnull(x):
        return None
    
    normalized_x = str(normalize_strings(x)).lower().split() + str(normalize_strings(x)).lower().split("_")
    if any(val.lower() in enf for val in normalized_x) or "ENFERMERIA" in x.upper():
        print(normalized_x)
        return "890305"
    
    if "mega" in str(normalize_strings(x)).lower():
        
        return "890301"
    
    if "trabajo social" in str(normalize_strings(x)).lower():
        return "890309"
    
    return None

def nomina(row:pd.Series):
    if not pd.isnull(row["fecha_ingreso"]) and not pd.isnull(row["fecha_retiro"]):
        if row["appointment_start_date"] >= row["fecha_ingreso"] and row["appointment_start_date"] <= row["fecha_retiro"]:
            return "SI"
        else: return "NO"
    elif not pd.isnull(row["fecha_ingreso"]):
        if row["appointment_start_date"] >= row["fecha_ingreso"]:
            return "SI"
        else: return "NO"
    else: None
    
def profesionalesdf(profesionales:pd.Series):
    list_profesionales = [name.replace("  "," ").split() for name in profesionales.unique()]
    query = ""
    for prof in list_profesionales:
        
        if len(prof) == 4:
            if query:
                query += f"\nUNION ALL\n(SELECT top 1 '{' '.join(prof)}' as professional, idUser as idProfesional, Documento as DocumentoProfesional, Tipo_Documento TipoDocumentoProfesional FROM TblDUsuarios where Nombres_Completos like '%{prof[0]}%{prof[2]}%' and idSpecialty <> 0) "
            else:
                query += f"(SELECT top 1 '{' '.join(prof)}' as professional,idUser as idProfesional, Documento as DocumentoProfesional, Tipo_Documento TipoDocumentoProfesional FROM TblDUsuarios where Nombres_Completos like '%{prof[0]}%{prof[2]}%' and idSpecialty <> 0) "
        elif len(prof) == 3:
            if query:
                query += f"\nUNION ALL\n(SELECT top 1 '{' '.join(prof)}' as professional,idUser as idProfesional, Documento as DocumentoProfesional, Tipo_Documento TipoDocumentoProfesional FROM TblDUsuarios where (Nombres_Completos like '%{prof[0]}%{prof[2]}%' OR Nombres_Completos like '%{prof[0]}%{prof[1]}%') and idSpecialty <> 0)"
            else:
                query += f"(SELECT top 1 '{' '.join(prof)}' as professional,idUser as idProfesional, Documento as DocumentoProfesional, Tipo_Documento TipoDocumentoProfesional FROM TblDUsuarios where (Nombres_Completos like '%{prof[0]}%{prof[2]}%' OR Nombres_Completos like '%{prof[0]}%{prof[1]}%') and idSpecialty <> 0)"
            
        elif len(prof) == 2:
            if query:
                query +=f"\nUNION ALL\n(SELECT top 1 '{' '.join(prof)}' as professional,idUser as idProfesional, Documento as DocumentoProfesional, Tipo_Documento TipoDocumentoProfesional FROM TblDUsuarios where Nombres_Completos like '%{prof[0]}%{prof[1]}%' and idSpecialty <> 0)"
            else:
                query +=f"(SELECT top 1 '{' '.join(prof)}' as professional,idUser as idProfesional, Documento as DocumentoProfesional, Tipo_Documento TipoDocumentoProfesional FROM TblDUsuarios where Nombres_Completos like '%{prof[0]}%{prof[1]}%' and idSpecialty <> 0)"
                  
    return sql_2_df(query, sql_conn_id=sql_connid)



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
    schedule_interval='55 6 * * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    

    
    get_honoraries_stating= PythonOperator(
                                     task_id = "get_honoraries_stating",
                                     python_callable = func_get_honoraries_stating,
                                     email_on_failure=True, 
                                     email='BI@clinicos.com.co',
                                     dag=dag
                                     )
    
    
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    # load_fact_honoraries_scheduled_appointments = MsSqlOperator(task_id='load_fact_honoraries_scheduled_appointments',
    #                                       mssql_conn_id=sql_connid,
    #                                       autocommit=True,
    #                                       sql="EXECUTE uspCarga_TblHHonorariosCitasModeloEspecializada",
    #                                       email_on_failure=True, 
    #                                       email='BI@clinicos.com.co',
    #                                       dag=dag
    #                                      )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_honoraries_stating>>task_end
#start_task >> extract_cita_detail >>get_cita_detail>>task_end
