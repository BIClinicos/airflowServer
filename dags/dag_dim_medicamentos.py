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

db_table = "TblDMedicamentos"
db_tmp_table = "TmpMedicamentos"
dag_name = 'dag_' + db_table

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def get_data_medications():

    query = f"""
        select PGD.idGenericProduct as id_MedicamentoGenerico ,PGD.genericProductChemical as [Producto_Químico_Genérico], PGD.drugConcentration as [Medicamento_Concentración], PGD.contraindications AS Contraindicaciones, PPF.name as [Presentación_Farmacéutica],PGD.posology AS Posología,PGD.adverseEffects as [Efectos_Adversos],PGD.isControlled as Controlado,PGD.isExpensive as Costoso FROM  dbo.productGenericDrugs PGD
        inner join dbo.productConfPharmaceuticalForm PPF on PGD.idPharmaceuticalForm=PPF.idPharmaceuticalForm
        """
    df = sql_2_df(query, sql_conn_id=sql_connid_gomedisys)  
    
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
    schedule_interval= "25 8 * * *",
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_data_medications_task = PythonOperator(
                                        task_id = "get_data_medications_task",
                                        python_callable = get_data_medications,
                                        email_on_failure=True, 
                                        email='BI@clinicos.com.co',
                                        dag=dag
                                        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_data_medications = MsSqlOperator(task_id='load_data_medications',
                                    mssql_conn_id=sql_connid,
                                    autocommit=True,
                                    sql="EXECUTE uspCarga_TblDMedicamentos",
                                    email_on_failure=True, 
                                    email='BI@clinicos.com.co',
                                    dag=dag
                                    )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_data_medications_task >> load_data_medications  >> task_end
    
