import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators import email_operator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from variables import sql_connid,sql_connid_gomedisys
from utils import sql_2_df

#  Se nombran las variables a utilizar en el dag
dag_name = 'dag_' + 'TEC_PYR_GEFDispensacionReporte'
dirname = '/opt/airflow/dags/generated_files/'

# Retorna el dia  hábil x dias atras
def get_working_day(c_date,days_ago=3):
  counter=days_ago
  if counter==0:
    return c_date
  if c_date.weekday() in (0,1,2,3,4):
    counter=counter-1
    past_day = c_date + timedelta(days=-1)
    return get_working_day(past_day,counter)
  else:
    past_day = c_date + timedelta(days=-1)
    return get_working_day(past_day,counter)

# Fecha de ejecución del dag
today = date.today()
month = today.month
year = today.year

# Fechas usadas para filtrar data de query de reporte en una venta de tiempo

# Dia 1 del mes anterior a las 0:00 horas
"""
#Esta fórmula presentó errores el 01/01/2023 por tanto se generó un nuevo cálculo para la fecha
past_month_date = datetime(year,month-1,1)"""
last_day = today - timedelta(today.day)
past_month_date = last_day - timedelta(last_day.day -1)

#Se calculan los valores de mes y año para el asunto del correo 
month_subj = past_month_date.month
year_subj = past_month_date.year

# Día 1 del mes actual a las 0:00 horas
first_month_execution_date = datetime(year,month,1)

# 3 días habiles atras del dia 1 del mes anterior a las 0:00 horas
get_working_ago_date = get_working_day(past_month_date,3)

# Formato de fechas para usar en query
past_month_date = past_month_date.strftime('%Y-%m-%d')
get_working_ago_date = get_working_ago_date.strftime('%Y-%m-%d')

# Nombre estandar de reportes generados (.csv,.xlsx)
#filename = f'Reporte_{year}-{month-1}.csv'
filename2 = f'Reporte_{year_subj}-{month_subj}.xlsx'

# Función de generacion de reporte y envío via email.
def func_get_TEC_PYR_GEFDispensacion ():

    print(past_month_date)
    print(get_working_ago_date)
    
    dispensing_query = f"""
        SELECT 
        T_DIS.firstGivenName as nombre1,
        T_DIS.secondGiveName as nombre22,
        T_DIS.[firstFamilyName]  AS apellido1,
        T_DIS.[secondFamilyName]  AS apellido2,
        T_DIS.[idUser],
        T_DIS.[idType] AS 'tipo de identificación',
        T_DIS.[identificationNumber] AS 'número de identificación',
        T_DIS.[gender] AS 'género',
        T_DIS.[homeAddress] AS 'dirección de residencia',
        T_DIS.[city] AS 'ciudad',
        T_DIS.[departament] AS 'departamento',
        T_DIS.[telecom] AS 'teléfono/celular',
        'Clinicos Programas de Atención Integral S.A.S IPS' AS 'Nombre de la Institución',
        110012347106 AS 'Código',
        T_DIS.[genericProductChemical] AS 'MEDICAMENTO RECETADO CON DENOMINACIÓN COMÚN',
        T_DIS.drugConcentration AS 'CONCENTRACIÓN',
        T_DIS.[pharmaceuticalForm] AS 'FORMA FARMACÉUTICA',
        T_FORM_MED.[administrationRoute] AS 'VÍA DE ADMINISTRACIÓN',
        T_FORM_MED.[periodicity] AS 'Frecuencia',
        T_FORM_MED.valueAdministrationTime AS 'Duración del tratamiento',
        T_FORM_MED.formulatedAmount AS 'Cantidad prescrita',
        T_DIS.dispensedQuantity AS 'Cantidad entregada',
        T_DIS.diagnostic AS 'Diagnósticos',
        T_FORM_MED.dateStart AS 'Fecha prescripción del medicamento',
        T_FORM_MED.dateRecorded AS 'Fecha autorización del medicamento',
        T_DIS.dateRecord  AS 'Fecha dispensación',
        T_DIS.formulation  AS 'Fórmula',
        T_DIS.pharmacy AS 'Dispensación',
        T_DIS.namePractitioner AS 'Nombre medico',
        T_DIS.idPractitioner AS 'ID_Medico'
        FROM [dbo].[TEC_PYR_GEFDispensacion] T_DIS 
        LEFT JOIN [dbo].[TEC_PYR_GEFMedicamentos] T_MED ON T_DIS.[idGenericProduct] = T_MED.[idGenericProduct] 
        LEFT JOIN (SELECT T_FORM.*,T_MED2.number FROM [dbo].[TEC_PYR_GEFFormulacion] T_FORM LEFT JOIN [dbo].[TEC_PYR_GEFMedicamentos] T_MED2 ON T_FORM.[idProductGeneric] = T_MED2.[idGenericProduct]) T_FORM_MED
        ON T_FORM_MED.number = T_MED.number AND
        T_DIS.idUser = T_FORM_MED.idUser AND
        T_DIS.idEncounter = T_FORM_MED.idEncounter
        WHERE
        T_FORM_MED.idUser IS NOT NULL AND
        T_DIS.dateRecord BETWEEN '{past_month_date}' AND '{first_month_execution_date}' AND
        T_FORM_MED.dateRecorded BETWEEN '{get_working_ago_date}' AND '{first_month_execution_date}'
    """
    # Dispensacion y formulacion debe ser maximo hasta el último día del mes
    df = sql_2_df(dispensing_query)
    
    print(df.columns)
    print(df.dtypes)
    print(df)

    # df.to_csv(dirname+filename, index=False)
    df.to_excel(dirname+filename2, index=False)

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
    # Se establece el cargue de los datos el día 3 de cada mes.
    schedule_interval= '20 5 3 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    # Se declara y se llama la función encargada de generar el reporte
    get_TEC_PYR_GEFDispensacion_python_task = PythonOperator(task_id = "get_TEC_PYR_GEFDispensacion",
        python_callable = func_get_TEC_PYR_GEFDispensacion,
        email_on_failure=True, 
        email='BI@clinicos.com.co',
        dag=dag
        )
    
    # Se declara la función encargada de enviar por correo los reportes generados
    email_summary = email_operator.EmailOperator(
        task_id='email_summary',
        #to=['BI@clinicos.com.co'],
        to=['BI@clinicos.com.co', 'kcastellanos@clinicos.com.co', 'quimico.farmaceutico@clinicos.com.co'],
        subject=f'Reporte Dispensacion {year_subj}-{month_subj}', 
        html_content=f"""<p>Saludos, adjunto se envía reporte de gomedisys para gestión farmacéutica del periodo {year_subj}-{month_subj}
        (mail creado automaticamente).</p>
        <br/>
        """,
        # files=[f'{dirname}{filename}',f'{dirname}{filename2}']
        files=[f'{dirname}{filename2}']
        )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_TEC_PYR_GEFDispensacion_python_task >> email_summary >> task_end