'''
dag_test_varias_funciones.py

Autor: Luis Esteban Santamaría Blanco. Data Engineer.
Fecha: 2 de Agosto de 2023.

Este script tiene como propósito hacer una demostración básica de cómo se construye un DAG de Apache Airflow.
Se construyó inspirado en las capacitaciones grabadas por el equipo técnico de Clínicos.

'''

import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator # Inicio y final del Dag
from airflow.operators.bash_operator import BashOperator # Ejecución de archivos o funciones bash de Windows.
from airflow.operators.python_operator import PythonOperator # Ejecución de funciones de Python.
from airflow.contrib.hooks.wasb_hook import WasbHook # Para conexión con Blob Storage
from datetime import datetime, timedelta
from datetime import date

# Variables a utilizar en el DAG
DB_TMP_TABLE = "tmp_prueba_clinicos"
DB_TABLE = "prueba_clinicos"
DAG_NAME = "dag_" + DB_TABLE


# Funciones
def function1():
    print("Funcion 1 ejecutada correctamente.")

def function2():
    print("Funcion 2 ejecutada correctamente.")

def function3():
    print("Funcion 3 ejecutada correctamente.")

# Asignación de valores para los parámetros del DAG
default_args = {
    'owner': 'CAPACITACION_CLINICOS',
    'depens_on_past': False, # ¿Depende de otro DAG u otra Ejecución? NO -> False.

    # Hay que asignar una fecha anterior a la actual para que se puedea ejecutar.
    # También se puede asignar una fecha futura si la ejecución no se hará desde ya.
    'start_date': datetime(2015, 6, 1) 
}

# Declaración del DAG
with DAG(DAG_NAME, # Nombre del DAG
        catchup=False, # Gener
        default_args=default_args,
        schedule_interval=None, # Su ejecución se hará manualmente 
        # schedule_interval='*/10 * * * *' # Para programar un horario de ejecución
        # ...='@hourly', ='@daily', ='@monthly' # Son otras opciones
        max_active_runs=1 # Cantidad máxima de instancias de ejecución activas permitidas para un DAG.
) as dag:
    
    start_task = DummyOperator(task_id='dummy_start') # Este operador indica que ya inició la ejecución.

    fucntion1 = PythonOperator(task_id='inicio_funcion_1', # Aparecerá en la interfaz gráfica del DAG en el servidor web. (Ver Tree View). Puede tener cualquier nombre.
                             python_callable=function1) # Se incluye la función a llamar.
    
    fucntion2 = PythonOperator(task_id='inicio_funcion_2', 
                             python_callable=function2)
    
    fucntion3 = PythonOperator(task_id='inicio_funcion_3', 
                             python_callable=function3)
    

start_task >> function1 >> function2 >> function3