import os
import xlrd
from airflow import DAG
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime, timedelta
from datetime import date
import pandas as pd
from pandas import read_excel
from variables import sql_connid
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_contains_name,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'rotacionpersonas'
dirname = '/opt/airflow/dags/files_rotacion_personas/'
filename = 'THM_VYD_Nomina.xlsx'
db_table = "THM_VYD_Nomina"
db_tmp_table = "tmp_THM_VYD_Nomina"
dag_name = 'dag_' + db_table
container_to = 'historicos'


# Se realiza un chequeo de la conexión al blob storage
def check_connection():
    print('Conexión OK')
    return(wb.check_for_blob(container,filename))

# Normalizar columnas de fechas de Excel
def norm_excel_date_gen(date_gen):

    # Conversion a entero y luego date.time
    try:
        int_date = int(date_gen)
        res = xlrd.xldate_as_datetime(int_date, 0)
        res = pd.to_datetime(res)
    # Excepcion por retorno de string
    except ValueError:
        res = pd.to_datetime(date_gen, format='%d/%m/%Y')
    # Excepcion por tipo (esperando fecha)
    except TypeError:
        res = date_gen   
    # Excepcion por conversion    
    except OverflowError:
        res = date_gen 
    return res 

# Normalizar de nombres de columnas
def norm_col_names(df):
    # Estandarización de los nombres de columnas del dataframe
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(' ','_')
    df.columns = df.columns.str.replace('á','a')
    df.columns = df.columns.str.replace('é','e')
    df.columns = df.columns.str.replace('í','i')
    df.columns = df.columns.str.replace('ó','o')
    df.columns = df.columns.str.replace('ú','u')
    df.columns = df.columns.str.replace('ñ','ni')
    return df

# Función de transformación de los archivos xlsx
def transform_table(path):

    # dataframe activos

<<<<<<< HEAD
    df_active = pd.read_excel(path, header = [4], sheet_name = 0)
    df_active["estado"] = "Activos"
    df_active["organizacion"] = "CLÍNICOS"

    df_active_innovar = pd.read_excel(path, header = [4], sheet_name = 2)
=======
    df_active = pd.read_excel(path, header = [0], sheet_name = 0)
    df_active["estado"] = "Activos"
    df_active["organizacion"] = "CLÍNICOS"
    df_active = norm_col_names(df_active)

    df_active_innovar = pd.read_excel(path, header = [0], sheet_name = 2)
>>>>>>> Manar
    df_active_innovar["estado"] = "Activos"
    df_active_innovar["organizacion"] = "INNOVAR"

    # dataframe de la hoja de retirados

<<<<<<< HEAD
    df_retired = pd.read_excel(path, header = [3], sheet_name = 1)
    df_retired["estado"] = "Retirados"
    df_retired["organizacion"] = "CLÍNICOS"

    df_retired_innovar = pd.read_excel(path, header = [4], sheet_name = 3)
=======
    df_retired = pd.read_excel(path, header = [0], sheet_name = 1)
    df_retired["estado"] = "Retirados"
    df_retired["organizacion"] = "CLÍNICOS"
    df_retired = norm_col_names(df_retired)

    df_retired_innovar = pd.read_excel(path, header = [1], sheet_name = 3)
>>>>>>> Manar
    df_retired_innovar["estado"] = "Retirados"
    df_retired_innovar["organizacion"] = "INNOVAR"

    # Remplazo de campos innovar
    assign_innovar = {
<<<<<<< HEAD
        " CEDULA ":"Identific.",
=======
        "DOCUMENTO":"Identific.",
>>>>>>> Manar
        "FECHA DE INGRESO  MDA":"Fecha Ingreso",
        "NOMBRE":"Empleado",
        "TIPO DE CONTRATO":"Tipo Contrato",
        "CELULAR":"Tel1",
        "DIRECCION":"Dirección",
        "CORREO CORPORATIVO":"e-mail",
        "FECHA NACIMIENTO":"Fecha Nacim",
        "SEDE":"Sucursal",
        "AREA":"UNIDAD",
        "FECHA DE RETIRO":"Fecha Retiro",
        "FECHA NACIMIENTO":"Fecha Nacim",
        "CARGO":"Cargo",
    }
    df_active_innovar.rename(columns = assign_innovar, inplace = True)
    df_retired_innovar.rename(columns = assign_innovar, inplace = True) 
<<<<<<< HEAD

    # Arreglo de los cargos con "-" en innovar
    df_active_innovar['Cargo'] = df_active_innovar['Cargo'].str.replace('-','')
    df_retired_innovar['Cargo'] = df_retired_innovar['Cargo'].str.replace('-','')

    # Append entre las hojas de activos y retirados
    df = pd.concat([df_active, df_active_innovar, df_retired, df_retired_innovar], ignore_index=True)
    print(f'Las dimensiones son {df.shape}')

    # Reemplazo de valores mal escritos en la columna nombre_ccosto
    df['Nombre CCosto'] = df['Nombre CCosto'].str.replace('Administrativa','Unidad administrativa')
    df['Nombre CCosto'] = df['Nombre CCosto'].str.replace('Unidades Domiciliaria','unidad domiciliaria')
    df['Nombre CCosto'] = df['Nombre CCosto'].str.upper()

    # Reemplazo de valores para estandarizar la columna UNIDAD
    df['UNIDAD'] = df['UNIDAD'].str.replace('GERENCIA  GENERAL','Unidad administrativa')
    df['UNIDAD'] = df['UNIDAD'].str.replace('MERK','Unidad administrativa')
    df['UNIDAD'] = df['UNIDAD'].str.replace(r'(^.*(Cultura|Talento)+.*$)','Cultura y Talento Humano', case = False)
    df['UNIDAD'] = df['UNIDAD'].str.replace(r'(^.*(Financiero)+.*$)','Financiera', case = False)
    df['UNIDAD'] = df['UNIDAD'].str.replace(r'(^.*(Tecno|calidad)+.*$)','Tecnología', case = False)
    df['UNIDAD'] = df['UNIDAD'].str.replace(r'(^.*(DOMICILIARIA)+.*$)','Unidad Domiciliaria', case = False)
    df['UNIDAD'] = df['UNIDAD'].str.upper()
    df['UNIDAD'].fillna(df['Nombre CCosto'])
=======
    df_active_innovar = norm_col_names(df_active_innovar)
    df_retired_innovar = norm_col_names(df_retired_innovar)

    ## Corregir Cargo INNOVAR
    df_active_innovar['cargo'] = df_active_innovar['cargo'].str.replace('-','') 
    df_retired_innovar['cargo'] = df_retired_innovar['cargo'].str.replace('-','')

    # Append entre las hojas de activos y retirados
    df = pd.concat([df_retired, df_active, df_active_innovar, df_retired_innovar], ignore_index=True)
>>>>>>> Manar

    # Estandarización de los nombres de columnas del dataframe
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(' ','_')
    df.columns = df.columns.str.replace('á','a')
    df.columns = df.columns.str.replace('é','e')
    df.columns = df.columns.str.replace('í','i')
    df.columns = df.columns.str.replace('ó','o')
    df.columns = df.columns.str.replace('ú','u')
    df.columns = df.columns.str.replace('ñ','ni')
<<<<<<< HEAD
    print(df['cargo'].dtype)

=======

    ## Quitar Nombre CCosto de ingesta - 20230321
    # Reemplazo de valores mal escritos en la columna nombre_ccosto
    # df['Nombre CCosto'] = df['Nombre CCosto'].str.replace('Administrativa','Unidad administrativa')
    # df['Nombre CCosto'] = df['Nombre CCosto'].str.replace('Unidades Domiciliaria','unidad domiciliaria')
    # df['Nombre CCosto'] = df['Nombre CCosto'].str.upper()

    # Reemplazo de valores para estandarizar la columna UNIDAD
    df['unidad'] = df['unidad'].str.replace('GERENCIA  GENERAL','Unidad administrativa')
    df['unidad'] = df['unidad'].str.replace('MERK','Unidad administrativa')
    df['unidad'] = df['unidad'].str.replace(r'(^.*(Cultura|Talento)+.*$)','Cultura y Talento Humano', case = False)
    df['unidad'] = df['unidad'].str.replace(r'(^.*(Financiero)+.*$)','Financiera', case = False)
    df['unidad'] = df['unidad'].str.replace(r'(^.*(Tecno|calidad)+.*$)','Tecnología', case = False)
    df['unidad'] = df['unidad'].str.upper()
    ## Quitar Nombre CCosto de ingesta - 20230321
    # df['Unidad'].fillna(df['Nombre CCosto'])
    
>>>>>>> Manar
    # Separación del cargo y nivel_cargo en dos columnas dentro del dataframe
    df_cargo = df['cargo'].str.split('-', n=1, expand = True)
    
    df_1 = df_cargo.apply(lambda x: "No reporta" if (x[1] == None) else x[0], axis = 1)
    df['nivel_cargo'] = df_1
    df['nivel_cargo'] = df['nivel_cargo'].str.strip()

    print(df['nivel_cargo'].head(30))

    df_2 = df_cargo.apply(lambda x: x[0] if (x[1] == None) else x[1], axis = 1)
    df['cargo'] = df_2
    df['cargo'] = df['cargo'].str.strip()

    print(df['cargo'].head(30))

    ## Campos nuevos para requerimiento febrero 16 - 2023
    df['fecha_ingreso_clinicos'] = df['fecha_ingreso_pilar']

    # Reordenar columnas del dataframe
    ## Ingresar nulos
    null_cols = [
        'e-mail'
        ,'tipo_contrato'
        ,'fecha_nacim'
        ,'direccion'
        ,'tel1'
        ,'estado_civil'
        ,'ciudad_nacim'
        ,'%_tiempo_trabajado'
        ,'nombre_ccosto'
        ,'ciud.ubic'
        ,'entidad_eps'
        ,'entidad_afp'
        ,'entidad_caja'
        ,'entidad_arp'
    ]
    df = df.reindex(columns=[*[*df.columns.tolist(), *null_cols]])
    df['fecha_nacim'] = datetime.strptime('1900-01-01', '%Y-%m-%d')

    ##
    df = df[['identific.'
      ,'tipo_id'
      ,'empleado'
      ,'estado_civil'
      ,'fecha_nacim'
      ,'ciudad_nacim'
      ,'tel1'
      ,'direccion'
      ,'e-mail'
      ,'nivel_cargo'
      ,'cargo'
      ,'%_tiempo_trabajado'
      ,'sucursal'
      ,'nombre_ccosto'
      ,'ciud.ubic'
      ,'fecha_ingreso'
      ,'fecha_retiro'
      ,'tipo_contrato'
      ,'entidad_eps'
      ,'entidad_afp'
      ,'entidad_caja'
      ,'entidad_arp'
      ,'estado'
      ,'unidad'
      ,'fecha_ingreso_clinicos'
      ,'organizacion']] #, 'migrado_innovar'


    df['fecha_retiro'] = pd.to_datetime(df['fecha_retiro'], format='%Y/%m/%d')

    ## Quitar de ingesta - 20230321
    # df['fecha_nacim'] = df['fecha_nacim'].apply(norm_excel_date_gen)

    ## Fill tipo_id
    df['tipo_id'] = df['tipo_id'].fillna('CC')

    df['sucursal'] = df['sucursal'].str.strip()


    print(df[['nivel_cargo', 'cargo']].head(46))
    print(df_1.head(46))
    print(df_2.head(46))

    # Llenado de nulos columnas no numericas
    obj_cols = df.select_dtypes(include=['object']).columns
    df[obj_cols] = df[obj_cols].fillna(value='S/D')

    print(df.isnull().sum())


    return df

# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_THM_VYD_Nomina ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_table(path)
    print(df.columns)
    print(df.dtypes)
    print(df.tail(50))

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_table, sql_connid)

    # move_to_history_contains_name(dirname, container, filename, container_to)



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
    # Se establece la ejecución del dag todos los viernes a las 12:30 PM(Hora servidor)
    schedule_interval= '30 10 * * fri',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    check_connection_task = PythonOperator(task_id='check_connection',
        python_callable=check_connection)

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_THM_VYD_Nomina_python_task = PythonOperator(task_id = "get_THM_VYD_Nomina",
                                                    python_callable = func_get_THM_VYD_Nomina,
                                                    email_on_failure=True, 
                                                    email='BI@clinicos.com.co',
                                                    dag=dag
                                                    )
    
    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >>check_connection_task >> get_THM_VYD_Nomina_python_task >> task_end