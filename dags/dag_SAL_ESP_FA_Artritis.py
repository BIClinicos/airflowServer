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
from utils import open_xls_as_xlsx,load_df_to_sql,search_for_file_prefix, get_files_xlsx_contains_name, get_files_with_prefix_args,search_for_file_contains, respond, read_csv, move_to_history_for_prefix,  get_files_xlsx_with_prefix, get_files_xlsx_with_prefix_args,file_get, remove_accents_cols, remove_special_chars, regular_camel_case, regular_snake_case, normalize_str_categorical

#  Se nombran las variables a utilizar en el dag

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')
container = 'famisanar'
dirname = '/opt/airflow/dags/files_famisanar/'
filename = 'SAL_ESP_FA_Artritis.xlsx'
db_table = "SAL_ESP_FA_Artritis"
db_tmp_table = "tmp_SAL_ESP_FA_Artritis"
dag_name = 'dag_' + db_table


# Función de transformación de los archivos xlsx
def transform_data (path):

    df = pd.read_excel(path)

    # missing_columns = [
    #     '12. PERTENENCIA ETNICA',
    #     '13. DIRECCION DE RESIDENCIA',
    #     '14. TELEFONO DE CONTACTO',
    #     'ARTICULACIONES AFECTADAS (Monoarticular - Oligoarticular - Poliarticular)',
    #     'COMPROMISO HEMICUERPO (Simetricos - Asimetricos)',
    #     'RIGIDEZ MATINAL (Mayor a 30 minutos - Menor a 30 minutos)',
    #     'SEROLOGÍA (Seronegativa - Seropositiva - Doblemente seropositiva)'
    # ]

    # for i in missing_columns:
    #     df[i] = ''
    #     print('lo hizo perfecto missing_columns: ', i)

    df.columns = remove_accents_cols(df.columns)
    df.columns = remove_special_chars(df.columns)
    df.columns = regular_snake_case(df.columns)

    df = df.rename(
        columns = {
            'fecha_reporte' : 'fecha_reporte', 
            'doc_para_cruces' : 'doc_cruces', 
            'cie10_de_reporte' : 'cie_10_reporte', 
            'estado' : 'estado', 
            'tipo_de_identificacion_del_paciente' : 'tipo_identificacion_paciente',
            'numero_de_identificacion_del_paciente' : 'identificacion_paciente', 
            'primer_nombre' : 'primer_nombre',
            'segundo_nombre' : 'segundo_nombre', 
            'primer_apellido' : 'primer_apellido', 
            'segundo_apellido' : 'segundo_apellido',
            'fecha_de_nacimiento' : 'fecha_nacimiento', 
            'edad' : 'edad', 
            'curso_de_vida' : 'curso_vida', 
            'sexo' : 'sexo',
            '12_pertenencia_etnica' : '12_pertenencia_etnica', 
            '13_direccion_de_residencia' : '13_direccion_de_residencia',
            '14_telefono_de_contacto' : '14_telefono_de_contacto', 
            'fecha_inicio_sintomas_de_ar' : 'fecha_inicio_sintomas_ar',
            'fecha_primera_visita_especialista_por_ar' : 'fecha_primera_visita_especialista_ar',
            'fecha_de_diagnostico_de_ar' : 'fecha_diagnostico_ar', 
            'factor_reumatoideo_inicial' : 'factor_reumatoideo_inicial',
            'anti_ccp_al_diagnostico' : 'anti_ccp_diagnostico', 
            'hta_al_diagnostico' : 'hta_diagnostico', 
            'dm_al_diagnostico' : 'dm_diagnostico',
            'ecv_al_diagnostico' : 'ecv_diagnostico', 
            'erc_al_diagnostico' : 'erc_diagnostico',
            'osteoporosis_al_diagnostico' : 'osteoporosis_diagnostico', 
            'sindrome_de_sjogren_al_diagnostico' : 'sindrome_sjogren_diagnostico',
            'hta_actual' : 'hta_actual', 
            'dm_actual' : 'dm_actual', 
            'ecv_actual' : 'ecv_actual', 
            'erc_actual' : 'erc_actual',
            'osteoporosis_actual' : 'osteoporosis_actual', 
            'sindrome_de_sjogren_actual' : 'sindrome_sjogren_actual',
            'fecha_del_ultimo_das_28_realizado' : 'fecha_ultimo_das28_realizado',
            'profesional_que_realizo_el_ultimo_das' : 'profesional_que_realizo_ultimo_das28', 
            'resultado_del_ultimo_das_28' : 'resultado_ultimo_das28',
            'estado_de_actividad_actual_de_la_ar_segun_das_28' : 'estado_actividad_actual_ar_das28',
            'fecha_del_ultimo_haq_(health_assessment_questionnaire)_realizado' : 'fecha_ultimo_haq',
            'haq_(health_assessment_questionnaire)_ultimo_semestre' : 'haq_ultimo_semestre',
            'fecha_de_rapid_3' : 'fecha_rapid3', 
            'resultado_rapid_3' : 'resultado_rapid3',
            'estado_de_actividad_actual_de_la_ar_segun_rapid_3' : 'estado_actividad_actual_ar_rapid3',
            'peso_ultimo_semestre' : 'peso_ultimo_semestre',
            'radiografia_de_manos_(ultimas_realizadas_despues_de_iniciales)' : 'radiografia_manos',
            'radiografia_de_pies_(ultimas_realizadas_despues_de_iniciales)' : 'radiografia_pies',
            'pcr_ultimo_semestre' : 'pcr_ultimo_semestre', 
            'vsg_ultimo_semestre' : 'vsg_ultimo_semestre',
            'hemoglobina_ultimo_semestre' : 'hemoglobina_ultimo_semestre', 
            'leucocitos_ultimo_semestre' : 'leucocitos_ultimo_semestre',
            'creatinina_ultimo_semestre' : 'creatinina_ultimo_semestre', 
            'tfg_ultimo_semestre' : 'tfg_ultimo_semestre',
            'parcial_de_orina_ultimo_semestre' : 'parcial_orina_ultimo_semestre', 
            'alt_ultimo_semestre' : 'alt_ultimo_semestre',
            'fecha_de_ingreso_a_la_ips_actual_donde_se_hace_el_seguimiento_y_atencion_de_la_ar_al_paciente' : 'fecha_ingreso_ips_actual_seguimiento_ar_paciente',
            'articulaciones_afectadas_(monoarticular_oligoarticular_poliarticular)' : 'articulaciones_afectadas_(monoarticular_oligoarticular_poliarticular)',
            'compromiso_hemicuerpo_(simetricos_asimetricos)' : 'compromiso_hemicuerpo_(simetricos_asimetricos)',
            'rigidez_matinal_(mayor_a_30_minutos_menor_a_30_minutos)' : 'rigidez_matinal_(mayor_a_30_minutos_menor_a_30_minutos)',
            'serologia_(seronegativa_seropositiva_doblemente_seropositiva)' : 'serologia_(seronegativa_seropositiva_doblemente_seropositiva)',
            'ultima_consulta_con_reumatologia_ultimo_ano' : 'ultima_consulta_reumatologia_ultimo_ano', 
            'proximo_control' : 'proximo_control_reumatologia',
            'numero_de_consultas_con_reumatologo_en_el_ultimo_ano' : 'numero_consultas_reumatologo_ultimo_ano',
            'ultima_consulta_con_internista_ultimo_ano' : 'ultima_consulta_internista_ultimo_ano', 
            'proximo_control_1' : 'proximo_control_internista',
            'numero_de_consultas_con_internista_en_el_ultimo_ano' : 'numero_consultas_internista_ultimo_ano',
            'ultima_consulta_con_m_general_ultimo_ano' : 'ultima_consulta_med_general_ultimo_ano', 
            'proximo_control_2' : 'proximo_control_med_general',
            'numero_de_consultas_con_m_general_en_el_ultimo_ano' : 'numero_consultas_med_general_ultimo_ano',
            'ultima_consulta_con_fisiatria_ultimo_ano' : 'ultima_consulta_fisiatria_ultimo_ano', 
            'proximo_control_3' : 'proximo_control_fisiatria',
            'numero_de_consultas_con_fisiatria_en_el_ultimo_ano' : 'consultas_fisiatria_ultimo_ano'
        }
    )

    print(df.dtypes)
    print("df columnas", df.columns)


    # df = df[[
    #     'fecha_reporte', 
    #     'doc_cruces', 
    #     'cie_10_reporte', 
    #     'estado',
    #     'tipo_identificacion_paciente', 
    #     'identificacion_paciente',
    #     'primer_nombre', 
    #     'segundo_nombre', 
    #     'primer_apellido',
    #     'segundo_apellido', 
    #     'fecha_nacimiento', 
    #     'edad', 
    #     'curso_vida', 
    #     'sexo', 
    #     '12_pertenencia_etnica', 
    #     '13_direccion_de_residencia', 
    #     '14_telefono_de_contacto',
    #     'fecha_inicio_sintomas_ar', 
    #     'fecha_primera_visita_especialista_ar',
    #     'fecha_diagnostico_ar', 
    #     'factor_reumatoideo_inicial',
    #     'anti_ccp_diagnostico', 
    #     'hta_diagnostico', 
    #     'dm_diagnostico',
    #     'ecv_diagnostico', 
    #     'erc_diagnostico', 
    #     'osteoporosis_diagnostico',
    #     'sindrome_sjogren_diagnostico', 
    #     'hta_actual', 
    #     'dm_actual', 
    #     'ecv_actual',
    #     'erc_actual', 
    #     'osteoporosis_actual', 
    #     'sindrome_sjogren_actual',
    #     'fecha_ultimo_das28_realizado', 
    #     'profesional_que_realizo_ultimo_das28',
    #     'resultado_ultimo_das28', 
    #     'estado_actividad_actual_ar_das28',
    #     'fecha_ultimo_haq', 
    #     'haq_ultimo_semestre', 
    #     'fecha_rapid3',
    #     'resultado_rapid3', 
    #     'estado_actividad_actual_ar_rapid3',
    #     'peso_ultimo_semestre', 
    #     'radiografia_manos', 
    #     'radiografia_pies',
    #     'pcr_ultimo_semestre', 
    #     'vsg_ultimo_semestre',
    #     'hemoglobina_ultimo_semestre', 
    #     'leucocitos_ultimo_semestre',
    #     'creatinina_ultimo_semestre', 
    #     'tfg_ultimo_semestre',
    #     'parcial_orina_ultimo_semestre', 
    #     'alt_ultimo_semestre',
    #     'fecha_ingreso_ips_actual_seguimiento_ar_paciente' , 
    #     'articulaciones_afectadas_(monoarticular_oligoarticular_poliarticular)',
    #     'compromiso_hemicuerpo_(simetricos_asimetricos)',
    #     'rigidez_matinal_(mayor_a_30_minutos_menor_a_30_minutos)',
    #     'serologia_(seronegativa_seropositiva_doblemente_seropositiva)',
    #     'ultima_consulta_reumatologia_ultimo_ano',
    #     'proximo_control_reumatologia',
    #     'numero_consultas_reumatologo_ultimo_ano',
    #     'ultima_consulta_internista_ultimo_ano', 
    #     'proximo_control_internista',
    #     'numero_consultas_internista_ultimo_ano',
    #     'ultima_consulta_med_general_ultimo_ano', 
    #     'proximo_control_med_general',
    #     'numero_consultas_med_general_ultimo_ano',
    #     'ultima_consulta_fisiatria_ultimo_ano', 
    #     'proximo_control_fisiatria',
    #     'consultas_fisiatria_ultimo_ano'
    # ]]

    num_col = [
        'factor_reumatoideo_inicial',
        'anti_ccp_diagnostico', 
        'hta_diagnostico', 
        'dm_diagnostico',
        'ecv_diagnostico', 
        'erc_diagnostico', 
        'osteoporosis_diagnostico',
        'sindrome_sjogren_diagnostico', 
        'resultado_ultimo_das28', 
        'estado_actividad_actual_ar_das28',
        'haq_ultimo_semestre', 
        'resultado_rapid3', 
        'peso_ultimo_semestre', 
        'radiografia_manos', 
        'radiografia_pies',
        'pcr_ultimo_semestre', 
        'vsg_ultimo_semestre',
        'hemoglobina_ultimo_semestre', 
        'leucocitos_ultimo_semestre',
        'creatinina_ultimo_semestre', 
        'tfg_ultimo_semestre',
        'parcial_orina_ultimo_semestre', 
        'alt_ultimo_semestre',
        'articulaciones_afectadas_(monoarticular_oligoarticular_poliarticular)',
        'compromiso_hemicuerpo_(simetricos_asimetricos)',
        'rigidez_matinal_(mayor_a_30_minutos_menor_a_30_minutos)',
        'serologia_(seronegativa_seropositiva_doblemente_seropositiva)',
        'numero_consultas_reumatologo_ultimo_ano',
        'numero_consultas_internista_ultimo_ano',
        'numero_consultas_med_general_ultimo_ano',
        'consultas_fisiatria_ultimo_ano'
    ]

    for i in num_col:
        df[i] = df[i].astype(str)
        df[i] = df[i].str.replace('300','No Data').replace('300.00','No Data')
        df[i] = pd.to_numeric(df[i], errors='coerce')


    date_col = [
        'fecha_reporte', 
        'fecha_nacimiento', 
        'fecha_inicio_sintomas_ar', 
        'fecha_primera_visita_especialista_ar',
        'fecha_diagnostico_ar', 
        'fecha_ultimo_das28_realizado', 
        'fecha_ultimo_haq', 
        'fecha_rapid3',
        'fecha_ingreso_ips_actual_seguimiento_ar_paciente' , 
        'ultima_consulta_reumatologia_ultimo_ano',
        'proximo_control_reumatologia',
        'ultima_consulta_internista_ultimo_ano', 
        'proximo_control_internista',
        'ultima_consulta_med_general_ultimo_ano', 
        'proximo_control_med_general',
        'ultima_consulta_fisiatria_ultimo_ano', 
        'proximo_control_fisiatria'
    ]

    for i in date_col:
        df[i] = df[i].astype(str)
        df[i] = pd.to_datetime(df[i], errors='coerce')


    float_col = [
        'edad', 
        'factor_reumatoideo_inicial',
        'anti_ccp_diagnostico', 
        'hta_diagnostico', 
        'dm_diagnostico',
        'ecv_diagnostico', 
        'erc_diagnostico', 
        'osteoporosis_diagnostico',
        'sindrome_sjogren_diagnostico', 
        'hta_actual', 
        'dm_actual', 
        'ecv_actual',
        'erc_actual', 
        'osteoporosis_actual', 
        'sindrome_sjogren_actual',
        'resultado_ultimo_das28', 
        'estado_actividad_actual_ar_das28',
        'haq_ultimo_semestre', 
        'resultado_rapid3', 
        'peso_ultimo_semestre', 
        'radiografia_manos', 
        'radiografia_pies',
        'vsg_ultimo_semestre',
        'hemoglobina_ultimo_semestre', 
        'leucocitos_ultimo_semestre',
        'creatinina_ultimo_semestre', 
        'tfg_ultimo_semestre'
    ]

    for i in float_col:
        df[i] = df[i].astype(str)
        df[i] = df[i].replace(' ','').replace('300','No Data')
        df[i] = pd.to_numeric(df[i], errors='coerce')

    cat_col = [
        'doc_cruces', 
        'cie_10_reporte', 
        'estado',
        'tipo_identificacion_paciente', 
        'identificacion_paciente',
        'primer_nombre', 
        'segundo_nombre', 
        'primer_apellido',
        'segundo_apellido', 
        'fecha_nacimiento', 
        'sexo',
        '13_direccion_de_residencia', 
        '14_telefono_de_contacto', 
        'estado_actividad_actual_ar_rapid3',
        'articulaciones_afectadas_(monoarticular_oligoarticular_poliarticular)',
        'compromiso_hemicuerpo_(simetricos_asimetricos)',
        'rigidez_matinal_(mayor_a_30_minutos_menor_a_30_minutos)',
        'serologia_(seronegativa_seropositiva_doblemente_seropositiva)'
    ]

    for i in cat_col:
        df[i] = df[i].astype(str)
        df[i] = normalize_str_categorical(df[i])

    int_col = [
        '12_pertenencia_etnica', 
        'profesional_que_realizo_ultimo_das28',
        'pcr_ultimo_semestre', 
        'parcial_orina_ultimo_semestre', 
        'alt_ultimo_semestre',
        'numero_consultas_reumatologo_ultimo_ano',
        'numero_consultas_internista_ultimo_ano',
        'numero_consultas_med_general_ultimo_ano',
        'consultas_fisiatria_ultimo_ano'
    ]

    for i in int_col:
        df[i] = pd.to_numeric(df[i], errors='coerce', downcast="integer")
        df[i] = df[i].astype(str).str.replace('300','', regex = False)
        df[i] = df[i].astype(str).str.replace('.0','', regex = False)
    
    return df


# Función de extracción del archivo del blob al servidor, transformación del dataframe y cargue a la base de datos mssql
def func_get_SAL_ESP_FA_Artritis ():

    path = dirname + filename
    print(path)
    file_get(path,container,filename, wb = wb)
    df = transform_data(path)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', -1)

    print("df antes", df)

    df = df.drop_duplicates(
        subset=[
        'fecha_reporte',
        'doc_cruces',
        'tipo_identificacion_paciente',
        'identificacion_paciente',
        'primer_nombre',
        'primer_apellido',
        'segundo_apellido',
        'fecha_nacimiento',
        'curso_vida',
        'sexo'
        ]
    )

    print(df.dtypes)
    print("df después", df)
    print(df.isnull().sum())

    

    if ~df.empty and len(df.columns) >0:
        load_df_to_sql(df, db_tmp_table, sql_connid)



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
    # Se establece el cargue de los datos el primer día de cada mes.
    schedule_interval= '0 4 1 * *',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')

    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    get_SAL_ESP_FA_Artritis_python_task = PythonOperator(
        task_id = "get_SAL_ESP_FA_Artritis",
        python_callable = func_get_SAL_ESP_FA_Artritis,
        # email_on_failure=True,
        # email='BI@clinicos.com.co',
        # dag=dag,
        )
    
    # Se declara la función encargada de ejecutar el "Stored Procedure"
    load_SAL_ESP_FA_Artritis = MsSqlOperator(
        task_id='Load_SAL_ESP_FA_Artritis',
        mssql_conn_id=sql_connid,
        autocommit=True,
        sql="EXECUTE sp_load_SAL_ESP_FA_Artritis",
        # email_on_failure=True,
        # email='BI@clinicos.com.co',
        # dag=dag,
        )

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> get_SAL_ESP_FA_Artritis_python_task >> load_SAL_ESP_FA_Artritis >> task_end