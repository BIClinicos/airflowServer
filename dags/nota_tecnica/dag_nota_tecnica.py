from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from datetime import datetime
import pandas as pd
from variables import sql_connid

from utils import get_blob_2_df, load_df_to_sql_pandas

container = 'nota-tecnica'
dag_name = 'dag_' + 'tblHNotaTecnica'

diagnostico_file = 'ApoyoDiagnostico.xlsx'
insumos_file = 'DispositivosInsumos.xlsx'
medicamentos_file = 'Medicamentos.xlsx'
salarios_file ='SalariosAsistenciales.xlsx'

table_tarifas = "TblHTarifarioConsultas"
table_diagnostico = "TblHTarifarioApoyoDiag"
table_medicamentos = "TblHTarifarioMedicamentos"
table_insumos = "TblHTarifarioInsumos"

COLUMNS_TARIFAS = ["nombre_especialidad","unidad","modalidad_nomina","municipio","valor_base",
                   "rodamiento","valor_neto","modalidad_ops","pacientes_hora","modalidad_atencion"]

COLUMNS_DIAGNOSTICO = ["proveedor","nombre","cod_proveedor","valor","fecha_vigencia","cups"]

COLUMNS_MEDICAMENTOS = ["PharmacologicalGroup","ActiveIngredientDescription",
                   "Concentration","PharmaceuticalForm","CommercialName","ContentUnit",
                   "UnitsPerPresentation","AdministrationRoute","SanitaryRegistration",
                   "UnitValue","PresentationValue","VAT","MedicationPbsNoPbs",
                   "RegulatedUnregulatedMedication","MaximumPriceInstitutionalTransaction"]

def read_tarifas_from_azure_storage(**kwargs):
    dfs_tarifas = get_blob_2_df(container, salarios_file, sheets=["VALOR HORA NOMINA","TARIFAS OPS"])
    df_nomina:pd.DataFrame = dfs_tarifas["VALOR HORA NOMINA"]
    df_nomina.columns = df_nomina.columns.str.strip()
    df_ops = dfs_tarifas["TARIFAS OPS"]
    df_ops.columns = df_ops.columns.str.strip()
    df_nomina.rename(columns = {"FUNCION":"nombre_especialidad", "UNIDAD":"unidad","Modalidad":"modalidad_nomina",
                                "Ciudad":"municipio",  "VALOR POR HORA":"valor_base","RODAMIENTO":"rodamiento",
                                "SALARIO + CARGA PRESTACIONAL":"valor_neto"}, inplace= True)
    df_nomina["valor_neto"] = df_nomina.apply(lambda x: 
        x["valor_neto"] + x["rodamiento"]/188 if not pd.isnull(x["rodamiento"]) else x["valor_neto"], axis=1)
    df_nomina["tipo_contratacion"] = "Nómina"
    df_ops.rename(columns = {"ESPECIALIDAD":"nombre_especialidad", "UNIDAD DE NEGOCIO":"unidad",
                             "MODALIDAD DE TARIFA":"modalidad_ops","UBICACIÓN":"municipio", "PACIENTES POR HORA":"pacientes_hora","MODALIDAD DE ATENCIÓN":"modalidad_atencion","TARIFA AJUSTADA 2023":"valor_neto"},inplace= True)
    df_ops["tipo_contratacion"] = "OPS"
    
    df:pd.DataFrame = pd.concat([df_nomina, df_ops], ignore_index=True)
    cols = df.filter(regex=r"^Unnamed",axis=1 ).columns
    df.drop(cols, axis=1, inplace=True)
    df = df[COLUMNS_TARIFAS]
    
    if (len(df) > 0):
        load_df_to_sql_pandas(df,table_tarifas,sql_connid, truncate= False)
        

def read_diagnosticos_from_azure_storage(**kwargs):
    df_diagnostico = get_blob_2_df(container, diagnostico_file)
    df_diagnostico.columns = df_diagnostico.columns.str.strip()
    
    df_diagnostico.rename(columns = {"Proveedor":"proveedor", "Nombre":"nombre","Codigo proveedor":"cod_proveedor",
                                "Valor":"valor",  "Fecha de vigencia":"fecha_vigencia","CUPS":"cups"}, inplace= True)
    
    cols = df_diagnostico.filter(regex=r"^Unnamed",axis=1 ).columns
    df_diagnostico.drop(cols, axis=1, inplace=True)
    df_diagnostico = df_diagnostico[COLUMNS_DIAGNOSTICO]
    
    if (len(df_diagnostico) > 0):
        load_df_to_sql_pandas(df_diagnostico,table_diagnostico,sql_connid, truncate= False)
 
def read_medicamentos_from_azure_storage(**kwargs):
    df_medicamento = get_blob_2_df(container, medicamentos_file, skiprows = 1)
    df_medicamento.columns = df_medicamento.columns.str.replace("\n"," ").str.replace("  "," ").str.strip()
    df_medicamento.rename(columns = 
                          {"GRUPO FARMACOLÓGICO":"PharmacologicalGroup", 
                           "PRINCIPIO ACTIVO":"ActiveIngredientDescription",
                           "CONCENTRACIÓN":"Concentration",
                            "FORMA FARMACÉUTICA":"PharmaceuticalForm",  
                            "PRESENTACION COMERCIAL":"CommercialName",
                            "PRESENTACIÓN":"ContentUnit",
                            "FACTOR DE CONVERSION":"UnitsPerPresentation",
                            "VÍA DE ADMINISTRACIÓN":"AdministrationRoute",
                            "REGISTRO SANITARIO":"SanitaryRegistration",
                            "VALOR UNITARIO":"UnitValue",
                            "VALOR PRESENTACIÓN":"PresentationValue",
                            "IVA":"VAT",
                            "MEDICAMENTO PBS / NO PBS":"MedicationPbsNoPbs",
                            "MEDICAMENTO REGULADO/ NO REGULADO (Si aplica)":"RegulatedUnregulatedMedication",
                            "PRECIO MAXIMO REGULADO PRESENTACION":"MaximumPriceInstitutionalTransaction"}, inplace= True)
    
    cols = df_medicamento.filter(regex=r"^Unnamed",axis=1 ).columns
    df_medicamento.drop(cols, axis=1, inplace=True)

    df_medicamento = df_medicamento[COLUMNS_MEDICAMENTOS]
    for col in ["MaximumPriceInstitutionalTransaction","UnitValue","PresentationValue"]:
        df_medicamento[col] = df_medicamento[col]\
        .replace('[\$\.]', '', regex=True).replace(",",".", regex=True).astype(float)
    df_medicamento["UnitsPerPresentation"] = df_medicamento["UnitsPerPresentation"]\
        .replace('[^0-9]', '', regex=True).apply(lambda x: int(x) if not pd.isnull(x) else None)
    if (len(df_medicamento) > 0):
        load_df_to_sql_pandas(df_medicamento,table_medicamentos,sql_connid, truncate= False)
        
        
def read_insumos_from_azure_storage(**kwargs):
    df_insumos = get_blob_2_df(container, insumos_file, skiprows = 1)
    df_insumos.columns = df_insumos.columns.str.strip().str.replace("\n"," ").str.replace("  "," ")
    
    df_insumos.rename(columns = 
                          {"CÓDIGO PRODUCTO":"cod_product", 
                           "REGISTO SANITARIO":"sanitary_registration",
                           "DESCRIPCION DEL PRODUCTO":"product_description",
                            "PRESENTACIÓN COMERCIAL":"comercial_presentation",  
                            "VALOR UNITARIO (SIN MARGEN)":"unit_value ",
                            "VALOR PRESENTACIÓN (SIN MARGEN)":"presentaion_value",
                            "IVA":"vat",
                            "REGULADO/ NO REGULADO (Si aplica)":"regulated",
                            "PBS / NO PBS":"pbs",
                            "OBSERVACIONES":"observations",}, inplace= True)
    
    cols = df_insumos.filter(regex=r"^Unnamed",axis=1 ).columns
    df_insumos.drop(cols, axis=1, inplace=True)
    df_insumos.drop("No.", axis=1, inplace=True)
    
    if (len(df_insumos) > 0):
        load_df_to_sql_pandas(df_insumos,table_insumos,sql_connid, truncate= False)
  
   
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
    # se establece la ejecución a las 12:20 PM(Hora servidor) todos los sabados
    schedule_interval= '0 6 * * 1',
    max_active_runs=1
    ) as dag:

    # Se declara la función que sirve para denotar el inicio del DAG a través de DummyOperator
    start_task = DummyOperator(task_id='dummy_start')


    #Se declara y se llama la función encargada de traer y subir los datos a la base de datos a través del "PythonOperator"
    read_tarifas_from_azure_storage_python_task = PythonOperator( task_id = "read_tarifas_from_azure_storage",
                                                        python_callable = read_tarifas_from_azure_storage,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    read_diagnosticos_from_azure_storage_python_task = PythonOperator( task_id = "read_diagnosticos_from_azure_storage",
                                                        python_callable = read_diagnosticos_from_azure_storage,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )
    
    
    read_medicamentos_from_azure_storage_python_task = PythonOperator( task_id = "read_medicamentos_from_azure_storage",
                                                        python_callable = read_medicamentos_from_azure_storage,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )    

    read_insumos_from_azure_storage_python_task = PythonOperator( task_id = "read_insumos_from_azure_storage",
                                                        python_callable = read_insumos_from_azure_storage,
                                                        email_on_failure=True, 
                                                        email='BI@clinicos.com.co',
                                                        dag=dag
                                                        )   

    # Se declara la función que sirva para denotar la Terminación del DAG, por medio del operador "DummyOperator"
    task_end = DummyOperator(task_id='task_end')

start_task >> read_tarifas_from_azure_storage_python_task  >> read_diagnosticos_from_azure_storage_python_task >> read_medicamentos_from_azure_storage_python_task >> read_insumos_from_azure_storage_python_task >> task_end