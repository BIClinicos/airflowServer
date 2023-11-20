from datetime import datetime, timedelta, date
from typing import Optional
from airflow.hooks.mssql_hook import MsSqlHook
from variables import sql_connid
from variables import connection_string
import pandas as pd
from airflow.contrib.hooks.wasb_hook import WasbHook
import os, re
from azure.storage.blob import ContainerClient
import xlrd
import pymssql
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from collections import Counter
from xlrd import Book
import numpy as np
from unicodedata import normalize
from numpy import vectorize

wb = WasbHook(wasb_conn_id= 'bs_clinicos_bi')

def last_day_of_month(any_day):
    """Returns last day of month

    Args: 
        any_day: Date in date.time format
    
    Returns: 
        Last date of month correspondig to any_day in date.time format. 
    """
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)

def add_days_to_date(date, days):
    """Add days to a date and return the date.
    
    Args: 
        date (string): Date string in YYYY-MM-DD format. 
        days (int): Number of days to add to date
    
    Returns: 
        date (date): Date in YYYY-MM-DD with X days added. 
    """

    return date + timedelta(days=days)

@vectorize
def normalize_strings(value:str):

    # -> NFD y eliminar diacríticos
    value = re.sub(
            r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+", r"\1", 
            normalize( "NFD", value), 0, re.I
        )

    # -> NFC
    value = normalize( 'NFC', value)

    return value

def normalize_str_categorical(df_serie,func_type='upper'):
  if func_type == 'upper':
    return df_serie.str.upper().str.strip()
  elif func_type == 'lower':
    return df_serie.str.lower().str.strip()

def sql_2_df(sql_query, **args) -> pd.DataFrame:
    sql_conn_id = args.get('sql_conn_id',sql_connid)
    print('in utils',sql_conn_id )
    sql_conn = MsSqlHook.get_connection(sql_conn_id)
    hook = sql_conn.get_hook()
    return hook.get_pandas_df(sql=sql_query)


def load_df_to_sql(df:pd.DataFrame, sql_table, sql_connid, truncate=True):
    """Function to upload excel file to SQL table"""
    # Convertir el DataFrame limpio a registros
    rows = df.to_records(index=False)
    rows_list = list(rows)
    
    try:
        row_list2 = [ tuple(None if item == 'None' or item == 'nan' or item == 'NAN' or pd.isnull(item) or pd.isna(item) or item == 'NaT' else item for item in row) for row in rows_list ]
    except TypeError:
        row_list2 = [ tuple(None if pd.isnull(item) or pd.isna(item) else item for item in row) for row in rows_list ]  
    
    # Upload data to SQL Server
    sql_conn = MsSqlHook(sql_connid)
    if truncate:
        try:
           sql_conn.run('TRUNCATE TABLE {}'.format(sql_table), autocommit=True)
        except (pymssql._pymssql.OperationalError,pymssql._mssql.MSSQLDatabaseException):
            insert_new_table(sql_table,df,sql_connid)
            return
        
    try:
        sql_conn.insert_rows(sql_table, row_list2, [f'[{val}]' for val in df.columns.to_list()],0)
    except (pymssql._mssql.MSSQLDatabaseException, pymssql._pymssql.ProgrammingError):
        sql_conn.insert_rows(sql_table, row_list2)
        
        
def insert_new_table(table, data:pd.DataFrame,sql_connid):
    query = f"INSET INTO {table} ({','.join([f'[{val}]' for val in data.columns])}) VALUES ({','.join(['%s'*len(data.columns)])})"
    print(query)
    params = data.to_dict('records')
    sql_conn = MsSqlHook(sql_connid)
    conn = sql_conn.get_conn()
    cursor = conn.cursor()
    # Ejecuta el update con executemany
    cursor.executemany(query, params)
    conn.commit()

    # Cierra el cursor y la conexión
    cursor.close()
    conn.close()
    
def get_engine(sql_connid,conn= None) -> Engine:
    if not conn: 
        sql_conn = MsSqlHook(sql_connid)
        conn = sql_conn.get_connection(sql_connid)
    if sql_connid == 'db_clinicos_bi':
        return create_engine(("mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER=srvbdclinicosbi.database.windows.net;DATABASE=BD_CLINICOS_BI;UID={conn.login};PWD={conn.password};"))
    else:
        return create_engine(("mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER=goreplica.database.windows.net;DATABASE=goMedisysCo_clinicos;UID={conn.login};PWD={conn.password};"))
    
    
def load_df_to_sql_pandas(df:pd.DataFrame, sql_table, sql_connid, pk:list=None,truncate= True):
    """Function to upload excel file to SQL table"""
    # df_cleaned = df.replace([np.nan,pd.NaT, pd.NA,'nan', 'NaT'], None)
    sql_conn = MsSqlHook(sql_connid)
    if truncate:
        sql_conn.run('TRUNCATE TABLE {}'.format(sql_table), autocommit=True)
    conn = sql_conn.get_connection(sql_connid)
    engine = get_engine( sql_connid,conn)
    df.to_sql(sql_table, engine, if_exists="append", index=False)
    


def load_df_to_sql_query(df:pd.DataFrame, sql_connid, query):
    """Function to upload excel file to SQL table"""
    # df_cleaned = df.replace([np.nan,pd.NaT, pd.NA,'nan', 'NaT'], None)
    sql_conn = MsSqlHook(sql_connid)
    # Convierte los valores NaN a None
    df = df.where(pd.notnull(df), None)
    conn = sql_conn.get_conn()
    cursor = conn.cursor()
    # Ejecuta el update con executemany
    cursor.executemany(query, df.to_records(index=False))
    conn.commit()

    # Cierra el cursor y la conexión
    cursor.close()
    conn.close()
    
    
def update_to_sql(data:list, sql_connid, query_update:str):
    """Function to upload excel file to SQL table"""
    sql_conn = MsSqlHook(sql_connid)
    conn = sql_conn.get_conn()
    cursor = conn.cursor()
    # Ejecuta el update con executemany
    cursor.executemany(query_update, data)
    conn.commit()

    # Cierra el cursor y la conexión
    cursor.close()
    conn.close()

#Función creada por FMGUTIERREZ
def load_df_to_sql_2(df, sql_table, sql_connid):
    """Function to upload excel file to SQL table"""
    rows = df.to_records(index=False)
    rows_list = list(rows)
    row_list2 = [ tuple(None if item == 'None' or item == 'nan' or item == 'NAN' or pd.isnull(item) or pd.isna(item) or item == 'NaT' else item for item in row) for row in rows_list ]
    # print(row_list2)

    # Upload data to SQL Server
    sql_conn = MsSqlHook(sql_connid)
    #sql_conn.run('TRUNCATE TABLE {}'.format(sql_table), autocommit=True)
    sql_conn.insert_rows(sql_table, row_list2)


def professional_names(value:str):
    if not value or pd.isnull(value): return None
    value = re.sub(r"(?: ?[-] ?){2,}","", value).strip()
    counter = Counter([val.strip() for val in value.split("-") if val.strip() != ""])
    return " - ".join([ f"{k} ({v})" for k, v in counter.items()])


def remove_accents_cols(df_cols):
    return df_cols.str.replace('ñ','ni').str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')


def replace_accents_cols(df_col):
    replacements = (
        ("Á", "A"),
        ("É", "E"),
        ("Í", "I"),
        ("Ó", "O"),
        ("Ú", "U"),
    )
    for a, b in replacements:
        df_col = df_col.replace(a, b).replace(a.upper(), b.upper())
    return df_col


def remove_special_chars(df_cols):
    return df_cols.str.replace(r'[$@&/.:-]',' ', regex=True)


def regular_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def regular_snake_case(df_cols):
    cols = df_cols.str.replace('ñ','ni')
    cols = cols.str.lower().str.replace('/',' ').str.replace('.',' ').str.strip()
    cols = cols.str.replace(r'\s+',' ',regex=True)
    cols = cols.str.replace(' ','_')
    return cols


def drop_from_sql_table(sql_table, sql_connid,condition):
    sql_conn = MsSqlHook(sql_connid)
    sql_conn.run('DELETE FROM {} WHERE {}'.format(sql_table, condition), autocommit=True)


def check_connection(container_name,blob_name):
    print('Conexión OK')
    return(wb.check_for_blob(container_name,blob_name))


def file_get(path,container_name,blob_name, **args):

    wbook:WasbHook = args.get('wb',wb)

    print ('Archivo halado con python operator', path,container_name, blob_name)
    wbook.get_file(path, container_name, blob_name)
    print ('Archivo halado con python operator')
    return('Blob gotten sucessfully')


def get_blob_2_df(container_name,blob_name, **args):

    wbook:WasbHook = args.get('wb',wb)
    header = args.get("head",0)
    skiprows = args.get("skiprows",0)
    sheets = args.get("sheets",None)
    blob = wbook.download(container_name, blob_name).content_as_bytes()
    if sheets:
        dfs = {}
        for sheet in sheets:
            dfs[sheet] = pd.read_excel(blob, sheet, skiprows=skiprows)
        return dfs
    return pd.read_excel(blob, skiprows=skiprows)


def filelist_get(dirname, container_name, blob_prefix, blob_delimiter: Optional[str] = '/', **args):

    wbook:WasbHook = args.get('wb',wb)
    if wbook.check_for_prefix(container_name, blob_prefix):
        blist_names = wbook.get_blobs_list(container_name, blob_prefix, delimiter = blob_delimiter)
        print('Blobs encontrados ', ' '.join(blist_names))
        for file in blist_names:
            wbook.get_file(dirname + file, container_name, file)
            print ('Archivo halado con python operator', dirname+file,container_name, file)
        return('Blobs gotten sucessfully')
    else:
        return('No blob available')


def respond():
    return 'Task ended'


def read_excel(dirname,filename,sheet) -> pd.DataFrame:
    path = os.path.join(dirname, filename)
    excel_to_df = pd.read_excel(path, engine = 'openpyxl',sheet_name=sheet)
    return excel_to_df

def red_excel_big(path, sheet_names:Optional[list]):
    workbook:Book = xlrd.open_workbook(path)
    if sheet_names:
        dfs = {}
        for sheet_name in sheet_names:
            data = []
            sheet = workbook.sheet_by_name(sheet_name)
            for row in range(sheet.nrows):
                data.append(sheet.row_values(row))
            dfs[sheet_name] = pd.DataFrame.from_records(data[1:],columns=data[0])
        return dfs
    return workbook.sheet_by_index(0)


def read_excel_args(**args):

    dirname = args.get('dirname',None)
    sheet = args.get('sheet',None)
    engine = args.get('engine','openpyxl')
    filename = args.get('filename',None)
    header =  args.get('header',None)
    usecols =  args.get('usecols',None)
    skiprows =  args.get('skiprows',None)
    print('filename',filename)
    

    print(engine)
    path = os.path.join(dirname, filename)
    # ERROR - Missing optional dependency 'xlrd'. Install xlrd >= 1.0.0 for Excel support Use pip or conda to install xlrd.
    excel_to_df = pd.read_excel(path,sheet_name=sheet, engine=engine, header=header,usecols=usecols, skiprows=skiprows)
    return excel_to_df


def read_excel_usecols(dirname,filename,sheet,usecols):
    path = os.path.join(dirname, filename)
    excel_to_df = pd.read_excel(path, engine = 'openpyxl',sheet_name=sheet,usecols=usecols)
    return excel_to_df


def read_csv(dirname,filename,separador, encoding='utf-8',header=0):
    path = os.path.join(dirname, filename)
    csv_to_df = pd.read_csv(path, sep=separador, low_memory=False, encoding=encoding, warn_bad_lines=True, error_bad_lines=False, header=header)
    # if engine != 'c':
    #     csv_to_df = pd.read_csv(path, sep=separador)
    # else:
    return csv_to_df


def read_csv_args(dirname,filename, **args):

    sep = args.get('sep')
    encoding = args.get('encoding','utf-8')
    decimal = args.get('decimal',',')
    usecols = args.get('usecols')
    skiprows =  args.get('skiprows',None)
    header =  args.get('header',None)

    path = os.path.join(dirname, filename)
    #csv_to_df = pd.read_csv(path, sep=sep, low_memory=False, encoding=encoding, warn_bad_lines=True, err_bad_lines=None, decimal=decimal, usecols=usecols)
    csv_to_df = pd.read_csv(path, sep=sep, low_memory=False, encoding=encoding, warn_bad_lines=True, error_bad_lines=False, decimal=decimal, usecols=usecols,skiprows=skiprows)
    return csv_to_df


def get_files_xlsx_with_prefix(dirname, name_container,blob_prefix, sheet):
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        if cadena.startswith(blob_prefix):
             path = dirname + cadena
             file_get(path, name_container, cadena)
             print('----File Downloaded----')
             d = read_excel(dirname, cadena, sheet)
             df = pd.DataFrame(data=d)
             if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df

    return df_acumulated


def get_files_xlsx_with_prefix_args(dirname,name_container,blob_prefix,sheet,**args):

    usecols = args.get('usecols', None)
    header = args.get('header', None)
    skiprows = args.get('skiprows', None)
    engine = args.get('engine', 'openpyxl')
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        if cadena.startswith(blob_prefix):
             path = dirname + cadena
             print("PATH : ", path, "CADENA : ", cadena, "CONTAINER : ", name_container )
             file_get(path, name_container, cadena)
             print('----File founded and Downloaded----')
             df = pd.DataFrame()
             try:
                d = read_excel_args( dirname=dirname, filename=cadena, sheet=sheet, usecols=usecols,header=header, skiprows=skiprows, engine=engine)
                df = pd.DataFrame(data=d)
                # print('DATAFRAME FILE',df)
             except Exception as e:
                print('ERROR',e)
                print('DATAFRAME FILE ERROR',df)
             if (~df_acumulated.empty & ~df_acumulated.empty):
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df

    return df_acumulated


def get_files_xlsx_contains_name(dirname, name_container,blob_prefix, sheet):
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        if blob_prefix in cadena:
             path = dirname + cadena
             file_get(path, name_container, cadena)
             print('----File Downloaded----')
             d = read_excel(dirname, cadena, sheet)
             df = pd.DataFrame(data=d)
             if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df

    return df_acumulated 


def get_files_xlsx_contains_name_args(dirname, name_container,blob_prefix, sheet,**args):

    usecols = args.get('usecols', None)
    header = args.get('header', None)
    skiprows = args.get('skiprows', None)
    engine = args.get('engine', 'openpyxl')
    connection_str = args.get('connection_str', connection_string)
    wb = args.get('wb', connection_string)

    container = ContainerClient.from_connection_string(conn_str=connection_str, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        if blob_prefix in cadena:
             path = dirname + cadena
             file_get(path, name_container, cadena, wb=wb)
             print('----File Downloaded----')
             d = read_excel_args( dirname=dirname, filename=cadena, sheet=sheet, usecols=usecols,header=header, skiprows=skiprows, engine=engine)
             df = pd.DataFrame(data=d)
             if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df

    return df_acumulated    


def get_files_xlsx_with_prefix_usecols(dirname, name_container,blob_prefix, sheet,usecols):
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        if cadena.startswith(blob_prefix):
             path = dirname + cadena
             file_get(path, name_container, cadena)
             print('----File Downloaded----')
             d = read_excel_usecols(dirname, cadena, sheet,usecols)
             df = pd.DataFrame(data=d)
             if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df

    return df_acumulated


def get_files_xlsx_for_ending(dirname, name_container,blob_ending, sheet):
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        if cadena.endswith(blob_ending):
             path = dirname + cadena
             file_get(path, name_container, cadena)
             print('----File Downloaded----')
             d = read_excel(dirname, cadena, sheet)
             df = pd.DataFrame(data=d)
             if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df
                

    return df_acumulated

    
def get_files_with_prefix(dirname, name_container,blob_prefix, separador,encoding='utf-8',header=0):
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        print('blob names',cadena)
        if cadena.startswith(blob_prefix):
             path = dirname + cadena
             file_get(path, name_container, cadena)
             print('----File Downloaded----')
             d = read_csv(dirname, cadena, separador,encoding,header=0)
             print(' AFTER READ FILE ')
             df = pd.DataFrame(data=d)
             if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df

    return df_acumulated


def get_files_blob_with_prefix_args(dirname,name_container,blob_prefix,wasb_hook,**args):
    sep = args.get('sep',None)
    encoding = args.get('encoding','utf-8')
    decimal = args.get('decimal',',')
    header = args.get('header', None)
    skiprows =  args.get('skiprows',None)

    blob_list = wasb_hook.get_blobs_list(name_container,blob_prefix)
    print('blob_list',blob_list)
    df = pd.DataFrame()
    for blob in blob_list:
        if blob.startswith(blob_prefix) :
            print('Get file ', dirname+blob)
            print('container: ', name_container)
            print('filename: ', blob)
            file_get(dirname+blob, name_container, blob)
            print('----File Downloaded----')
            d = read_csv_args(dirname, blob, sep=sep,encoding=encoding, decimal=decimal,skiprows=skiprows,header=header)
            df = pd.DataFrame(data=d)

    return df


def get_files_with_prefix_args(dirname,name_container,blob_prefix,**args):

    sep = args.get('sep',None)
    encoding = args.get('encoding','utf-8')
    decimal = args.get('decimal',',')
    header = args.get('header', None)
    skiprows =  args.get('skiprows',None)

    print('este es el encoding = ', encoding)
        
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        if cadena.startswith(blob_prefix) :
             path = dirname + cadena
             print('Archivo a traer: ', path)
             print('Contenedor: ', name_container)
             print('Cadena: ', cadena)
             file_get(path, name_container, cadena)
             print('----File Downloaded----')

             print('este es el encoding = ', encoding)

             d = read_csv_args(dirname, cadena, sep=sep,encoding=encoding, decimal=decimal,skiprows=skiprows,header=header)
             print(' AFTER READ FILE ')
             df = pd.DataFrame(data=d)
             if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df
    return df_acumulated


def get_files_with_ending_args(dirname,name_container,blob_ending,**args):

    sep = args.get('sep',None)
    encoding = args.get('encoding','utf-8')
    decimal = args.get('decimal',',')
    usecols = args.get('usecols', None)
        
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        if cadena.endswith(blob_ending):
             path = dirname + cadena
             file_get(path, name_container, cadena)
             print('----File Downloaded----')
             d = read_csv_args(dirname, cadena, sep=sep,encoding=encoding, decimal=decimal, usecols=usecols)
             print(' AFTER READ FILE ')
             df = pd.DataFrame(data=d)
             if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df


    return df_acumulated


def get_files_for_ending(dirname, name_container,blob_ending, separador):
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        print('blob names',cadena)
        if cadena.endswith(blob_ending):
             path = dirname + cadena
             file_get(path, name_container, cadena)
             print('----File', cadena ,'  Downloaded----')
             d = read_csv(dirname, cadena, separador)
             df = pd.DataFrame(data=d)
             if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
             if df_acumulated.empty:
                df_acumulated = df
                print('this is a test empty file')        
        # df_acumulated.info()
    return df_acumulated


def clean_container_for_prefix(name_container,prefix):
   container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
   blob_list = container.list_blobs()
   for blob in blob_list:
        cadena = blob.name
        if cadena.startswith(prefix):
            wb.delete_file(name_container,cadena, is_prefix=False, ignore_if_missing=True)


def clean_container_for_ending(name_container,ending):
   container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
   blob_list = container.list_blobs()
   for blob in blob_list:
        cadena = blob.name
        if cadena.endswith(ending):
            wb.delete_file(name_container,cadena, is_prefix=False, ignore_if_missing=True)


def move_to_history_folder_ending(dirname, name_container, ending,container_to='history'):
   container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
   blob_list = container.list_blobs()
   for blob in blob_list:
        cadena = blob.name
        if cadena.endswith(ending):
            time = datetime.now()
            time = str(time)
            path = dirname + cadena
            save_as = cadena + time
            wb.load_file(path, container_to, save_as)
            wb.delete_file(name_container,cadena, is_prefix=False, ignore_if_missing=True)


def move_to_history_for_prefix(dirname, name_container, prefix, container_to='history'):
   container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
   blob_list = container.list_blobs()
   for blob in blob_list:
        cadena = blob.name
        if cadena.startswith(prefix):
            time = datetime.now()
            time = str(time)
            path = dirname + cadena
            save_as = cadena + time
            wb.load_file(path, container_to, save_as)
            wb.delete_file(name_container,cadena, is_prefix=False, ignore_if_missing=True)


def move_to_history_contains_name(dirname, name_container, file_name, container_to='historicos',**args):
   connection_str = args.get('connection_str', connection_string)
   wb = args.get('wb', connection_string)
   container = ContainerClient.from_connection_string(conn_str=connection_str, container_name=name_container)
   blob_list = container.list_blobs()
   for blob in blob_list:
        cadena = blob.name
        print('CADENA',cadena)
        if file_name in cadena:
            time = datetime.now()
            time = str(time)
            path = dirname + cadena
            save_as = cadena + time
            wb.load_file(path, container_to, save_as)
            wb.delete_file(name_container,cadena, is_prefix=False, ignore_if_missing=True)


def get_table_from_db(sql):
    conn = MsSqlHook.get_connection(sql_connid)
    hook = conn.get_hook()
    return hook.get_pandas_df(sql=sql)


def search_for_file_prefix(file_name,name_container, connection_str=connection_string):
    print('search_for_file_prefix', file_name, name_container)
    container = ContainerClient.from_connection_string(conn_str=connection_str, container_name=name_container)
    print('container', container)
    blob_list = container.list_blobs()
    for blob in blob_list:
        print('blobname', blob.name)
        if blob.name.startswith(file_name):
            return True
    return False


def search_for_file_contains(file_name,name_container):
    container = ContainerClient.from_connection_string(conn_str=connection_string, container_name=name_container)
    blob_list = container.list_blobs()
    for blob in blob_list:
        if file_name in blob.name:
            return True
    return False


def get_files_xlsx_add_column_from_filename(dirname, name_container, blobname_contains, sheet, **args ):

    usecols = args.get('usecols', None)
    header = args.get('header', None)
    skiprows = args.get('skiprows', None)
    engine = args.get('engine', 'openpyxl')
    connection_str = args.get('connection_str', connection_string)
    wb = args.get('wb', connection_string)
    name_separator = args.get('usecols', '_')
    name_position = args.get('usecols', 0)
    new_col = args.get('usecols', '')

    container = ContainerClient.from_connection_string(conn_str=connection_str, container_name=name_container)
    blob_list = container.list_blobs()
    df_acumulated= pd.DataFrame()
    for blob in blob_list:
        cadena = blob.name
        if blobname_contains in cadena:
            filename=cadena
            path = dirname + filename
            file_get(path, name_container, filename,wb=wb)
            d = read_excel_args(dirname=dirname, filename=filename, sheet=sheet, usecols=usecols,header=header, skiprows=skiprows, engine=engine)
            df = pd.DataFrame(data=d)
            print('******filename', filename)

            # df[new_col]= extract_from_filename(filename, '_', 1 )
            if ~df_acumulated.empty:
                df_acumulated = pd.concat([df_acumulated,df] , ignore_index=True)
            if df_acumulated.empty:
                df_acumulated = df
    return [df_acumulated,filename]


def extract_from_filename(filename, name_separator='_', name_position=0):
    text_splited = filename.split(name_separator)
    return text_splited[name_position]

def validate_date_format(test_str,format):
    try:
        res = bool(datetime.strptime(test_str, format))
    except ValueError:
        res = False
    return res

def excel_date_format(y, col):
    x = y[col]  
    # Elimina el contenido decimal con regex
    x = x.str.replace(r'\.\d+$', '', regex=True)  
    # Intenta convertir a numérico
    numeric_x = pd.to_numeric(x, errors='coerce')
    # Verifica si el valor es una fecha en formato YYYY-MM-DD
    date_format_mask = x.str.match(r'\d{4}-\d{2}-\d{2}')   
    # Convierte valores a datetime64[ns]
    x = np.where(
        numeric_x.isna(),
        np.where(date_format_mask, x, None),
        datetime(1899, 12, 30) + pd.to_timedelta(numeric_x, unit='D')
    ) 
    # Convierte en serie
    x = pd.to_datetime(x, errors = 'coerce')
    return x


def open_xls_as_xlsx(filename):
    # first open using xlrd
    book = xlrd.open_workbook(filename)
    index = 0
    nrows, ncols = 0, 0
    while nrows * ncols == 0:
        sheet = book.sheet_by_index(index)
        nrows = sheet.nrows+1   #bm added +1
        ncols = sheet.ncols+1   #bm added +1
        index += 1

    # prepare a xlsx sheet
    book1 = Workbook()
    sheet1 = book1.get_active_sheet()

    for row in range(1, nrows):
        for col in range(1, ncols):
            sheet1.cell(row=row, column=col).value = sheet.cell_value(row-1, col-1) #bm added -1's

    return book1


def identify_updated_data (df, col_date, last_day, process, audit_table = "biDataUpdatesAudit", con = 'db_clinicos_bi'):
    """ Auditoria de actualizacion de datos de cargue manual

        Para procesos donde se dependa del cargue de archivos a la bodega de datos y se tenga un
        campo de fechas que pueda ser usado para establecer si la informacion esta actualizada, 
        se puede hacer uso de este metodo en compagnia de los correctos operadores en el DAG.

    Arg:
        df: data frame sobre el cual se realiza el ETL
        col_date: nombre de la columna que contiene las fechas para usar la comparacion
        last_day: fecha tomada como referecia para decir si la informacion esta desactualizada
        process: nombre del proceso o DAG involucrado
        audit_tabe: nombre de la tabla usada
    Retorno:
        caracter (string): indicador de branch, puede ser retrasd
    """
    m_date = df[col_date].max()
    data = {
        'data_date' : m_date, 
        'event' : 'Data retrasada', 
        'description' : f'La informacion del {process} cargada no está al día',
        'dateTime' : date.today()
    }
    df_load = pd.DataFrame([data])
    if m_date < last_day:
        load_df_to_sql_2(df_load, audit_table, con)
        return 'retrasada'
    else: 
        return 'al_dia'