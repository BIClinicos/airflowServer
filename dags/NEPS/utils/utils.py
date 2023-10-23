import re
from numpy import vectorize
import pandas as pd
from datetime import datetime

def regex_hours(value:str):
    value = value.replace('"',"")
    pathern = re.search(r"(?:o2|ox).+?(?=\s{4}|-|\n|$)", value)
    if pathern:
        pathernHour = re.search(r"(?:^| )((?:o2|ox(?:)).+?[^\d])(\d+\s?h(?:o)?r?a?s?)", pathern.group(0))
        if pathernHour:
            return pathernHour.group(2)
        
def regex_only_hours(value):
    pathernHour = re.search(r"(?:^| )((?:o2|ox(?:)).+?[^\d])(\d+\s?h(?:o)?r?a?s?)", value)
    if pathernHour:
        return pathernHour.group(2)

def regex_hours_last(value:str):
    pathern = re.search(r"\d\s?l(?:i?t?r?o?s?).+[^\d](\d+\s?h(?:o?r?a?s?))", value)
    if pathern: return pathern.group(1)

def get_hours(row:pd.Series):
    row = row.astype(str).str.lower().str.replace("  "," ")
    hour = regex_hours(row["Recomendaciones"])
    if hour: return hour
    else: 
        hour = regex_hours(row["Recomendaciones"])
        if hour: return hour
    if not hour: 
        hour = regex_only_hours(row["Recomendaciones"])
        if hour: return hour
    if not hour: 
        hour = regex_hours_last(row["Recomendaciones"])
        if hour: return hour
    else:
        return hour

def other_hours(value:str, pather=None):
    if not value: return None
    value = value.lower()
    if not pather: 
        value = value.replace("  ", " ")
        pather=r"(\d+\s?h(?:o?(?:ra?)?s?))"
    pathern = re.search(pather, value) 
    if pathern:
        return pathern.groups()[0]
    if value.__contains__("sueño") or value.__contains__("noche") or value.__contains__("nocturno"): return '12 horas'

def generar_rango_fechas(group:pd.DataFrame):
    fecha_minima = group['date_control'].min().strftime('%Y-%m-%d')
    fecha_maxima = group['date_control'].max().strftime('%Y-%m-%d')
    rango_fechas = pd.date_range(start=fecha_minima, end=fecha_maxima, freq='MS')
    if len(rango_fechas) == 0 and fecha_minima == fecha_maxima:
        df_auxiliar = pd.DataFrame({'date_control': [fecha_maxima]})
    elif len(rango_fechas) == 0 and fecha_minima != fecha_maxima:
        df_auxiliar = pd.DataFrame({'date_control': [fecha_minima,fecha_maxima]})
    else:
        df_auxiliar = pd.DataFrame({'date_control': rango_fechas})
        
    df_auxiliar["idUser"],df_auxiliar["Documento"] = group["idUser"].iloc[0],group["Documento"].iloc[0]
    df_auxiliar["date_control"] = pd.to_datetime(df_auxiliar["date_control"])
    df_combinado = pd.merge(df_auxiliar, group, on=["date_control","idUser","Documento"], how='left')
    df_combinado = df_combinado.ffill().bfill()
    return df_combinado

def obtener_fila_prevaleciente(group):
    # Si todas las columnas son nulas excepto una, esa será la fila prevaleciente
    nulos = group.isnull().sum(axis=1)
    if (nulos == len(group.columns) - 1).any():
        return group[~group.isnull().any(axis=1)].iloc[0]
    else:
        # Si al menos dos columnas no son nulas, deja cualquiera
        return group.iloc[0]


def get_dispositivo(value:str):
    if not value or pd.isna(value): return None
    value = str(value).lower()
    path = re.search(r"concen?t|cn?o?ne?ce?nt?r?a?d?o?r?",value)
    if path : return "Concentrador"
    path = re.search(r"bala| balñ?a",value)
    if path : return "Bala de oxigeno"
    path = re.search(r"termo",value)
    if path : return "Termo productor de oxigeno"
    
    
def get_cpap_bpap(value:str):
    if not value or pd.isna(value): return None
    value = str(value).lower()
    path = re.search(r" ?cp[a-z]?p",value)
    if path : return "CPAP"
    path = re.search(r" ?bp[a-z]?p",value)
    if path : return "BPAP"
    
    
def regex_suspencion_oxigeno(value: str):
    if value:
        path = re.search(r"(?<=:^|\s)suspensi[oó]n(?: \w+ )*ox[ií]geno", value)
        if path:
            return True
        else:
            return False
    else: return False
    
def regex_disnea(value: str):
    if value:
        str_path = re.search(r"mmrc(?: \w+ )*\d+(?:\/\d+|-\d+)*", value)
        if str_path:
            return re.sub(r"mmrc(?: \w+ )*", "", str_path.group(0))
    else: return None

def regex_exacerbacion(value: str):
    if value:
        value = re.sub(r"libre de |libre |sin |no ", "", value)
        value = re.sub(r"evidencia de |presenta |signos de |s[ií]ntomas de ", "", value)
        path = re.search(r"exacerbaci[oó]n", value)
        if path: 
            return True
    return False
        
def regex_control_asma(value:str):
    if value:
        path = re.search(r"asma(?:\s*\w+ )*\s?(?<!no )controlad[ao]", value)
        if path:
            return True
        
        
def generar_rango_horas(group: pd.DataFrame):
    fecha_minima = group['date_control'].min()
    fecha_maxima = group['date_control'].max()
    horas_inicial = horas_final = None

    str_horas_min = group.loc[group["date_control"]
                              == fecha_minima, "Horas_Oxigeno"].iloc[-1]
    str_horas_max = group.loc[group["date_control"]
                              == fecha_maxima, "Horas_Oxigeno"].iloc[-1]

    if str_horas_min:
        pathern = re.search(
            "\d+", group.loc[group["date_control"] == fecha_minima, "Horas_Oxigeno"].iloc[-1])
        if pathern:
            horas_inicial = int(pathern.group(0))

    if str_horas_max:
        pathern = re.search(
            "\d+", group.loc[group["date_control"] == fecha_maxima, "Horas_Oxigeno"].iloc[-1])
        if pathern:
            horas_final = int(pathern.group(0))

    if horas_inicial and horas_final:
        if horas_inicial == horas_final:
            return pd.Series([(horas_final - horas_inicial), "Sin cambios"], index=["cambio_horas", "detalle_horas"])
        else:
            return pd.Series([(horas_final - horas_inicial), "Ajuste en horas"], index=["cambio_horas", "detalle_horas"])
    if not horas_inicial and not horas_final:
        return pd.Series([None, "En valoración"], index=["cambio_horas", "detalle_horas"])
    if horas_inicial and not horas_final:
        return pd.Series([None, "Suspensión de Oxígeno"], index=["cambio_horas", "detalle_horas"])