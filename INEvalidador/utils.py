import pkg_resources
import re
import pandas as pd
from typing import List

# Función para convertirlas todas las columnas de la base a mayuscula
def columnas_a_mayuscula(df: pd.DataFrame):
    columnas_originales = df.columns
    columnas_nuevas = []
    for columna in columnas_originales:
        col = columna.upper()
        columnas_nuevas.append(col)
    diccionario = dict(zip(columnas_originales, columnas_nuevas))
    df = df.rename(columns=diccionario)
    return df

def condicion_a_variables(condicion: str) -> List[str]:
    # La expresión regular coincide con cualquier cadena que comience con una letra mayúscula seguida de números y letras mayúsculas
    pattern = r'\b[A-Z][A-Z0-9]+\b'
    # Utilizamos la función findall para encontrar todas las coincidencias en el texto
    return tuple(set(re.findall(pattern, condicion)))

def extract_number(s):
    # Extraer el número deseado de la cadena
    return int(s[7:-2])

def extraer_UPMS():
    # Crear un diccionario vacío para almacenar los resultados
    archivo_UPMS= pkg_resources.resource_filename(__name__, 'extras/UPMS.xlsx')
    df = pd.read_excel(archivo_UPMS)
    dic_upms = {}

    # Agrupar por 'GRUPO' y recorrer cada grupo
    for group, group_data in df.groupby('GRUPO'):
        upms = []
        for idx, row in group_data.iterrows():
            # Si 'SUSTITUTO UPM' no es NaN, tomar ese valor, de lo contrario, tomar el valor de 'UPM'
            upm_value = row['SUSTITUTO UPM'] if pd.notna(row['SUSTITUTO UPM']) else row['UPM']
            upms.append(extract_number(upm_value))
        dic_upms[f"GRUPO{group}"] = upms

    return dic_upms