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