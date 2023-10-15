import glob
import os
import re
from datetime import datetime
from typing import List

import openpyxl # al parecer no se usa
import pandas as pd
import pkg_resources

def columnas_a_mayuscula(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertir todas las columnas del DataFrame a mayúsculas.

    Args:
    - df (pd.DataFrame): DataFrame de entrada.

    Returns:
    - pd.DataFrame: DataFrame con nombres de columnas en mayúsculas.
    """
    columnas_originales = df.columns
    columnas_nuevas = [columna.upper() for columna in columnas_originales]
    diccionario = dict(zip(columnas_originales, columnas_nuevas))
    df = df.rename(columns=diccionario)
    return df

def condicion_a_variables(condicion: str) -> List[str]:
    """
    Extraer variables de una condición basada en una expresión regular.

    Args:
    - condicion (str): Cadena de entrada con la condición.

    Returns:
    - List[str]: Lista de variables extraídas.
    """
    pattern = r'\b[A-Z][A-Z0-9]+\b'
    matches = set(re.findall(pattern, condicion))
    blacklist = {"VACIO", "NO", "ES"}
    return [word for word in matches if word not in blacklist]

def extract_number(s: str) -> int:
    """
    Extraer un número de una cadena.

    Args:
    - s (str): Cadena de entrada.

    Returns:
    - int: Número extraído.
    """
    return int(s[7:-2])

def extraer_UPMS(ruta: str="") -> dict:
    # Crear un diccionario vacío para almacenar los resultados
    # si no se pasa una ruta, se usa el archivo que viene en el paquete
    if not ruta:
        ruta = pkg_resources.resource_filename(__name__, 'archivos/UPMS.xlsx')
    df = pd.read_excel(ruta)
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


def concatenar_exceles(folder1, folder2, output_folder):
    # Crear el directorio de salida si no existe
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Fecha actual
    now = datetime.now()
    date_str = now.strftime("%d-%m-%H-%M-%S")

    # Buscar todos los archivos Excel en folder1
    folder1_files = glob.glob(f"{folder1}/InconsistenciasGRUPO*.xlsx") 
    ruta_salida = f"{output_folder}/Salidas{date_str}"
    if not os.path.exists(ruta_salida):
        os.makedirs(ruta_salida)
        print(f"Se ha creado la carpeta: {ruta_salida}")
    else:
        print(f"La carpeta ya existe: {ruta_salida}")
    for folder1_file in folder1_files:
        # Obtener el número de grupo del nombre del archivo
        group_number = folder1_file.split("GRUPO")[1].split("_")[0]
        
        # Crear el nombre del archivo correspondiente en folder2
        folder2_file = f"{folder2}/InconsistenciasGRUPO{group_number}.xlsx"

        # Leer el archivo Excel de folder1
        df1 = pd.read_excel(folder1_file)

        # Verificar si existe el archivo correspondiente en folder2
        if os.path.exists(folder2_file):
            # Leer el archivo Excel de folder2
            df2 = pd.read_excel(folder2_file)
            
            # Concatenar ambos DataFrames
            df_concatenated = pd.concat([df1, df2], ignore_index=True)
        else:
            # Si no existe el archivo correspondiente en folder2, usar el original de folder1
            df_concatenated = df1

        # Guardar el DataFrame combinado en output_folder
        output_file = f"{ruta_salida}/InconsistenciasGRUPO{group_number}.xlsx"
        df_concatenated.to_excel(output_file, index=False)
