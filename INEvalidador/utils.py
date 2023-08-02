import pandas as pd

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