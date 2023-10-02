from INEvalidador import Validador
import pandas as pd
from datetime import datetime
from INEvalidador import conexionSQL
sql = conexionSQL(False)


def escribir_query_sq(archivo):
    df_queries = pd.read_excel(archivo)
    now = datetime.now()
    date_str = now.strftime("%d-%m-%Y")
    sintaxis = f"Sintaxis{date_str}.txt"
    # Iniciar ciclo de escritura de las sintaxis SQL
    vars = list(df_queries["variable"])
    tablas = [sql.base_col.get(i.split(".", 1)[1].upper()) for i in vars if "." in i]
    valores_nuevos = list(df_queries["valor nuevo"])
    ronda = [f"ENCOVI_{tablas[0][-2:]}"] * len(tablas)
    vars = [var.split(".", 1)[1] for var in vars]
    tablas = [tabla[:-3] for tabla in tablas]
    datos_cart = list(df_queries[["depto","mupio","sector","estructura","vivienda","hogar","cp"]].itertuples(index=False))
    filtros = []
    for dep, mup, sec, estr, viv, hog, cpp in datos_cart:
        filtros.append(f"'level-1'.depto = {dep} and 'level-1'.mupio = {mup} and 'level-1'.sector = {sec} and 'level-1'.estructura = {estr} and 'level-1'.vivienda = {viv} and 'level-1'.hogar = {hog} and 'level-1'.cp = {cpp}")
    cuadruplas = list(zip(ronda, tablas, vars, valores_nuevos, filtros))
    for rond, tabla, variable, valor_nuevo, filtro in cuadruplas:
        with open(sintaxis, "a") as archivo:
            archivo.write(f"UPDATE {rond}.{tabla} AS {tabla} SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")
    for rond, tabla, variable, valor_nuevo, filtro in cuadruplas:
        # Reemplazar la subcadena " and 'level-1'.cp = 0" por la cadena vacía
        filtro = filtro.replace(" and 'level-1'.cp = 0", "")
        with open(sintaxis, "a") as archivo:
            archivo.write(f"UPDATE {rond}.{tabla} AS {tabla} SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")  # \n para una nueva línea después de cada instrucción

# Considerar reemplazar la cadena cp = 0 por la vacia