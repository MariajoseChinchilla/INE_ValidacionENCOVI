from sqlalchemy import create_engine, text
import pandas as pd
import os
from datetime import datetime
import re

from INEvalidador.conexionSQL import baseSQL
from INEvalidador.limpieza import Limpieza
from INEvalidador.utils import columnas_a_mayuscula, condicion_a_variables

class LimpiezaSQL:
    def __init__(self, usuario, contraseña, host, puerto, ruta_archivo, comision):
        self.sql = baseSQL(False)
        self.limpieza = Limpieza()
        # ahora es un Excel
        self.ruta_archivo = ruta_archivo
        self.ruta_archivo_query = str
        self.ruta_escritorio = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        self.ruta_limpieza = os.path.join(self.ruta_escritorio, "Limpieza")

        self.escribir_query_sq(self.ruta_archivo)
        with open(self.ruta_archivo_query, "r", encoding="utf-8") as archivo:
            primera_linea = archivo.readline()
            match = re.search(r'UPDATE\s+(\w+)\.', primera_linea)
            if match:
                base_de_datos = match.group(1) + f"_COM{comision}"
                self.url_conexion = f"mysql+mysqlconnector://{usuario}:{contraseña}@{host}:{puerto}/{base_de_datos}"
        self.engine = create_engine(self.url_conexion)

    def escribir_query_sq(self, archivo, comision, usuario, fecha_inicio:datetime="2023-1-1", fecha_final:datetime="2025-12-31"):
            now = datetime.now()
            date_str = now.strftime("%d-%m-%Y")
            ruta_sintaxis = os.path.join(self.ruta_limpieza, "Sintaxis en SQL", f"output{date_str}")
            if not os.path.exists(ruta_sintaxis):
                os.makedirs(ruta_sintaxis)
            nombre = os.path.basename(archivo).split(".")[0].replace(" ", "_")
            ruta_archivo = os.path.join(ruta_sintaxis, f"Sintaxis_{nombre}.txt")
            self.ruta_archivo_query = ruta_archivo
            # Tomar el df original subido por el analista y obtener el valor en la llave primaria a editar
            df_original = pd.read_excel(archivo)
            variables_a_editar = list(var.split(".")[1].upper() for var in df_original["variable"])
            deptos = list(df_original["depto"])
            mupios = list(df_original["mupio"])
            sectores = list(df_original["sector"])
            estructuras = list(df_original["estructura"])
            viviendas = list(df_original["vivienda"])
            hogares = list(df_original["hogar"])
            cps = list(df_original["cp"])
            # Hacer de la identificación cartográfica un filtro
            condiciones = list(zip(variables_a_editar, deptos, mupios, sectores, estructuras, viviendas, hogares, cps))
            dfs = []
            filtros = []
            for idx, (var, depto, mupio, sec, estru, vivi, hog, cp) in enumerate(condiciones):
                filtro = f"DEPTO = {depto} & MUPIO = {mupio} & SECTOR = {sec} & ESTRUCTURA = {estru} & VIVIENDA = {vivi} & HOGAR = {hog} & CP = {cp}"
                filtro.replace("& CP = 0","").replace("& CP = nan","")
                llave_primaria = self.sql.base_col.get(var).replace("_PR","").replace("_SR","").upper() + "-ID"
                df_query = self.limpieza.filtrar_base_limpieza(filtro, [llave_primaria], fecha_inicio, fecha_final)
                filtros.append(filtro)
                # Agregar columnas "variable" y "valor_nuevo" a df_query
                df_query["variable"] = df_original.at[idx, "variable"]
                df_query["valor nuevo"] = df_original.at[idx, "valor nuevo"]
                dfs.append(df_query)
            df_queries = pd.concat(dfs)
            # Escribir sintaxis SQL usando el df_queries para tomar la llave primaria de la tabla a editar
            # Pendiente arreglar qué pasa si se quieren editar variables de diferentes tablas
            
            df_queries = df_queries.dropna(subset=["variable"])
            df_queries = df_queries.dropna(subset=["valor nuevo"])
            vars = list(df_queries["variable"])
            id_columns = [col for col in df_queries.columns if col.endswith('-ID')]
            ids = list(df_queries[id_columns[0]])
            ids_y_vars = list(zip(ids, variables_a_editar))
            valores_antiguos = []
            for i in range(len(df_original) - 1):
                valores_antiguos.append(df_original.loc[i, vars[i]])
            print(valores_antiguos)

            tablas = []
            for i in vars:
                if isinstance(i, str) and "." in i:
                    tablas.append(self.sql.base_col.get(i.split(".", 1)[1].upper()))
            valores_nuevos = list(df_queries["valor nuevo"])
            ronda = [f"ENCOVI_{tablas[0][-2:]}"] * len(tablas)
            comisiones = [comision] * len(tablas)
            vars = [var.split(".", 1)[1] for var in vars]
            tablas = [tabla[:-3] for tabla in tablas]
            cuadruplas = list(zip(ronda, tablas, vars, valores_nuevos, filtros, ids, comisiones, valores_antiguos))
            for id, var in ids_y_vars:
                tabla = self.sql.base_col.get(var).replace("_PR","").replace("_SR","") + "-id"
                filtros.append(f"{tabla} = {id}")
            fecha = datetime.now()
            for rond, tabla, variable, valor_nuevo, filtro, id, comision, valor_viejo in cuadruplas:
                with open(ruta_archivo, "a") as archivo:
                    # archivo.write(f"UPDATE {rond}.{tabla} AS {tabla} JOIN `level-1` ON {tabla}.`level-1-id` = `level-1`.`level-1-id` SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")
                    # archivo.write(f"INSERT INTO {base_datos}.{tabla (bitácora)} SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")
                    archivo.write(f"UPDATE {rond}_COM{comision}.{tabla} AS {tabla}  SET {variable} = {valor_nuevo} WHERE {tabla}.`{tabla}-id` = {id}; \n")
                    archivo.write(f"UPDATE ine_encovi.bitacora AS bitacora  SET usuario = {usuario} and base_datos = {rond}_COM{comision} and tabla = {tabla} and variable = {variable} and valor_anterior = {valor_viejo} and valor_nuevo = {valor_nuevo} and id_registro = {id} and fecha_creacion = {fecha}; \n")
        
    def ejecutar_consulta_desde_archivo(self):
        """
        Ejecuta consultas SQL desde un archivo de texto. Cada línea se considera una consulta separada.
        
        Parameters:
            ruta_archivo (str): Ruta al archivo que contiene las consultas SQL.
        """
        with open(self.ruta_archivo_query, 'r', encoding='utf-8') as archivo:
            with self.engine.connect() as conexion:
                for linea in archivo:
                    consulta_sql = linea.strip()  # Elimina espacios en blanco al principio y al final
                    if consulta_sql:  # Evita ejecutar líneas vacías
                        conexion.execute(text(consulta_sql))
