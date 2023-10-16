from sqlalchemy import create_engine, text
import pandas as pd
import os
from datetime import datetime
import re

from INEvalidador.conexionSQL import baseSQL
from INEvalidador.limpieza import Limpieza
from INEvalidador.utils import columnas_a_mayuscula, condicion_a_variables

import os
import re
import pandas as pd
from sqlalchemy import create_engine

class LimpiezaSQL:
    def __init__(self, usuario, contraseña, host, puerto, ruta_archivo, comision):
        self.sql = baseSQL(False)
        self.limpieza = Limpieza()
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

    def descargar_tablas(self):
        # Obtenemos la lista de tablas de la base de datos
        lista_tablas = self.engine.table_names()

        # Determinamos si el sufijo es PR o SR
        sufijo = "_PR" if "PR" in self.url_conexion else "_SR"

        # Creando la ruta de la carpeta donde guardar las tablas
        ruta_final = os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision))
        if not os.path.exists(ruta_final):
            os.makedirs(ruta_final)  # Creamos la ruta si no existe

        for tabla in lista_tablas:
            df = pd.read_sql(tabla, self.engine)
            ruta_archivo_feather = os.path.join(ruta_final, f"{tabla}{sufijo}.feather")
            df.to_feather(ruta_archivo_feather)

        print(f"Tablas guardadas en: {ruta_final}")

    def df_para_limpieza(self, variables, fecha_inicio: datetime="2023-1-1", fecha_final: datetime="2100-12-31"):

        df_a_unir = list(set([self.base_col.get(var) for var in variables]))
        tipos = []
        for i in range(len(df_a_unir)):
            tipo = df_a_unir[i][-2:] # devuelve SR o PR
            tipos.append(tipo)
        tipos = list(set(tipos))
        tipo = tipos[0]

        
        df_a_unir = [self.base_df.get(archivo) for archivo in df_a_unir] 

        df_base = self.base_df.get(f'level-1_{tipo}')
        for df in df_a_unir:
            df = df.drop('INDEX', axis=1)
            df_base = pd.merge(df_base, df, on='LEVEL-1-ID', how='inner')

        df_cases = self.base_df.get(f'cases_{tipo}')
        df_base = pd.merge(df_base, df_cases, left_on='CASE-ID', right_on='ID', how='inner')
        df_base = df_base.query('DELETED == 0')

        # Si tipo es "PR", agregamos el dataframe "caratula_PR.feather"
        if len(tipos) == 1 and tipos[0] == 'PR':
            # Agregar dataframe con la caratula
            caratula_pr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), 'caratula_PR.feather'))
            caratula_pr_df = columnas_a_mayuscula(caratula_pr_df)
            # Agregar dataframe con las fechas
            tiempo_pr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), "tiempo_control_PR.feather"))
            tiempo_pr_df = columnas_a_mayuscula(tiempo_pr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_pr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX",axis=1)
            df_base = pd.merge(df_base, caratula_pr_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'
        
        # Si tipo es "SR", agregamos el dataframe "estado_de_boleta_SR.feather"
        elif len(tipos) == 1 and tipos[0] == 'SR':
            # Agregar dataframe estado boleta
            estado_boleta_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), 'estado_de_boleta_SR.feather'))
            estado_boleta_df = columnas_a_mayuscula(estado_boleta_df)
            # Agregar dataframe de control de tiempo
            tiempo_sr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision),  "control_tiempo_SR.feather"))
            tiempo_sr_df = columnas_a_mayuscula(tiempo_sr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_sr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX",axis=1)
            df_base = pd.merge(df_base, estado_boleta_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'

        # Si es validacion entre rondas, agregar tablas pertinentes
        elif len(tipos) == 2:
            # Agregar dataframe estado boleta
            estado_boleta_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), 'estado_de_boleta_SR.feather'))
            estado_boleta_df = columnas_a_mayuscula(estado_boleta_df)
            # Agregar dataframe de control de tiempo
            tiempo_sr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), "tiempo_control_PR.feather"))
            tiempo_sr_df = columnas_a_mayuscula(tiempo_sr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_sr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX_x",axis=1)
            df_base = pd.merge(df_base, estado_boleta_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'
            # Agregar dataframe con la caratula
            caratula_pr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision),  'caratula_PR.feather'))
            caratula_pr_df = columnas_a_mayuscula(caratula_pr_df)
            # Agregar dataframe con las fechas
            tiempo_pr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza",str(self.comision), "tiempo_control_PR.feather"))
            tiempo_pr_df = columnas_a_mayuscula(tiempo_pr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_pr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX_y",axis=1)
            df_base = df_base.drop("INDEX_x",axis=1)
            df_base = pd.merge(df_base, caratula_pr_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'

        # Validar solo las encuestas terminadas
        if "PPA10" in df_base.columns and "ESTADO_PR" in df_base.columns:      
            df_base = df_base[df_base["PPA10"] == 1]
        elif "ESTADO_SR" in df_base.columns:
            df_base = df_base[df_base["ESTADO_SR"] == 1]
        elif "ESTADO_PR" in df_base.columns and "PPA10" not in df_base.columns:
            df_base = df_base[df_base["ESTADO_PR"] == 1]
        # Validar ambas rondas terminadas en caso de validacion entre rondas para persona
        elif "ESTADO_PR" in df_base.columns and "PPA10" in df_base.columns and "ESTADO_SR" in df_base.columns:
            df_base = df_base[(df_base["PPA10"] == 1 and df_base["ESTADO_SR"] == 1)]

        # Agregar código CP = 0 para las validaciones de hogares
        if "CP" not in df_base.columns:
            df_base["CP"] = 0

        # Agregar filtrado por fecha tomando el capítulo 1 como inicio de la encuesta
        if "FECHA_INICIO_CAP_1" in df_base.columns:
            df_base["FECHA_INICIO_CAP_1"] = pd.to_datetime(df_base["FECHA_INICIO_CAP_1"], format="%d/%m/%y")
            df_base = df_base[(df_base["FECHA_INICIO_CAP_1"] >= fecha_inicio) & (df_base["FECHA_INICIO_CAP_1"] <= fecha_final)]
            df_base["FECHA"] = df_base["FECHA_INICIO_CAP_1"]
            
        if "FECHA_INICIO_CAPXIIIA" in df_base.columns:
            df_base["FECHA_INICIO_CAPXIIIA"] = pd.to_datetime(df_base["FECHA_INICIO_CAPXIIIA"], format="%d/%m/%y")
            df_base = df_base[(df_base["FECHA_INICIO_CAPXIIIA"] >= fecha_inicio) & (df_base["FECHA_INICIO_CAPXIIIA"] <= fecha_final)]
            df_base["FECHA"] = df_base["FECHA_INICIO_CAPXIIIA"]
            

        for columna in df_base.columns:
            if columna[-2:] == "_y":
                df_base.drop(columns=columna, inplace=True)
            if columna[-2:] == "_x":
                df_base.rename(columns={columna : columna[0:-2]}, inplace=True)
            if columna[-3:] == "_SR":
                df_base.rename(columns={columna : columna[0:-3]}, inplace=True)
            if columna[-3:] == "_PR":
                df_base.rename(columns={columna : columna[0:-3]}, inplace=True)

        return df_base
    
    def escribir_query_sq(self, archivo, comision, usuario, fecha_inicio:datetime="2023-1-1", fecha_final:datetime="2100-12-31"):
        now = datetime.now()
        date_str = now.strftime("%d-%m-%Y")
        ruta_sintaxis = os.path.join(self.ruta_limpieza, "Sintaxis en SQL", f"output{date_str}")
        if not os.path.exists(ruta_sintaxis):
            os.makedirs(ruta_sintaxis)
        nombre = os.path.basename(archivo).split(".")[0]
        ruta_archivo = os.path.join(ruta_sintaxis, f"{nombre}.txt")
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
