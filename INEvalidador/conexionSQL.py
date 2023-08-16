from sqlalchemy import create_engine, text
import pandas as pd
import os
from datetime import datetime

from .utils import columnas_a_mayuscula, condicion_a_variables

class baseSQL:
    def __init__(self, descargar: bool=True):
        if descargar:
            # Parámetros de conexión
            usuario = 'mchinchilla'
            contraseña = 'mchinchilla$2023'
            host = '20.10.8.4'
            puerto = '3307'
            # Crear la conexión de SQLAlchemy
            engine_PR = create_engine(f'mysql+mysqlconnector://{usuario}:{contraseña}@{host}:{puerto}/ENCOVI_PR')
            engine_SR = create_engine(f'mysql+mysqlconnector://{usuario}:{contraseña}@{host}:{puerto}/ENCOVI_SR')
            self.__conexion_PR = engine_PR.connect()
            self.__conexion_SR = engine_SR.connect()
            self.extraer_base()
        # Diccionario para almacenar los nombres de los archivos y las columnas
        self.base_df = {}   # Diccionario que asocia nombre de df con el df
        self.base_col = {}  # Diciconario que asocia variable con el nombre del df

        # Recorre todos los archivos en el directorio especificado
        for archivo in os.listdir('db'):
            if archivo.endswith('.feather'):  # Verifica si el archivo es un archivo Feather
                ruta_completa = os.path.join('db', archivo)
                # Lee el archivo Feather
                df = pd.read_feather(ruta_completa)
                df = columnas_a_mayuscula(df)
                # Agrega el nombre del archivo y las columnas al diccionario
                nombre_df = archivo.replace('.feather', '')
                self.base_df[nombre_df] = df
                # Agregar columnas a base_col
                columnas = df.columns.tolist()
                try:
                    columnas.remove('LEVEL-1-ID')
                except:
                    pass
                try:
                    columnas.remove('INDEX')
                except:
                    pass
                for col in columnas:
                    self.base_col[col] = nombre_df

    def df_para_condicion(self, condicion: str, fecha_inicio, fecha_final):
        # PR, tomar primera ronda
        variables = condicion_a_variables(condicion)

        df_a_unir = list(set([self.base_col.get(var) for var in variables]))
        tipo = df_a_unir[0][-2:] # devuelve SR o PR
        
        df_a_unir = [self.base_df.get(archivo) for archivo in df_a_unir] 

        df_base = self.base_df.get(f'level-1_{tipo}')
        for df in df_a_unir:
            df = df.drop('INDEX', axis=1)
            df_base = pd.merge(df_base, df, on='LEVEL-1-ID', how='inner')

        df_cases = self.base_df.get(f'cases_{tipo}')
        df_base = pd.merge(df_base, df_cases, left_on='CASE-ID', right_on='ID', how='inner')
        df_base = df_base.query('DELETED == 0')

        # Si tipo es "PR", agregamos el dataframe "caratula_PR.feather"
        if tipo == 'PR':
            # Agregar dataframe con la caratula
            caratula_pr_df = pd.read_feather('db/caratula_PR.feather')
            caratula_pr_df = columnas_a_mayuscula(caratula_pr_df)
            # Agregar dataframe con las fechas
            tiempo_pr_df = pd.read_feather("db/tiempo_control_PR.feather")
            tiempo_pr_df = columnas_a_mayuscula(tiempo_pr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_pr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX",axis=1)
            df_base = pd.merge(df_base, caratula_pr_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'
        
        # Si tipo es "SR", agregamos el dataframe "estado_de_boleta_SR.feather"
        if tipo == 'SR':
            # Agregar dataframe estado boleta
            estado_boleta_df = pd.read_feather('db/estado_de_boleta_SR.feather')
            estado_boleta_df = columnas_a_mayuscula(estado_boleta_df)
            # Agregar dataframe de control de tiempo
            tiempo_sr_df = pd.read_feather("db/control_tiempo_SR.feather")
            tiempo_sr_df = columnas_a_mayuscula(tiempo_sr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_sr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX",axis=1)
            df_base = pd.merge(df_base, estado_boleta_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'

        # Validar solo las encuestas terminadas
        if "PPA10" in df_base.columns:      
            df_base = df_base[df_base["PPA10"] == 1]
        if "ESTADO_SR" in df_base.columns:
            df_base = df_base[df_base["ESTADO_SR"] == 1]
        if "ESTADO_PR" in df_base.columns:
            df_base = df_base[df_base["ESTADO_PR"] == 1]

        # Agregar código CP = 0 para las validaciones de hogares
        if "CP" not in df_base.columns:
            df_base["CP"] = 0

        # Agregar filtrado por fecha tomando el capítulo 1 como inicio de la encuesta
        if "FECHA_INICIO_CAP_1" in df_base.columns:
            df_base["FECHA_INICIO_CAP_1"] = pd.to_datetime(df_base["FECHA_INICIO_CAP_1"])
            df_base = df_base[(df_base["FECHA_INICIO_CAP_1"] >= fecha_inicio) & (df_base["FECHA_INICIO_CAP_1"] <= fecha_final)]
        if "FECHA_INICIO_CAPXIIIA" in df_base.columns:
            df_base["FECHA_INICIO_CAPXIIIA"] = pd.to_datetime(df_base["FECHA_INICIO_CAPXIIIA"])
            df_base = df_base[(df_base["FECHA_INICIO_CAPXIIIA"] >= fecha_inicio) & (df_base["FECHA_INICIO_CAPXIIIA"] <= fecha_final)]

        return df_base


    def info_tablas(self, tipo: str='PR'):
            conexion = self.__conexion_PR if tipo == 'PR' else self.__conexion_SR

            resultado = conexion.execute(text("SHOW TABLES"))
            tablas = [row[0] for row in resultado]

            i = 0
            for tabla_nombre in tablas:
                try:
                    # Usar text en las consultas
                    filas = conexion.execute(text(f"SELECT COUNT(*) FROM `{tabla_nombre}`")).fetchone()[0]
                    columnas = conexion.execute(text(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{tabla_nombre}'")).fetchone()[0]

                    i += 1
                    print(f"> {tabla_nombre}({i})\n   Filas: {filas} - Columnas: {columnas}")
                except Exception as e:
                    print(f'> Error "{e}" al obtener la forma de la tabla {tabla_nombre}')

    def tablas_a_feather(self, tipo: str = 'PR', dir_salida: str = 'output'):
        conexion = self.__conexion_PR if tipo == 'PR' else self.__conexion_SR

        resultado = conexion.execute(text("SHOW TABLES"))
        tablas = [row[0] for row in resultado]

        # Convertir cada tabla en un DataFrame y exportarlo en formato feather
        for tabla_nombre in tablas:
            try:
                # Formatear el nombre de la tabla correctamente con comillas invertidas
                tabla_con_comillas = f'`{tabla_nombre}`'
                df = pd.read_sql(text(f'SELECT * FROM {tabla_con_comillas}'), con=conexion)


                # Crear el directorio de salida si no existe 
                if not os.path.exists(dir_salida):
                    os.makedirs(dir_salida)
                df.reset_index(inplace=True)
                # Exportar el DataFrame en formato feather
                tabla_nombre = f"{tabla_nombre}_{tipo}"
                df.to_feather(os.path.join(dir_salida, f'{tabla_nombre}.feather'))

            except Exception as e:
                print(f'> Error al convertir la tabla {tabla_nombre} en un DataFrame y exportarlo: {str(e)}')
        
    def extraer_base(self):
        self.tablas_a_feather('PR', 'db')
        self.tablas_a_feather('SR', 'db')
