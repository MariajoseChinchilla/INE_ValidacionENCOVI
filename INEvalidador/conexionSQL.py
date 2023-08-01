import mysql.connector
import pandas as pd
import os
from sqlalchemy import create_engine, text

class baseSQL:
    def __init__(self):
        # Parámetros de conexión
        usuario = 'mchinchilla'
        contraseña = 'Mchinchilla$2023'
        host = '10.0.0.90'
        puerto = '3307'

        # Crear la conexión de SQLAlchemy
        engine_PR = create_engine(f'mysql+mysqlconnector://{usuario}:{contraseña}@{host}:{puerto}/ENCOVI_PR')
        engine_SR = create_engine(f'mysql+mysqlconnector://{usuario}:{contraseña}@{host}:{puerto}/ENCOVI_SR')
        self.__conexion_PR = engine_PR.connect()
        self.__conexion_SR = engine_SR.connect()

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
                df.to_feather(os.path.join(dir_salida, f'{tabla_nombre}.feather'))

            except Exception as e:
                print(f'> Error al convertir la tabla {tabla_nombre} en un DataFrame y exportarlo: {str(e)}')
        
    def extraer_base(self):
        self.tablas_a_feather('PR', 'db/ronda_1')
        self.tablas_a_feather('SR', 'db/ronda_2')

p = baseSQL()
p.extraer_base()