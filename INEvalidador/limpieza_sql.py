from sqlalchemy import create_engine, text
import pandas as pd
import os
from datetime import datetime
from INEvalidador.utils import columnas_a_mayuscula, condicion_a_variables
import re

class LimpiezaSQL:
    def __init__(self, usuario, contraseña, host, puerto, ruta_archivo, comision):
        self.ruta_archivo = ruta_archivo
        with open(self.ruta_archivo, "r", encoding="utf-8") as archivo:
            primera_linea = archivo.readline()
            match = re.search(r'UPDATE\s+(\w+)\.', primera_linea)
            if match:
                base_de_datos = match.group(1) + f"_COM{comision}"
                self.url_conexion = f"mysql+mysqlconnector://{usuario}:{contraseña}@{host}:{puerto}/{base_de_datos}"
        self.engine = create_engine(self.url_conexion)
        print(base_de_datos)
    
    def ejecutar_consulta_desde_archivo(self):
        """
        Ejecuta consultas SQL desde un archivo de texto. Cada línea se considera una consulta separada.
        
        Parameters:
            ruta_archivo (str): Ruta al archivo que contiene las consultas SQL.
        """
        with open(self.ruta_archivo, 'r', encoding='utf-8') as archivo:
            with self.engine.connect() as conexion:
                for linea in archivo:
                    consulta_sql = linea.strip()  # Elimina espacios en blanco al principio y al final
                    if consulta_sql:  # Evita ejecutar líneas vacías
                        conexion.execute(text(consulta_sql))
