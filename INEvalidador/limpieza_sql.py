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
    def __init__(self, usuario, contraseña, host, puerto, comision):
        self.sql = baseSQL(False)
        self.limpieza = Limpieza()
        self.ruta_archivo_query = str
        self.ruta_escritorio = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        self.ruta_limpieza = os.path.join(self.ruta_escritorio, "Sintaxis")
        # atributos de conexion
        self.usuario = usuario
        self.password = contraseña
        self.host = host
        self.puerto = puerto
        self.comision = comision

    def conexion_sintaxis(self, ruta_archivo: str):
        self.ruta_archivo_query = self.limpieza.escribir_query_sq(ruta_archivo)
        with open(self.ruta_archivo_query, "r", encoding="utf-8") as archivo:
            primera_linea = archivo.readline()
            match = re.search(r'UPDATE\s+(\w+)\.', primera_linea)
            if match:
                base_de_datos = match.group(1) + f"_COM{self.comision}"
                self.url_conexion = f"mysql+mysqlconnector://{self.usuario}:{self.password}@{self.host}:{self.puerto}/{base_de_datos}"
        self.engine = create_engine(self.url_conexion)

    def conexion_descarga(self, tipo: str) -> None:
        base_de_datos = f"ENCOVI_{tipo}_COM{self.comision}"
        self.url_conexion = f"mysql+mysqlconnector://{self.usuario}:{self.password}@{self.host}:{self.puerto}/{base_de_datos}"
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
 