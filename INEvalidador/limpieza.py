import os
from datetime import datetime
import pandas as pd
import pkg_resources
import re 
from .utils import extraer_UPMS, condicion_a_variables
import numpy as np
import copy
import logging
from tqdm import tqdm
import unicodedata
from .conexionSQL import baseSQL
from INEvalidador.limpieza_sql import LimpiezaSQL

class Limpieza:
    def __init__(self, ruta_criterios_limpieza: str="", descargar: bool = False, host: str = '20.10.8.4', puerto: str = '3307', usuario: str = 'mchinchilla', 
                password: str = 'mchinchilla$2023'):
        self.ruta_escritorio = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        self.marca_temp = datetime.now().strftime("%d-%m-%Y")
        self.sql = baseSQL(descargar, host, puerto, usuario, password)

        self.salida_principal = os.path.join(self.ruta_escritorio, f"Validador\Datos para Revisión\output_{self.marca_temp}")
        if not os.path.exists(self.salida_principal):
            os.makedirs(self.salida_principal)
        self.criterios_limpieza = pd.read_excel(ruta_criterios_limpieza)
        self.columnas = ["FECHA", "DEPTO", "MUPIO","SECTOR","ESTRUCTURA","VIVIENDA","HOGAR", "CP","ENCUESTADOR"]
        self._capturar_converciones = False

        self.__replacements = {
            '<=': '<=',
            '<>': '!=',
            '>==': '>=',
            '<==': '<=',
            'no esta en': 'not in',
            'esta en': 'in',
            '\n': ' ',
            '\r': '',
            'no es (vacio)': 'no es vacio',
            'no es (vacio)': '!= ""',
            'no es vacio': '!= ""',
            'es (vacio)': '== ""',
            'es vacio': '== ""',
            'NA': 'None',
            '<>': '!=',
            ' =': '==',
        }
        # Precompile the regular expression for efficiency
        self.__patron = re.compile("|".join(map(re.escape, self.__replacements.keys())), flags=re.IGNORECASE)

        self.dic_upms = extraer_UPMS()

    def __configurar_logs(self, carpeta: str):
        # Configurar logging
        logging.basicConfig(
            filename=os.path.join(carpeta, f'root.log'),
            filemode='w',
            format='%(levelname)s - %(message)s',
            level=logging.DEBUG
        )

        # Crear un logger adicional para las conversiones de condiciones
        self.logger_conv = logging.getLogger('Logger_conv')
        handler1 = logging.FileHandler(os.path.join(carpeta, 'cond_conv.log'))
        formatter1 = logging.Formatter('%(levelname)s - %(message)s')
        handler1.setFormatter(formatter1)
        self.logger_conv.addHandler(handler1)
        self.logger_conv.setLevel(logging.DEBUG)
        self.logger_conv.info('Log de condiciones convertidas a formato pandas')
    
    def quitar_tildes(self, cadena: str) -> str:
        nfkd_form = unicodedata.normalize('NFKD', cadena)
        return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

    # Function to search and replace the matches
    def __translate(self, match):
        return self.__replacements[match.group(0)]
    
    def columnas_condicion_nula(self, condicion: str):
        matches = [(m, '==') for m in re.findall(r'\b([A-Z0-9]+) == ""', condicion)]
        matches.extend([(m, '!=') for m in re.findall(r'\b([A-Z0-9]+) != ""', condicion)])
        return matches

    def leer_condicion(self, condicion: str) -> str:
        # Quitar espacios extras
        condicion_convertida = ' '.join(condicion.split())
        condicion_convertida = self.quitar_tildes(condicion_convertida)
        # Para las columnas de texto, busca patrones del tipo 'variable = (vacio)' o 'variable no es (vacio)'
        text_var_pattern = r'(\w+)\s*(==|!=)\s*\((vacio)\)'
        text_var_matches = re.findall(text_var_pattern, condicion_convertida)

        for var, op in text_var_matches:
            if op == '==':
                condicion_convertida = condicion_convertida.replace(f'{var} {op} (vacio)', f'{var} == ""')
            elif op == '!=':
                condicion_convertida = condicion_convertida.replace(f'{var} {op} (vacio)', f'{var} != ""')

        # Reemplaza los símbolos y frases con su equivalente en Python
        condicion_convertida = self.__patron.sub(self.__translate, condicion_convertida)
        condicion_convertida = re.sub(r"\s+y\s+", " & ", condicion_convertida, flags=re.IGNORECASE)
        condicion_convertida = re.sub(r"\s+o\s+", " | ", condicion_convertida, flags=re.IGNORECASE)

        # Reemplaza las comparaciones entre variables para que sean legibles en Python
        condicion_convertida = re.sub(r'(\w+)\s*(<=|>=|<|>|==|!=)\s*(\w+)', r'\1 \2 \3', condicion_convertida)

        # Si "está en" se encuentra en la condición, lo reemplaza por la sintaxis correcta en Python
        if "está en" in condicion_convertida:
            condicion_convertida = re.sub(r'(\w+)\s+está en\s+(\(.*?\))', r'\1 in \2', condicion_convertida)
        
        # Agrega paréntesis alrededor de la condición
        condicion_convertida = '(' + condicion_convertida + ')'
        # Capturar conversion de cadena
        if self._capturar_converciones:
            self.logger_conv.info('{}  |--->  {}'.format(condicion, condicion_convertida))
        
        for col, tipo in self.columnas_condicion_nula(condicion_convertida):
            # Verificar si la columna es de tipo int o float
            if np.issubdtype(self.df[col].dtype, np.integer) or np.issubdtype(self.df[col].dtype, np.floating):
                # Sustituir cuando la columna sea de tipo numérica
                if tipo == "==":
                    condicion_convertida = condicion_convertida.replace(f'{col} {tipo} ""', f'{col}.isnull()')       #modificaciones para variables tipo numérica
                if tipo == "!=":
                    condicion_convertida = condicion_convertida.replace(f'{col} {tipo} ""', f'~{col}.isnull()')       #modificaciones para variables tipo numérica
            else:
                if tipo == "==":
                    condicion_convertida = condicion_convertida.replace(f'{col} {tipo} ""', f'({col}.isna() | {col} == "")')       #modificaciones para variables tipo numérica
                if tipo == "!=":
                    condicion_convertida = condicion_convertida.replace(f'{col} {tipo} ""', f'~{col}.isna() & {col} != ""')  
        return condicion_convertida

    def filtrar_base_limpieza(self, condicion: str, columnas: list, fecha_inicio: datetime="2023-1-1", fecha_final:datetime="2023-12-31") -> pd.DataFrame:
        # var = columnas + ["FECHA", "ENCUESTADOR", "DEPTO", "MUPIO", "SECTOR","ESTRUCTURA", "VIVIENDA", "HOGAR", "CP", "CAPITULO", "SECCION", "PREGUNTA", "DEFINICION DE INCONSISTENCIA", "CODIGO ERROR", "COMENTARIOS"]
        self.df = self.sql.df_para_limpieza(condicion, columnas)
        self.df["VARIABLE"] = None
        self.df["VALOR NUEVO"] = None  
        filtered_df = self.df.query(self.leer_condicion(condicion))[["DEPTO","MUPIO","SECTOR","ESTRUCTURA","VIVIENDA","HOGAR","CP"] + columnas + ["VARIABLE", "VALOR NUEVO"]]
        return copy.deepcopy(filtered_df)

    # Busca el archivo limpieza en carpeta de archivos
    def archivos_limpieza(self, fecha_inicio: datetime="2023-1-1", fecha_final: datetime="2100-12-31"):
        try:
            # Calcular el total de condiciones
            total_conditions = self.criterios_limpieza.shape[0]
            self.criterios_limpieza["VARIABLES A EXPORTAR"] = self.criterios_limpieza["VARIABLES A EXPORTAR"].str.replace(r'\s*,\s*', ',').str.split(r'\s+|,')
            # Configurar logging
            self.__configurar_logs(self.salida_principal)
            logging.info("Inicio del proceso de exportación de inconsistencias")
            logging.info(f"Se encontraron {total_conditions} condiciones.")

            # Inicializar la barra de progreso
            pbar = tqdm(total=total_conditions, unit='condicion')

            # Hacer cuadruplas con condicion, capitulo, seccion, etc
            nombre_arch = list(self.criterios_limpieza["NOMBRE ARCHIVO"])
            condicion = list(self.criterios_limpieza["CONDICION O CRITERIO"])
            variables = list(self.criterios_limpieza["VARIABLES A EXPORTAR"])
            cuadruplas_exportacion = list(zip(nombre_arch, condicion, variables))

            # Leer filtros y tomar subconjuntos de la base e ir uniendo las bases hasta generar una sola con las columnas solicitadas
            for nombre, cond, var in cuadruplas_exportacion:
                try:
                    # Aplicar filtro a la base de datos
                    Validacion = self.filtrar_base_limpieza(cond, var, fecha_inicio, fecha_final)
                    carto = set(["DEPTO", "MUPIO", "SECTOR", "ESTRUCTURA", "VIVIENDA", "HOGAR", "CP", "VARIABLE", "VALOR NUEVO"])
                    diff = list(set(Validacion.columns) - carto)
                    for i in diff:
                        Validacion.rename(columns={i: f"{self.sql.base_col[i][:-3]}.{i}".lower() for i in diff}, inplace=True)
                    Validacion.rename(columns={i: i.lower() for i in carto}, inplace=True)
                    if Validacion.shape[0] == 0:
                        continue 
                    Validacion.to_excel(os.path.join(self.salida_principal, f'{nombre}.xlsx'), index=False)
                except Exception as e:
                    # Manejar error específico de una expresión
                    logging.error(f"{cond}: {e}.")
                    continue 
                finally:
                    # Actualizar barra de progreso
                    pbar.update()

        except Exception as e:
            # Manejar error general en caso de problemas durante el proceso
            logging.error(f"Error general: {e}")

    # Generar archivos de limpieza de datos ingresando valores por campos de texto
    def limpieza_por_query(self, nombre, condicion: str, columnas: str, fecha_inicio: datetime= "2023-1-1", fecha_final: datetime = "2023-12-31" ):
        columnas = [col.strip() for col in columnas.split(",")]
        Validacion = self.filtrar_base_limpieza(condicion, columnas, fecha_inicio, fecha_final)
        carto = set(["DEPTO", "MUPIO", "SECTOR", "ESTRUCTURA", "VIVIENDA", "HOGAR", "CP", "VARIABLE", "VALOR NUEVO"])
        diff = list(set(Validacion.columns) - carto)
        for i in diff:
            Validacion.rename(columns={i: f"{self.sql.base_col[i][:-3]}.{i}".lower() for i in diff}, inplace=True)
        Validacion.rename(columns={i: i.lower() for i in carto}, inplace=True)
        if Validacion.shape[0] == 0:
            pass 
        Validacion.to_excel(os.path.join(self.salida_principal, f'{nombre}.xlsx'), index=False)
        marca_temp = datetime.now().strftime("%d-%m-%Y")
        # carpeta_padre = f"Limpieza/DatosLimpieza{marca_temp}"

        # Configurar logging
        self.__configurar_logs(self.salida_principal)
        for i in Validacion.columns:
            Validacion.rename(columns={i: i.lower()}, inplace=True)
        
        Validacion.to_excel(os.path.join(self.salida_principal, f'{nombre}.xlsx'), index=False)