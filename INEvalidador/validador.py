from typing import List, Tuple
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import numpy as np
import unicodedata
import logging
import re
import os

class Validador:
    def __init__(self, ruta_base: str="BD_PERSONAS_PILOTO.sav", ruta_expresiones: str="Expresiones.xlsx"):
        self.df = pd.read_spss(ruta_base, convert_categoricals=False)
        self.df = self.df[self.df["PPA10"] == 1]
        self.expresiones = pd.read_excel(ruta_expresiones)
        self.columnas = ["DEPTO", "MUPIO","SECTOR","ESTRUCTURA","VIVIENDA","HOGAR", "CP"]
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

    def convertir_a_entero(self):
        columnas = list(self.df.columns)
        for columna in columnas:
            if np.issubdtype(self.df[columna].dtype, np.floating):
                self.df[columna] = self.df[columna].fillna(-1)
                self.df[columna] = pd.to_numeric(self.df[columna], downcast='integer')

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

    # Function to search and replace the matches
    def __translate(self, match):
        return self.__replacements[match.group(0)]

    def quitar_tildes(self, cadena: str) -> str:
        nfkd_form = unicodedata.normalize('NFKD', cadena)
        return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

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
        # Capturar convercion de cadena
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
        return condicion_convertida

    # Función para filtrar base de datos dada una query
    def filter_base(self, conditions: str, columnas: list) -> pd.DataFrame:
        return self.df.query(self.leer_condicion(conditions))[columnas]

    # Función para leer todos los criterios y exportar una carpeta por capítulo y un excel por sección 
    def process_general_data(self,columnas):
        self._capturar_converciones = True
        try:
            grouped = self.expresiones.groupby(["Capítulo", "Sección"])

            # Crear lista con expresiones para filtrar
            tuplas_chap_sec = [(name[0], name[1]) for name, _ in grouped]

            # Calcular el total de condiciones
            total_conditions = self.expresiones.shape[0]
            
            # Crear carpeta para guardar los archivos de inconsistencias generales y guardar el log de errores
            marca_temp = datetime.now().strftime("%Y%m%d%H%M%S")
            carpeta_padre = "Inconsistencias_{}".format(marca_temp)
            if not os.path.exists(carpeta_padre):
                os.mkdir(carpeta_padre)

            self.__configurar_logs(carpeta_padre)
            logging.info("Inicio del proceso de validación de datos.")
            logging.info("Se encontraron {} condiciones en {} secciones.".format(total_conditions, len(tuplas_chap_sec)))

            # Inicializar la barra de progreso
            pbar = tqdm(total=total_conditions, unit='condicion')

            # Leer filtros y tomar subconjuntos de la base
            for capitulo, seccion in tuplas_chap_sec:
                # Crear carpeta por capitulo
                folder_name = f"C{capitulo}"
                ruta_carpeta = os.path.join(carpeta_padre, folder_name)
                if not os.path.exists(ruta_carpeta):
                    os.makedirs(ruta_carpeta)
                conditions = list(self.expresiones[(self.expresiones["Capítulo"] == capitulo) & (self.expresiones["Sección"] == seccion)]["Condición o Criterio"])
                for condition in conditions:
                    try:
                        # Aplicar filtro a la base de datos
                        Validacion = self.filter_base(condition, columnas)
                        # Generar el nombre de la hoja
                        sheet_name = "S{}V{}".format(seccion, conditions.index(condition))
                        # Crea la ruta completa al archivo
                        filename = os.path.join(ruta_carpeta, "S{}.xlsx".format(seccion))
                        # Exportar subconjunto de datos a una hoja de Excel
                        Validacion.to_excel(filename, sheet_name=sheet_name)
                        # Actualizar la barra de progreso
                    except Exception as e:
                        # Manejar error específico de una expresión
                        logging.error(f"{condition}: {e}")
                        pass
                    finally:
                        pbar.update()

            # Cerrar la barra de progreso
            pbar.close()
        except Exception as e:
            # Manejar error general en caso de problemas durante el proceso
            logging.error(f"Error general: {e}")
            self._capturar_converciones = False

    # Función para leer todos los criterios y exportar un solo excel con las columnas DEPTO, MUPIO, HOGAR, CP, CAPITULO, SECCION
    def process_to_export(self,identificador):
        try:
            # Calcular el total de condiciones
            total_conditions = self.expresiones.shape[0]
            
            # Crear carpeta para guardar los archivos de inconsistencias generales y guardar el log de errores
            marca_temp = datetime.now().strftime("%Y%m%d%H%M%S")
            carpeta_padre = f"Inconsistencias_{marca_temp}"
            if not os.path.exists(carpeta_padre):
                os.mkdir(carpeta_padre)
            
            # Configurar logging
            self.__configurar_logs(carpeta_padre)
            logging.info("Inicio del proceso de validación de datos.")
            logging.info("Se encontraron {} condiciones.".format(total_conditions))

            # Inicializar la barra de progreso
            pbar = tqdm(total=total_conditions, unit='condicion')

            # Hacer cuadruplas con condicion, capitulo, seccion, etc
            conditions = list(self.expresiones["Condición o Criterio"])
            capitulos = list(self.expresiones["Capítulo"])
            secciones = list(self.expresiones["Sección"])
            descripcion_inconsistencia = list(self.expresiones["Definición de la Validación"])
            analista = list(self.expresiones["Analista"])
            codigo_error = list(self.expresiones["Código de Error"])

            cuadruplas_exportacion = list(zip(capitulos, secciones, descripcion_inconsistencia, conditions, analista, codigo_error))

            # Crear lista vacía para almacenar los dataframes resultantes
            dfs = []
            # Leer filtros y tomar subconjuntos de la base e ir uniendo las bases hasta generar una sola con las columnas solicitadas
            for i in range(len(cuadruplas_exportacion)):
                try:
                    # Aplicar filtro a la base de datos
                    Validacion = self.filter_base(cuadruplas_exportacion[i][3],self.columnas)
                    Validacion["CAPÍTULO"] = cuadruplas_exportacion[i][0]
                    Validacion["SECCIÓN"] = cuadruplas_exportacion[i][1]
                    Validacion["DEFINICIÓN DE INCONSISTENCIA"] = cuadruplas_exportacion[i][2]
                    Validacion["Código error"] = cuadruplas_exportacion[i][5]
                    Validacion["Analista"] = cuadruplas_exportacion[i][4]
                    dfs.append(Validacion)  # Agregar el dataframe a la lista de dataframes
                except Exception as e:
                    # Manejar error específico de una expresión
                    logging.error(f"{cuadruplas_exportacion[i][3]}: {e}. Error de {cuadruplas_exportacion[i][4]}")
                    pass
                finally:
                    # Actualizar barra de progreso
                    pbar.update()
            df_exportacion = pd.concat(dfs)  # Concatenar todos los dataframes de la lista
            df_exportacion.to_excel(os.path.join(carpeta_padre, f'Inconsistencias{identificador}.xlsx'),index=False)
            # Cerrar la barra de progreso
            pbar.close()

        except Exception as e:
            # Manejar error general en caso de problemas durante el proceso
            logging.error(f"Error general: {e}")


    # Función para devolver inconsistencias dado un analista, capitulo, seccion en especifico
    def process_specific_data(self, capitulo, seccion, analista):
        try:        
            # Crear lista con expresiones para filtrar
            expressions = list(self.expresiones[(self.expresiones["Analista"] == analista) & (self.expresiones["Capítulo"] == capitulo) & (self.expresiones["Sección"] == seccion)]["Condición o Criterio"])
            
            # Crear archivo tipo ExcelWriter para exportar en diferentes pestañas
            writer = pd.ExcelWriter("C{}S{}.xlsx".format(capitulo,seccion))
            
            # Leer filtros y tomar subconjuntos de la base
            for i in range(len(expressions)):
                try:
                    Validacion = self.filter_base(expressions[i])  # Aplicar filtro a la base de datos
                    sheet_name = "S{}V{}".format(capitulo, seccion, i)  # Generar el nombre de la hoja
                    Validacion.to_excel(writer, sheet_name=sheet_name)  # Exportar subconjunto de datos a una hoja de Excel
                except Exception as e:
                    print(f"Error al procesar la expresión {expressions[i]}: {e}")  # Manejar error específico de una expresión

            writer.save()  # Guardar el archivo de Excel con las hojas generadas
            print("Proceso completado exitosamente.")  # Indicar que el proceso ha finalizado con éxito
        
        except Exception as e:
            print(f"Error general: {e}")  # Manejar error general en caso de problemas durante el proceso

    def columnas_condicion_nula(self, condicion: str) -> List[Tuple[str, str]]:
        matches = [(m, '==') for m in re.findall(r'\b([A-Z0-9]+) == ""', condicion)]
        matches.extend([(m, '!=') for m in re.findall(r'\b([A-Z0-9]+) != ""', condicion)])
        return matches
    


