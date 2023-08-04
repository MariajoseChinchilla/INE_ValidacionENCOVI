from typing import List, Tuple
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import numpy as np
import unicodedata
import logging
import re
import os

from .conexionSQL import baseSQL


class Validador:
    def __init__(self, ruta_expresiones: str="Expresiones.xlsx", descargar: bool=True):
        # nuevo
        self.sql = baseSQL(descargar)
        self.df = pd.DataFrame
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

        self.dic_upms = {
            'GRUPO1': [29, 220, 395, 575, 767, 966, 1532, 1733, 1883, 1925],
            'GRUPO2': [2611, 2774, 2924, 3081, 3227, 3307, 3478, 3686, 3780, 6096],
            'GRUPO3': [0, 2423, 2233, 3857, 1159, 1345, 2198, 5815, 5734, 5758, 5781],
            'GRUPO4': [4021, 4252, 4567, 4723, 4873, 4409, 4990, 5203, 5233, 5368],
            'GRUPO5': [19256, 5537, 5485, 5502, 5508, 5693, 5705, 5574, 5577, 5600],
            'GRUPO6': [6310, 6332, 6241, 6263, 6282, 6211, 6234, 5932, 5953, 5955],
            'GRUPO7': [5838, 5859, 5885, 5901, 6176, 6116, 6139, 6164, 6064, 6086],
            'GRUPO8': [5989, 6012, 6023, 6582, 6593, 6646, 6378, 6433, 6481, 6517],
            'GRUPO9': [7105, 7128, 7201, 6737, 6795, 7013, 7037, 6951, 6809, 6876],
            'GRUPO10': [7950, 8013, 8034, 8104, 7769, 7542, 7826, 7860, 8205, 8270],
            'GRUPO11': [8343, 8380, 7286, 7349, 7410, 7476, 9071, 9148, 8957, 9009],
            'GRUPO12': [8591, 8619, 8688, 8720, 9220, 8786, 8843, 8859, 24731, 24185],
            'GRUPO13': [9442, 9382, 9320, 9370, 9713, 9749, 9846, 9911, 9980, 9827],
            'GRUPO14': [9525, 9541, 9592, 9657, 8461, 8475, 8535, 7559, 7618, 7702],
            'GRUPO15': [10233,10289,10361,10375,10454,10474,10528,10585,10657,11048],
            'GRUPO16': [10738,10807,10876,10952,11012,11518,10002,10061,10103,10161],
            'GRUPO17': [11525,12049,11337,11395,12093,11196,11270,13179,11980,11792],
            'GRUPO18': [12448,13215, 13282, 13294, 13329,12314,12372,12384,12501,12939],
            'GRUPO19': [12131,12181,12200,12249,12554,12568,12644,12700,12721,12771],
            'GRUPO20': [12991,13028,13059,13101,13146,11835,11904,13425,13384,13502],
            'GRUPO21': [14180,14320,14441,13938,14054,15312,13543,13594,13699,13783],
            'GRUPO22': [14585,14649,14731,14853,15019,15173,11637,11679,12840,12856],
            'GRUPO23': [17096,16469,16615,16165,16935,15701,18457,17301,17326,17426],
            'GRUPO24': [16362,16306,15976,15835,17243,15417,15534,15555,16774,11175],
            'GRUPO25': [18573,18345,17550,17946,18022,18084,18227,17843,17653,17744],
            'GRUPO26': [19197,19013,19053,19065,19151,19111,18927,18951,18810,18850],
            'GRUPO27': [18686,21061,20769,20808,20927,20653,20279,20390,20503,19350],
            'GRUPO28': [19423,19549,19678,19851,19923,20019,20143,19270,19306,18892],
            'GRUPO29': [22012,22025,21175,21198,21606,21416,21480,21539,21890,21946],
            'GRUPO30': [21294,21315,21623,21684,21745,21817,22388,22439,22502,22567],
            'GRUPO31': [22487,22158,22202,22248,22262,22309,22649,22699,22748,22797],
            'GRUPO32': [22851,22903,23387,23419,23172,23210,23246,23098,23109,23143],
            'GRUPO33': [23288, 23319, 23344, 22926, 22961, 23009, 23044, 5680, 5620, 5643],
            'GRUPO34': [22976,23457,23523,23534,23590,23726,23786,23845,23905,23995],
            'GRUPO35': [24063, 24113, 23655, 24570, 24598, 24666, 24677, 24785, 25042, 25056],
            'GRUPO36': [24835, 24882,24935,24989,25708,25455,25524,25570,25589,25639],
            'GRUPO37': [24223,24265,24284,24324,24364,24407,24450,24495,24537,25184],
            'GRUPO38': [25130, 25199, 25261, 25347, 25400, 25315, 8947]
            }
        
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
    def filter_base(self, condicion: str, columnas: list) -> pd.DataFrame:
        self.df = self.sql.df_para_condicion(condicion)
        return self.df.query(self.leer_condicion(condicion))[columnas]

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
    def process_to_export(self):
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
            pregunta = list(self.expresiones["Pregunta"])
            descripcion_inconsistencia = list(self.expresiones["Definición de la Validación"])
            codigo_error = list(self.expresiones["Código de Error"])

            cuadruplas_exportacion = list(zip(capitulos, secciones, descripcion_inconsistencia, conditions, pregunta, codigo_error))

            # Crear lista vacía para almacenar los dataframes resultantes
            dfs = []
            # Leer filtros y tomar subconjuntos de la base e ir uniendo las bases hasta generar una sola con las columnas solicitadas
            for i in range(len(cuadruplas_exportacion)):
                try:
                    # Aplicar filtro a la base de datos
                    Validacion = self.filter_base(cuadruplas_exportacion[i][3],self.columnas)
                    Validacion["CAPÍTULO"] = cuadruplas_exportacion[i][0]
                    Validacion["SECCIÓN"] = cuadruplas_exportacion[i][1]
                    Validacion["Pregunta"] = cuadruplas_exportacion[i][4]
                    Validacion["DEFINICIÓN DE INCONSISTENCIA"] = cuadruplas_exportacion[i][2]
                    Validacion["Código error"] = cuadruplas_exportacion[i][5]
                    dfs.append(Validacion)  # Agregar el dataframe a la lista de dataframes
                except Exception as e:
                    # Manejar error específico de una expresión
                    logging.error(f"{cuadruplas_exportacion[i][3]}: {e}. Error de {cuadruplas_exportacion[i][4]}")
                    pass
                finally:
                    # Actualizar barra de progreso
                    pbar.update()
                    
            df_power = pd.concat(dfs) # Hacer copia de los dfs para exportar por supervisor luego
            df_power.to_csv(os.path.join(carpeta_padre, 'InconsistenciasPowerBi.csv'), index=False)

            for upm, sectors in self.dic_upms.items():
                # Filtra las filas donde la columna "SECTOR" está en los valores de la UPM actual
                filtered_df = df_power[df_power['SECTOR'].isin(sectors)]

                # Exporta el DataFrame filtrado a un archivo Excel
                filtered_df.to_excel(os.path.join(carpeta_padre, f'Inconsistencias{upm}.xlsx'), index=False)

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
    