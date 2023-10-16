from typing import List, Tuple
from datetime import datetime
import pkg_resources
from tqdm import tqdm
import pandas as pd
import numpy as np
import unicodedata
import logging
import re
import copy
import pickle
import glob

import os

from .utils import extraer_UPMS, columnas_a_mayuscula
from .conexionSQL import baseSQL
from .scripR import ScripR

class Validador:
    def __init__(
            self,
            ruta_expresiones: str="",
            descargar: bool=True,
            ruta_criterios_limpieza: str="",
            ruta_UPM_R: str="",
            ruta_UPM_py: str=""):
        # atributos de rutas de archivos
        self.ruta_UPM_R = ruta_UPM_R
        self.ruta_UPM_py = ruta_UPM_py
        self.ruta_escritorio = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        self.dir_salida = os.path.join(self.ruta_escritorio, 'Validador\db')
        self.ruta_escritorio = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        # crea carpeta ValidadorINE en el escritorio en caso no exista
        if not os.path.exists(os.path.join(self.ruta_escritorio, "Validador")):
            os.mkdir(os.path.join(self.ruta_escritorio, "Validador"))
        # carpeta de salida principal
        self.marca_temp = datetime.now().strftime("%d-%m-%Y")
        self.salida_validaciones = os.path.join(self.ruta_escritorio, f"Validador\output_{self.marca_temp}")
        if not os.path.exists(self.salida_validaciones):
            os.makedirs(self.salida_validaciones)
        # demas atributos
        self.df_ = pd.DataFrame
        self.df = pd.DataFrame
        # nuevo
        self.sql = baseSQL(descargar)
        # si no se pasa la ruta de expresiones, se usa la ruta por defecto
        if not ruta_expresiones:
            ruta_expresiones = pkg_resources.resource_filename(__name__, "archivos\Expresiones.xlsx")
        self.expresiones = pd.read_excel(ruta_expresiones)
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

        self.dic_upms = extraer_UPMS(ruta=self.ruta_UPM_py)
        
    def obtener_carpeta_mas_reciente(self, directorio):
        carpeta_mas_reciente = None
        fecha_mas_reciente = None
        
        for carpeta in os.listdir(directorio):
            match = re.match(r'Inconsistencias_(\d{2}-\d{2}-\d{2}-\d{2})', carpeta)
            if match:
                fecha_str = match.group(1)
                fecha = datetime.strptime(fecha_str, '%d-%m-%H-%M')
                
                if fecha_mas_reciente is None or fecha > fecha_mas_reciente:
                    fecha_mas_reciente = fecha
                    carpeta_mas_reciente = carpeta
                    
        if carpeta_mas_reciente:
            return os.path.join(directorio, carpeta_mas_reciente)
        else:
            return None
    
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

    def columnas_condicion_nula(self, condicion: str) -> List[Tuple[str, str]]:
        # Elimina espacios extra y realiza reemplazos para convertir la condición al formato correcto
        condicion = ' '.join(condicion.split())  
        condicion = condicion.replace(" es vacio", ' == ""').replace(" no es vacio", ' != ""')

        # print(f"Condición tras reemplazo: {condicion}")

        pattern_equal = r'\b([A-Z0-9]+) == ""'
        matches_equal = re.findall(pattern_equal, condicion)
        # print(f"Coincidencias para == : {matches_equal}")

        pattern_not_equal = r'\b([A-Z0-9]+) != ""'
        matches_not_equal = re.findall(pattern_not_equal, condicion)
        # print(f"Coincidencias para != : {matches_not_equal}")

        matches = [(m, '==') for m in matches_equal]
        matches.extend([(m, '!=') for m in matches_not_equal])
        # print(f"Matches resultantes: {matches}")

        return matches
    
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
        # Capturar conversion de cadena
        if self._capturar_converciones:
            self.logger_conv.info('{}  |--->  {}'.format(condicion, condicion_convertida))
        # print(self.df.columns)
        # print(condicion_convertida)
        for col, tipo in self.columnas_condicion_nula(condicion_convertida):
            # Verificar si la columna es de tipo int o float
            # print(col)
            df = columnas_a_mayuscula(pd.read_feather(os.path.join(self.dir_salida, f"{self.sql.base_col.get(col)}.feather")))
            if np.issubdtype(df[col].dtype, np.integer) or np.issubdtype(df[col].dtype, np.floating):
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

    def filter_base(self, condicion: str, columnas: list, fecha_inicio: datetime="2023-1-1", fecha_final: datetime="2023-12-31") -> pd.DataFrame:
        self.df = self.sql.df_para_condicion(condicion, fecha_inicio, fecha_final)
        filtered_df = self.df.query(self.leer_condicion(condicion))[columnas]
        return copy.deepcopy(filtered_df)

    # Función para leer todos los criterios y exportar un solo excel con las columnas DEPTO, MUPIO, HOGAR, CP, CAPITULO, SECCION
    def process_to_export(self, fecha_inicio: datetime, fecha_final: datetime):
        try:
            # Calcular el total de condiciones
            total_conditions = self.expresiones.shape[0]

            self.ruta_carpeta_padre = os.path.join(self.salida_validaciones, "Validaciones_py")
            os.mkdir(self.ruta_carpeta_padre)

            # Configurar logging
            self.__configurar_logs(self.ruta_carpeta_padre)
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
            analista = list(self.expresiones["Analista"])

            cuadruplas_exportacion = list(zip(capitulos, secciones, descripcion_inconsistencia, conditions, pregunta, codigo_error, analista))

            # Crear lista vacía para almacenar los dataframes resultantes
            dfs = []
            # Leer filtros y tomar subconjuntos de la base e ir uniendo las bases hasta generar una sola con las columnas solicitadas
            for cap, sec, desc, cond, preg, cod, analista in cuadruplas_exportacion:
                try:
                    # Aplicar filtro a la base de datos
                    Validacion = self.filter_base(cond, self.columnas, fecha_inicio, fecha_final)
                    if Validacion.shape[0] == 0:
                        continue 
                    Validacion["CAPITULO"] = cap
                    Validacion["SECCION"] = sec
                    Validacion["PREGUNTA"] = preg
                    Validacion["DEFINICION DE INCONSISTENCIA"] = desc
                    Validacion["CODIGO ERROR"] = cod
                    Validacion["COMENTARIOS"] = None
                    Validacion["CONDICION"] = cond
                    Validacion = Validacion[["FECHA", "ENCUESTADOR","DEPTO","MUPIO","SECTOR","ESTRUCTURA","VIVIENDA","HOGAR","CP","CAPITULO","SECCION","PREGUNTA","DEFINICION DE INCONSISTENCIA","CODIGO ERROR","COMENTARIOS"]]
                    dfs.append(Validacion)  # Agregar el dataframe a la lista de dataframes
                except Exception as e:
                    # Manejar error específico de una expresión
                    logging.error(f"{cond}: {e}. Error de {analista}")
                    continue 
                finally:
                    # Actualizar barra de progreso
                    pbar.update()
                    
            dia = datetime.now().day
            mes = datetime.now().month
            año = datetime.now().year


            # Agregar validaciones de biyección entre CPs para actividad económica
            # Validación para capítulo 14
            expresion1 = "PPA03 >= 18 & CP no es vacio & (P10C21 está en (5,6) o P10D07 está en (5,6) o P10C47 > 0)"
            cap141 = self.filter_base(expresion1,["LEVEL-1-ID", "FECHA", "DEPTO", "CP", "ENCUESTADOR", "MUPIO", "SECTOR", "ESTRUCTURA", "VIVIENDA", "HOGAR"], fecha_inicio, fecha_final)
            expresion2 = "P14A04 no es vacio y P14A03A no es vacio"
            cap142 = self.filter_base(expresion2, ["P14A04", "P14A03A", "FECHA", "LEVEL-1-ID", "ENCUESTADOR", "DEPTO", "CP", "MUPIO", "SECTOR", "ESTRUCTURA", "VIVIENDA", "HOGAR"], fecha_inicio, fecha_final)
            # Agrupación conservando columnas adicionales
            agrupado14 = cap141.groupby(["LEVEL-1-ID"]).agg({
                'CP': lambda x: list(set(x)),
                'FECHA': 'first',
                'DEPTO': 'first',
                'MUPIO': 'first',
                'SECTOR': 'first',
                'ESTRUCTURA': 'first',
                'VIVIENDA': 'first',
                'HOGAR': 'first',
                "ENCUESTADOR": "first",
            }).reset_index()

            agrupado142 = cap142.groupby(["LEVEL-1-ID"]).agg({
                'P14A04': lambda x: list(set(x)),
                'FECHA': 'first',
                'DEPTO': 'first',
                'MUPIO': 'first',
                'SECTOR': 'first',
                'ESTRUCTURA': 'first',
                'VIVIENDA': 'first',
                "ENCUESTADOR": "first",
                'HOGAR': 'first',
            }).reset_index()

            # Fusión de dataframes
            final14 = agrupado14.merge(agrupado142, how="inner", on=["LEVEL-1-ID"], suffixes=('_x', '_y'))

            for columna in final14.columns:
                if columna[-2:] == "_x":
                    final14.rename(columns={columna: columna[:-2]}, inplace=True)
                if columna[-2:] == "_y":
                    final14.drop(columna, axis=1, inplace=True)

            # Comparar y añadir columna "COINCIDENCIA"
            final14["COINCIDENCIA"] = final14["CP"].astype(str) == final14["P14A04"].astype(str)

            final14["CAPITULO"] = 14
            final14["SECCION"] = "A"
            final14["PREGUNTA"] = 3
            final14["DEFINICION DE INCONSISTENCIA"] = "No está registrando los mismos CPs en actividad agrícola en el capítulo 10 y el 14. No es coincidente la información de los CPs registrados. Verifique sección C o D capítulo 10, persona ingresada como actividad económica no agrícola y no está siendo registrada en el capítulo 14 o bien, está registrando información en el capítulo 14 de una persona pero no la categorizó como actividad económica no agrícola en el capítulo 10."
            final14["CODIGO ERROR"] = "ACECO14"
            final14["COMENTARIOS"] = None

            final14 = final14[(final14["COINCIDENCIA"] == False) & (final14["CP"].notna()) & (final14["P14A04"].notna())]
            if not final14.empty:
                final14['CPs'] = final14.apply(lambda row: list(set(tuple(row['CP'])).symmetric_difference(set(tuple(row['P14A04'])))), axis=1)
                final14 = final14[["FECHA", "ENCUESTADOR", "DEPTO", "MUPIO", "SECTOR","ESTRUCTURA", "VIVIENDA", "HOGAR", "CPs", "CAPITULO", "SECCION", "PREGUNTA", "DEFINICION DE INCONSISTENCIA", "CODIGO ERROR", "COMENTARIOS"]]
                final14.rename(columns={"CPs": "CP"}, inplace=True)
                if "LEVEL-1-ID" in final14.columns:
                    final14.drop("LEVEL-1-ID", axis=1, inplace=True)
                if "P14A04" in final14.columns:
                    final14.drop("P14A04", axis=1, inplace=True)
                if "COINCIDENCIA" in final14.columns:
                    final14.drop("COINCIDENCIA", axis=1, inplace=True)

            if final14.empty:
                final14.rename(columns={"CPs": "CP"}, inplace=True)
                final14.drop("LEVEL-1-ID", axis=1, inplace=True)
                final14.drop("P14A04", axis=1, inplace=True)
                final14.drop("COINCIDENCIA", axis=1, inplace=True)

            # Unir esto al csv acumulado
            dfs.append(final14)
            self.df_ = dfs
            df_power = pd.concat(self.df_) # Hacer copia de los dfs para exportar por supervisor luego
            df_power.to_csv(os.path.join(self.ruta_carpeta_padre, f'InconsistenciasPowerBi_{dia}-{mes}-{año}.csv'), index=False)
            reporte_codigo = df_power.groupby(["CODIGO ERROR", "DEFINICION DE INCONSISTENCIA"]).size().reset_index(name="FRECUENCIA")
            reporte_encuestador = df_power.groupby(["ENCUESTADOR"]).size().reset_index(name="FRECUENCIA")
            reporte_codigo.to_excel(os.path.join(self.ruta_carpeta_padre, f'Frecuencias_por_codigo_{dia}-{mes}-{año}.xlsx'), index=False)
            reporte_encuestador.to_excel(os.path.join(self.ruta_carpeta_padre, f'Frecuencias_por_encuestador_{dia}-{mes}-{año}.xlsx'), index=False)

            for upm, sectors in self.dic_upms.items():
                # Filtra las filas donde la columna "SECTOR" está en los valores de la UPM actual
                filtered_df = df_power[df_power["SECTOR"].isin(sectors)]

                # Exporta el DataFrame filtrado a un archivo Excel
                filtered_df.to_excel(os.path.join(self.ruta_carpeta_padre, f'Inconsistencias{upm}_{dia}-{mes}-{año}.xlsx'), index=False)

            # Cerrar la barra de progreso
            pbar.close()
            

        except Exception as e:
            # Manejar error general en caso de problemas durante el proceso
            logging.error(f"Error general: {e}")


    def validar_encuesta(self, fecha_inicio: datetime, fecha_final: datetime):
        # Procesar datos para validar con validaciones originales
        self.process_to_export(fecha_inicio, fecha_final)
        # Ejecutar el scrip de Mario
        ScripR().procesar_datos(self.salida_validaciones, self.ruta_UPM_R)

        # Obtener la ruta a la carpeta más reciente
        # ruta_externa = self.obtener_carpeta_mas_reciente(self.salida_principal)

        # Concatenar los Exceles para generar la salida a reportar
        ruta_py = os.path.join(self.salida_validaciones, "Validaciones_py")
        ruta_r = os.path.join(self.salida_validaciones, "Inconsistencias_R")
        ruta_salida = os.path.join(self.salida_validaciones, "Salidas Finales")
        self.concatenar_exceles(ruta_py, ruta_r, ruta_salida)

    def concatenar_exceles(self, folder1, folder2, output_folder):

        # Buscar todos los archivos Excel en folder1
        folder1_files = glob.glob(f"{folder1}\InconsistenciasGRUPO*.xlsx")

        # Crear carpeta de salidas finales dentro de la carpeta de salidas
        self.ruta_salida_final = os.path.join(output_folder, "Salidas Finales")
        if not os.path.exists(self.ruta_salida_final):
            os.makedirs(self.ruta_salida_final)
            print(f"Se ha creado la carpeta: {self.ruta_salida_final}")
        else:
            print(f"La carpeta ya existe: {self.ruta_salida_final}")

        if not folder1_files:  # Si no hay archivos en folder1
            # Iterar a través de los archivos en folder2
            for folder2_file in glob.glob(f"{folder2}/InconsistenciasGRUPO*.xlsx"):
                group_number = folder2_file.split("GRUPO")[1].split("_")[0]

                # Leer el archivo Excel de folder2
                df2 = pd.read_excel(folder2_file)

                # Guardar el DataFrame en output_folder
                output_file = f"{self.ruta_salida_final}/InconsistenciasGRUPO{group_number}_{self.marca_temp}.xlsx"
                df2.sort_values(by=["DEPTO", "CODIGO ERROR"]).to_excel(output_file, index=False)
        else:
            for folder1_file in folder1_files:
                group_number = folder1_file.split("GRUPO")[1].split("_")[0]

                # Crear el nombre del archivo correspondiente en folder2
                folder2_file = f"{folder2}/InconsistenciasGRUPO{group_number}.xlsx"

                # Leer el archivo Excel de folder1
                df1 = pd.read_excel(folder1_file)

                if os.path.exists(folder2_file):
                    # Leer el archivo Excel de folder2
                    df2 = pd.read_excel(folder2_file)

                    # Concatenar ambos DataFrames
                    df_concatenated = pd.concat([df1, df2], ignore_index=True)
                else:
                    df_concatenated = df1

                # Guardar el DataFrame combinado en output_folder
                output_file = f"{self.ruta_salida_final}/InconsistenciasGRUPO{group_number}_{self.marca_temp}.xlsx"
                df_concatenated.sort_values(by=["DEPTO", "CODIGO ERROR"]).to_excel(output_file, index=False)
