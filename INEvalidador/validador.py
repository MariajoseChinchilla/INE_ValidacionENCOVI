from typing import List, Tuple
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import numpy as np
import unicodedata
import logging
import re
import copy
import pickle
import glob
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import os
import rpy2.robjects as robjects

from .utils import extraer_UPMS
from .conexionSQL import baseSQL
sql = baseSQL(False)



class Validador:
    def __init__(self, ruta_expresiones: str="Expresiones.xlsx", descargar: bool=True, criterios_limpieza: str="Limpieza.xlsx"):
        self.df_ = pd.DataFrame
        # nuevo
        self.sql = baseSQL(descargar)
        self.df = pd.DataFrame
        self.expresiones = pd.read_excel(ruta_expresiones)
        self.criterios_limpieza = pd.read_excel(criterios_limpieza)
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
        matches = [(m, '==') for m in re.findall(r'\b([A-Z0-9]+) == ""', condicion)]
        matches.extend([(m, '!=') for m in re.findall(r'\b([A-Z0-9]+) != ""', condicion)])
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

    def filter_base(self, condicion: str, columnas: list, fecha_inicio, fecha_final) -> pd.DataFrame:
        self.df = self.sql.df_para_condicion(condicion, fecha_inicio, fecha_final)
        filtered_df = self.df.query(self.leer_condicion(condicion))[columnas]
        return copy.deepcopy(filtered_df)
    
    def filtrar_base_limpieza(self, condicion: str, columnas: list, fecha_inicio, fecha_final) -> pd.DataFrame:
        self.df = self.sql.df_para_limpieza(condicion, columnas)
        filtered_df = self.df.query(self.leer_condicion(condicion))[columnas]
        return copy.deepcopy(filtered_df)


    # Función para leer todos los criterios y exportar un solo excel con las columnas DEPTO, MUPIO, HOGAR, CP, CAPITULO, SECCION
    def process_to_export(self, fecha_inicio: datetime, fecha_final: datetime):
        try:
            # Calcular el total de condiciones
            total_conditions = self.expresiones.shape[0]
            
            # Crear carpeta para guardar los archivos de inconsistencias generales y guardar el log de errores
            marca_temp = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
            carpeta_padre = f"Mariajose/Validaciones_{marca_temp}"

            if not os.path.exists("Inconsistencias"):
                os.mkdir("Inconsistencias")

            self.ruta_carpeta_padre = f"Mariajose/Validaciones_{marca_temp}"
            if not os.path.exists(self.ruta_carpeta_padre):
                os.mkdir(self.ruta_carpeta_padre)

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

            # Unir esto al csv acumulado
            dfs.append(final14)
            self.df_ = dfs
            df_power = pd.concat(dfs) # Hacer copia de los dfs para exportar por supervisor luego
            df_power.to_csv(os.path.join(carpeta_padre, f'InconsistenciasPowerBi_{dia}-{mes}-{año}.csv'), index=False)
            reporte_codigo = df_power.groupby(["CODIGO ERROR", "DEFINICION DE INCONSISTENCIA"]).size().reset_index(name="FRECUENCIA")
            reporte_encuestador = df_power.groupby(["ENCUESTADOR"]).size().reset_index(name="FRECUENCIA")
            reporte_codigo.to_excel(os.path.join(carpeta_padre, f'Frecuencias_por_codigo_{dia}-{mes}-{año}.xlsx'), index=False)
            reporte_encuestador.to_excel(os.path.join(carpeta_padre, f'Frecuencias_por_encuestador_{dia}-{mes}-{año}.xlsx'), index=False)

            for upm, sectors in self.dic_upms.items():
                # Filtra las filas donde la columna "SECTOR" está en los valores de la UPM actual
                filtered_df = df_power[df_power["SECTOR"].isin(sectors)]

                # Exporta el DataFrame filtrado a un archivo Excel
                filtered_df.to_excel(os.path.join(carpeta_padre, f'Inconsistencias{upm}_{dia}-{mes}-{año}.xlsx'), index=False)

            # Cerrar la barra de progreso
            pbar.close()
            

        except Exception as e:
            # Manejar error general en caso de problemas durante el proceso
            logging.error(f"Error general: {e}")


    def validar_encuesta(self, fecha_inicio: datetime, fecha_final: datetime):
        # Procesar datos para validar con validaciones originales
        self.process_to_export(fecha_inicio, fecha_final)
        # Ejecutar el scrip de Mario
        robjects.r.source("InconsistenciasOP.R")

        # Obtener la ruta a la carpeta más reciente
        ruta_externa = self.obtener_carpeta_mas_reciente("Mario")

        # Concatenar los Exceles para generar la salida a reportar
        self.concatenar_exceles(self.ruta_carpeta_padre, ruta_externa, "Salidas_Finales")

    def concatenar_exceles(self, folder1, folder2, output_folder):
        # Crear el directorio de salida si no existe
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Fecha actual
        now = datetime.now()
        date_str = now.strftime("%d-%m-%H-%M-%S")

        # Buscar todos los archivos Excel en folder1
        folder1_files = glob.glob(f"{folder1}/InconsistenciasGRUPO*.xlsx")

        # Crear carpeta de salidas finales dentro de la carpeta de salidas
        self.ruta_salida_final = f"{output_folder}/Salidas{date_str}"
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
                output_file = f"{self.ruta_salida_final}/InconsistenciasGRUPO{group_number}_{date_str}.xlsx"
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
                output_file = f"{self.ruta_salida_final}/InconsistenciasGRUPO{group_number}_{date_str}.xlsx"
                df_concatenated.sort_values(by=["DEPTO", "CODIGO ERROR"]).to_excel(output_file, index=False)

    # Método para generar archivos para limpieza de datos


    def archivos_limpieza(self, fecha_inicio: datetime="2023-1-1", fecha_final: datetime="2023-12-31"):
        try:
                # Calcular el total de condiciones
                total_conditions = self.criterios_limpieza.shape[0]
                self.criterios_limpieza["VARIABLES A EXPORTAR"] = self.criterios_limpieza["VARIABLES A EXPORTAR"].str.replace(r'\s*,\s*', ',').str.split(r'\s+|,')
                # Crear carpeta para guardar los archivos de inconsistencias generales y guardar el log de errores
                marca_temp = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")
                carpeta_padre = f"Limpieza/DatosLimpieza{marca_temp}"

                if not os.path.exists("Limpieza"):
                    os.mkdir("Limpieza")

                self.ruta_carpeta_padre = f"Limpieza/DatosLimpieza{marca_temp}"

                if not os.path.exists(self.ruta_carpeta_padre):
                    os.mkdir(self.ruta_carpeta_padre)

                # Configurar logging
                self.__configurar_logs(carpeta_padre)
                logging.info("Inicio del proceso de exportación de inconsistencias")
                logging.info("Se encontraron {} condiciones.".format(total_conditions))

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
                        for i in Validacion.columns:
                            Validacion.rename(columns={i: i.lower()}, inplace=True)
                        Validacion["Queries"] = None
                        if Validacion.shape[0] == 0:
                            continue 
                        Validacion.to_excel(os.path.join(carpeta_padre, f'{nombre}.xlsx'), index=False)
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
    def limpieza_por_query(self, nombre, condicion: str, columnas: list, fecha_inicio: datetime= "2023-1-1", fecha_final: datetime = "2023-12-31" ):
        Validacion = self.filtrar_base_limpieza(condicion, columnas, fecha_inicio, fecha_final)
        marca_temp = datetime.now().strftime("%d-%m-%Y")
        carpeta_padre = f"Limpieza/DatosLimpieza{marca_temp}"

        if not os.path.exists("Limpieza"):
            os.mkdir("Limpieza")

        self.ruta_carpeta_padre = f"Limpieza/DatosLimpieza{marca_temp}"

        if not os.path.exists(self.ruta_carpeta_padre):
            os.mkdir(self.ruta_carpeta_padre)

        # Configurar logging
        self.__configurar_logs(carpeta_padre)
        for i in Validacion.columns:
            Validacion.rename(columns={i: i.lower()}, inplace=True)
        Validacion["Queries"] = None
        Validacion.to_excel(os.path.join(carpeta_padre, f'{nombre}.xlsx'), index=False)

    def subir_a_drive(self, ruta):
        dia = datetime.now().day
        mes = datetime.now().month
        año = datetime.now().year

        SCOPES = ['https://www.googleapis.com/auth/drive']

        ruta_archivos_exportar = self.obtener_carpeta_mas_reciente("Salidas_Finales")
        # Autenticación
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file("CredencialesCompartido.json", SCOPES)
            creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('drive', 'v3', credentials=creds)

        # Función para subir un archivo a una carpeta específica
        def upload_to_folder(folder_id, filename):
            df = pd.read_excel(filename)
            if len(df) <= 1:  # Si tiene encabezado pero no registros
                match = re.search(r'GRUPO(\d+)', filename)
                if match:
                    group_number = int(match.group(1))
                    print(f"Archivo vacío en el grupo {group_number}.")
                return  # No continuamos con la subida

            media = MediaFileUpload(filename)
            request = service.files().create(media_body=media, body={
                'name': os.path.basename(filename),
                'parents': [folder_id]
            })

            try:
                file = request.execute()
                print(f'Archivo {filename} subido correctamente con ID: {file.get("id")}')
            except HttpError as error:
                print(f'Ha ocurrido un error al subir el archivo {filename}: {str(error)}')

        # Lista de archivos sin ordenar
        files = [os.path.join(ruta, f) for f in os.listdir(ruta) if os.path.isfile(os.path.join(ruta, f))]

        # Ordenar la lista de archivos
        files = [f for f in os.listdir(ruta) if os.path.isfile(os.path.join(ruta, f))]

        # Creamos un diccionario para almacenar archivos según su número de grupo
        files_dict = {}
        for f in files:
            match = re.search(r'GRUPO(\d+)', f)
            if match:
                group_number = int(match.group(1))
                files_dict[group_number] = f

        folder_ids = ["1PVrC64OpF4lLUTn7GB78NDk4tVHKt_9p","1BCUo3eRu4keGA1BxRaDQz7MpD-9Kybam","124JJ2lqUViTPccSSDmX5BaCnwSh0VTTL","1SaOktnsV36R_zUB-_i3LBTmZVhwcVsps",
                    "1cL7VHAHUwsRymHoRS35uqfkSAY443yG_","1uKxCkBWQUrhsYi3TPJvDsGgOpbjPG7iS","19mX4lM2KpJH4BtxOgux9C3OcmyfBbTPX",
                    "1JoxWGL90fxwvIUXOJNs7X6X4qJwt2xML","1cUSFdi2Iy1_8NG7MSqwLThkjJqhZG3Lh","1r8v7-7QuD7I7M31cPimxM6UvKRuHWIue",
                    "1MlXNuRpvZ-uZVToix0rA8_COvIV_sz41",
                    "190oc3i_n1SdB1WEBwDTjs7kKr-NGftou","1FZmN3LKUnCEDKJBeRAa8ass5DYjmQeKH","1Pe-P-RYDR8LaKZxdJmXAerhOe4JcJLVf",
                    "1QtWFQfRHUSn7F3cqJOkEJtIERl7Sfe-N","1oqRunCgEdN9EAmaUrwwXck18GjQL9hSW","1c_t2TkrO6iXo583ePsHDGNco7dNZ1-3v",
                    "1I6mmP-kx456tSofwOg5YG5HGE93EGams","1KtGdixpy-p8JEj45NfoEb8JtZ29xYpFS","1kQVlHOIBO-p1bmsk-rgxCVjG6P_V2TGr",
                    "1my1uG69TEZba7xSLbdaHphYyb9lOLFtg","1K921NLXIqY5oRyxxivqdL5D7CIFwPjm-","1CnfCG1zkXcHGjvS3GeHMb13_tzOooWn4",
                    "1Sm-YBHKldQ8epDdqDUVfGCo0lQ1v0sCp","1yO0qyfXWg9Y0lVXWGWB4lTIeYkIGtY9I","11z9lGtsWFWgotd05xK-0VHHd-kVYNAGV",
                    "1m92ZurDG-J4_aqPnCub88HsYnWtaEaUI","1-sKuqlI8uTdgJ20PsF0_OwPAF84ZxDPH",
                    "1OKlNUxasy5eXpmqLJIZCXK5PoOoMoGVC","1b4Ya2RjwkmymD0IhI81Lq36KhTh5aTXw","11KOYySNg6CCDNEA_8UawUuU45wplZbO5",
                    "1uWIq6hM3BNjtXOFQ2g4ifwufZGBDvHUf","1vSyw6zjJ3iMBNealEETEaOvmFKfo6OA0","15NVXVv5LFQ7vs-d-YmVptUBnx3ZxrxKx",
                    "1qB5f-R2XEyfiEwpJH_9U3gTQfJRUwf5N","1NmLZNNgnZA3jaXx4td0tITiPaMlvD_3m","1DvvWSgpLFmLnpH4Yv074gPqebKjyaBo3",
                    "1FRz1FK4ogxvzcFSQUjIiMaBPjUqH7lvA","1IdZdQ6Y8ExDs4XhdQmDJjnyp1JrnrRFW"]

        # Recorremos la lista de IDs de carpeta
        for i, folder_id in enumerate(folder_ids):
            # Buscamos si hay un archivo que corresponde al número de grupo actual (i + 1)
            filename = files_dict.get(i + 1)
            
            if filename:  # Si hay un archivo, lo subimos
                upload_to_folder(folder_id, os.path.join(ruta, filename))
            else:
                print(f"No se encontró archivo para el grupo {i + 1}. Pasando al siguiente grupo.")
