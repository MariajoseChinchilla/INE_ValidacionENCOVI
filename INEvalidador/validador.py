from datetime import datetime
from tqdm import tqdm
import pandas as pd
import numpy as np
import logging
import re
import os

class Validador:
    def __init__(self, ruta_base: str="BD_PERSONAS_PILOTO.sav", ruta_expresiones: str="Expresiones.xlsx"):
        self.df = pd.read_spss(ruta_base, convert_categoricals=False)
        self.df = self.df[self.df["PPA10"] == 1]
        self.expresiones = pd.read_excel(ruta_expresiones)
        self.columnas = ["DEPTO", "MUPIO","SECTOR","ESTRUCTURA","VIVIENDA","HOGAR", "CP"]

    def leer_condicion(self, condition: str) -> str: 
        # Para las columnas de texto, busca patrones del tipo 'variable = (vacío)' o 'variable no es (vacío)'
        text_var_pattern = r'(\w+)\s*(==|!=)\s*\((vacío|vacio)\)'
        text_var_matches = re.findall(text_var_pattern, condition)

        for var, op in text_var_matches:
            if op == '==':
                condition = condition.replace(f'{var} {op} (vacío)', f'{var} == ""')
                condition = condition.replace(f'{var} {op} (vacio)', f'{var} == ""')
            elif op == '!=':
                condition = condition.replace(f'{var} {op} (vacío)', f'{var} != ""')
                condition = condition.replace(f'{var} {op} (vacio)', f'{var} != ""')

        # Reemplaza los símbolos y frases con su equivalente en Python
        condition = condition.replace("<> ""","no es vacio")
        condition = condition.replace("NO ESTA EN","not in").replace('<=', '<=').replace("VACIO", "vacío").replace("VACÍO", "vacío").replace("ó","o").replace("Ó","o").replace("vacio", "vacío")
        condition = condition.replace("NO","no").replace('=', '==').replace('<>', '!=').replace(">==", ">=").replace("<==","<=").replace("Y", "y")
        condition = condition.replace(' y ', ' & ').replace(' o ', '|').replace('NO ESTA EN', 'not in').replace('no está en', 'not in').replace("no esta en","not in")
        condition = condition.replace('ESTA EN', 'in').replace('está en', 'in').replace("no es vacio", "no es vacío").replace("\n"," ").replace("\r","").replace("esta en","in")
        condition = condition.replace("no  es vacio","no es vacío").replace("no es  vacio","no es vacío").replace("no  es vacío","no es vacío")
        condition = condition.replace("no es  vacío","no es vacío").replace("no es  (vacio)","no es vacío").replace("no es  (vacío)","no es vacío")
        condition = condition.replace("no  es (vacío)","no es vacío").replace("no  es (vacio)","no es vacío").replace("ES","es")
        condition = condition.replace("no esta  en","not in").replace("no  esta en","not in").replace("no está  en","not in").replace("no  está en","not in")

        # Para las demás columnas, asume que son numéricas y reemplaza 'no es (vacío)' por '!= None' y 'es (vacío)' por '== None'
        condition = condition.replace('no es (vacío)', '!= None')
        condition = condition.replace('no es vacío', '!= None')
        condition = condition.replace('es (vacío)', '== None')
        condition = condition.replace('es vacío', '== None')


        condition = condition.replace("NA", 'None')

        # Reemplaza las comparaciones entre variables para que sean legibles en Python
        condition = re.sub(r'(\w+)\s*(<=|>=|<|>|==|!=)\s*(\w+)', r'\1 \2 \3', condition)

        # Si "está en" se encuentra en la condición, lo reemplaza por la sintaxis correcta en Python
        if "está en" in condition:
            condition = re.sub(r'(\w+)\s+está en\s+(\(.*?\))', r'\1 in \2', condition)
        
        # Agrega paréntesis alrededor de la condición
        condition = '(' + condition + ')'
        return condition

    # Función para filtrar base de datos dada una query
    def filter_base(self, conditions: str, columnas: list) -> pd.DataFrame:
        # Descomponemos la condición
        conditions = conditions.split('&')

        # Iniciamos con todos los datos
        df_filtered = self.df

        # Iteramos sobre las condiciones
        for condition in conditions:
            condition = condition.strip()  # Eliminamos los espacios en blanco alrededor
            if 'no es (vacío)' in condition:
                # Si la condición es 'no es (vacío)', usamos pd.notna()
                col_name = condition.replace('no es (vacío)', '').strip()
                df_filtered = df_filtered[pd.notna(df_filtered[col_name])]
            else:
                # Para todas las demás condiciones, utilizamos eval()
                df_filtered = df_filtered[df_filtered.eval(self.leer_condicion(condition))]

        return df_filtered[columnas]


    # Función para leer todos los criterios y exportar una carpeta por capítulo y un excel por sección 
    def process_general_data(self,columnas):
        try:
            grouped = self.expresiones.groupby(["Capítulo", "Sección"])

            # Crear lista con expresiones para filtrar
            tuplas_chap_sec = [(name[0], name[1]) for name, _ in grouped]

            # Calcular el total de condiciones
            total_conditions = self.expresiones.shape[0]
            
            # Crear carpeta para guardar los archivos de inconsistencias generales y guardar el log de errores
            marca_temp = datetime.now().strftime("%Y%m%d%H%M%S")
            carpeta_padre = f"Inconsistencias_{marca_temp}"
            if not os.path.exists(carpeta_padre):
                os.mkdir(carpeta_padre)
            
            # Configurar logging
            logging.basicConfig(
                filename=os.path.join(carpeta_padre, f'app{marca_temp}.log'),
                filemode='w',
                format='%(levelname)s - %(message)s',
                level=logging.DEBUG
            )
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
                        Validacion = self.filter_base(condition,columnas)
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
            logging.basicConfig(
                filename=os.path.join(carpeta_padre, f'app{marca_temp}.log'),
                filemode='w',
                format='%(levelname)s - %(message)s',
                level=logging.DEBUG
            )
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
            df_exportacion.to_excel(os.path.join(carpeta_padre, "Inconsistencias.xlsx"),index=False)
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
