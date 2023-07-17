from datetime import datetime
from tqdm import tqdm
import pandas as pd
import numpy as np
import logging
import re
import os

class Validador:
    def __init__(self, ruta_base: str="BasePrueba.sav", ruta_expresiones: str="Expresiones.xlsx"):
        self.df = pd.read_spss(ruta_base)
        self.expresiones = pd.read_excel(ruta_expresiones)

    def leer_condicion(self, condition): # agregar tipos de datos
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
        condition = condition.replace('<=', '<=').replace("VACIO", "vacío").replace("VACÍO", "vacío")
        condition = condition.replace('=', '==').replace('<>', '!=').replace(">==", ">=").replace("<==","<=").replace("Y", "y")
        condition = condition.replace(' y ', ' & ').replace(' o ', ' | ').replace('NO ESTA EN', 'not in').replace('no está en', 'not in')
        condition = condition.replace('ESTA EN', 'in').replace('está en', 'in')

        # Para las demás columnas, asume que son numéricas y reemplaza 'no es (vacío)' por '!= np.nan' y 'es (vacío)' por '== np.nan'
        condition = condition.replace(' no es (vacío)', '!="NaN"')
        condition = condition.replace(' no es vacío', '!="NaN"')
        condition = condition.replace(' es (vacío)', '=="NaN"')
        condition = condition.replace(' es vacío', '=="NaN"')

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
    def filter_base(self, conditions):
        filter = self.leer_condicion(conditions)
        df_filtered = self.df[self.df.eval(filter)]
        return df_filtered

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

    def process_general_data(self):
        try:
            grouped = self.expresiones.groupby(["Capítulo", "Sección"])

            # Crear lista con expresiones para filtrar
            tuplas_chap_sec = [(name[0], name[1]) for name, _ in grouped]

            # Crear carpeta para guardar los archivos de inconsistencias generales y guardar el log de errores
            marca_temp = datetime.now().strftime("%Y%m%d%H%M%S")
            carpeta_padre = f"Inconsistencias_{marca_temp}"
            if not os.path.exists(carpeta_padre):
                os.mkdir(carpeta_padre)
            # Configurar logging
            logging.basicConfig(
                filename=os.path.join(carpeta_padre, f'app{marca_temp}.log'),
                filemode='w',
                format='%(name)s - %(levelname)s - %(message)s',
                level=logging.DEBUG
            )
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
                        Validacion = self.filter_base(condition)  # Aplicar filtro a la base de datos
                        sheet_name = "S{}V{}".format(seccion, conditions.index(condition))  # Generar el nombre de la hoja
                        filename = os.path.join(ruta_carpeta, "S{}.xlsx".format(seccion))  # Crea la ruta completa al archivo
                        Validacion.to_excel(filename, sheet_name=sheet_name)  # Exportar subconjunto de datos a una hoja de Excel
                    except Exception as e:
                        logging.error(f"Error al procesar la expresión {condition}: {e}")  # Manejar error específico de una expresión
                        pass

        except Exception as e:
            logging.error(f"Error general: {e}")  # Manejar error general en caso de problemas durante el proceso
