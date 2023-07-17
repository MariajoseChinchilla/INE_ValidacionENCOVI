import pandas as pd
import re
import os
import numpy as np

df = pd.read_spss("BasePrueba.sav")
expresiones = pd.read_excel("Expresiones.xlsx")

def leer_condicion(condition):
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
    condition = condition.replace('<=', '<=')
    condition = condition.replace('=', '==').replace('<>', '!=').replace(">==", ">=").replace("<==","<=").replace("Y", "y")
    condition = condition.replace(' y ', ' & ').replace(' o ', ' | ').replace('NO ESTA EN', 'not in')

    # Para las demás columnas, asume que son numéricas y reemplaza 'no es (vacío)' por '!= np.nan' y 'es (vacío)' por '== np.nan'
    condition = condition.replace(' no es (vacío)', '!="NaN"')
    condition = condition.replace(' no es vacío', '!="NaN"')
    condition = condition.replace(' es (vacío)', '=="NaN"')
    condition = condition.replace(' es vacío', '=="NaN"')

    condition = condition.replace("NA", 'np.nan')

    # Reemplaza las comparaciones entre variables para que sean legibles en Python
    condition = re.sub(r'(\w+)\s*(<=|>=|<|>|==|!=)\s*(\w+)', r'\1 \2 \3', condition)

    # Si "está en" se encuentra en la condición, lo reemplaza por la sintaxis correcta en Python
    if "está en" in condition:
        condition = re.sub(r'(\w+)\s+está en\s+(\(.*?\))', r'\1 in \2', condition)
    
    # Agrega paréntesis alrededor de la condición
    condition = '(' + condition + ')'
    return condition

# Función para filtrar base de datos dada una query
def filter_base(conditions):
    global df
    filter = leer_condicion(conditions)
    df_filtered = df.query(filter, local_dict={'np': np})
    return df_filtered

# Función para devolver inconsistencias dado un analista, capitulo, seccion en especifico
def process_specific_data(capitulo, seccion, analista):
    global expresiones
    try:        
        # Crear lista con expresiones para filtrar
        expressions = list(expresiones[(expresiones["Analista"] == analista) & (expresiones["Capítulo"] == capitulo) & (expresiones["Sección"] == seccion)]["Condición o Criterio"])
        
        # Crear archivo tipo ExcelWriter para exportar en diferentes pestañas
        writer = pd.ExcelWriter("C{}S{}.xlsx".format(capitulo,seccion))
        
        # Leer filtros y tomar subconjuntos de la base
        for i in range(len(expressions)):
            try:
                Validacion = filter_base(expressions[i])  # Aplicar filtro a la base de datos
                sheet_name = "S{}V{}".format(capitulo, seccion, i)  # Generar el nombre de la hoja
                Validacion.to_excel(writer, sheet_name=sheet_name)  # Exportar subconjunto de datos a una hoja de Excel
            except Exception as e:
                print(f"Error al procesar la expresión {expressions[i]}: {e}")  # Manejar error específico de una expresión

        writer.save()  # Guardar el archivo de Excel con las hojas generadas
        print("Proceso completado exitosamente.")  # Indicar que el proceso ha finalizado con éxito
    
    except Exception as e:
        print(f"Error general: {e}")  # Manejar error general en caso de problemas durante el proceso


# Funcion de lectura de expresiones generales
def process_general_data():
    global df, expresiones
    try:
        grouped = expresiones.groupby(["Capítulo", "Sección"])

        # Crear lista con expresiones para filtrar
        tuplas_chap_sec = [(name[0], name[1]) for name, _ in grouped]

        # Leer filtros y tomar subconjuntos de la base
        for capitulo, seccion in tuplas_chap_sec:
            # Crear carpeta por capitulo
            folder_name = "C{}".format(capitulo)
            ruta_carpeta = os.path.join(os.getcwd(), folder_name)
            if not os.path.exists(ruta_carpeta):
                os.mkdir(ruta_carpeta)
            conditions = list(expresiones[(expresiones["Capítulo"] == capitulo) & (expresiones["Sección"] == seccion)]["Condición o Criterio"])
            for condition in conditions:
                try:
                    Validacion = filter_base(condition)  # Aplicar filtro a la base de datos
                    sheet_name = "S{}V{}".format(seccion, conditions.index(condition))  # Generar el nombre de la hoja
                    filename = os.path.join(ruta_carpeta, "S{}.xlsx".format(seccion))  # Crea la ruta completa al archivo
                    Validacion.to_excel(filename, sheet_name=sheet_name)  # Exportar subconjunto de datos a una hoja de Excel
                except Exception as e:
                    print(f"Error al procesar la expresión {condition}: {e}")  # Manejar error específico de una expresión

    except Exception as e:
        print(f"Error general: {e}")  # Manejar error general en caso de problemas durante el proceso

#leer_condicion("PPA03 < 7 y P09A01A es (vacío) | P09A01B es (vacío) |  P09A01C es (vacío) | P09A02A es (vacío) | P09A02B es (vacío) | P09A02C es vacío | P09A03A es vacío | P09A03B es (vacío) |  P09A03C es (vacío) | P09A04A es (vacío) | P09A04B es (vacío) | P09A04C es (vacío) | P09A05A es (vacío) | P09A05B es (vacío) | P09A05C es (vacío)")
#filter_base("PPA03 < 7 y P09A01A es (vacío) | P09A01B es (vacío) |  P09A01C es (vacío) | P09A02A es (vacío) | P09A02B es (vacío) | P09A02C es vacío | P09A03A es vacío | P09A03B es (vacío) |  P09A03C es (vacío) | P09A04A es (vacío) | P09A04B es (vacío) | P09A04C es (vacío) | P09A05A es (vacío) | P09A05B es (vacío) | P09A05C es (vacío)")
#process_general_data()

filter_base('(P05D02 = 1) & (P05D03A <>NA )')