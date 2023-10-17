import os
from datetime import datetime
import re 
import copy
import logging
from tqdm import tqdm

import unicodedata
import pandas as pd
import numpy as np

from .utils import extraer_UPMS, columnas_a_mayuscula, condicion_a_variables
from INEvalidador.conexionSQL import baseSQL

class Limpieza:
    def __init__(self, comision, ruta_criterios_limpieza: str="", descargar: bool = False, host: str = '10.0.0.170', 
                puerto: str = '3307', usuario: str = 'mchinchilla', 
                password: str = 'Mchinchilla2023'):
        self.ruta_escritorio = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        self.marca_temp = datetime.now().strftime("%d-%m-%Y")
        self.sql = baseSQL(descargar, host, puerto, usuario, password, comision)
        self.comision = comision

        self.salida_principal = os.path.join(self.ruta_escritorio, f"Validador\Datos para Revisión\output_{self.marca_temp}")
        if not os.path.exists(self.salida_principal):
            os.makedirs(self.salida_principal)
        if ruta_criterios_limpieza:
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
        self.df = self.df_para_limpieza(condicion)
        self.df["VARIABLE"] = None
        self.df["VALOR NUEVO"] = None  
        filtered_df = self.df.query(self.leer_condicion(condicion))[["DEPTO","MUPIO","SECTOR","ESTRUCTURA","VIVIENDA","HOGAR","CP"] + columnas + ["VARIABLE", "VALOR NUEVO"]]
        return copy.deepcopy(filtered_df)

    # Busca el archivo limpieza en carpeta de archivos
    def archivos_limpieza(self, fecha_inicio: datetime="2023-1-1", fecha_final: datetime="2100-12-31"):
        try:
            # Calcular el total de condiciones
            total_conditions = self.criterios_limpieza.shape[0]
            self.criterios_limpieza["VARIABLES A EXPORTAR"] = self.criterios_limpieza["VARIABLES A EXPORTAR"].str.replace(r'\s*,\s*', ',').str.split(',').apply(lambda x: [i.strip() for i in x if i.strip()])
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
        # marca_temp = datetime.now().strftime("%d-%m-%Y")
        # carpeta_padre = f"Limpieza/DatosLimpieza{marca_temp}"

        # Configurar logging
        self.__configurar_logs(self.salida_principal)
        for i in Validacion.columns:
            Validacion.rename(columns={i: i.lower()}, inplace=True)
        
        Validacion.to_excel(os.path.join(self.salida_principal, f'{nombre}.xlsx'), index=False)

    def df_para_limpieza(self, condicion, fecha_inicio: datetime="2023-1-1", fecha_final: datetime="2100-12-31"):

        variables = condicion_a_variables(condicion)
        df_a_unir = list(set([self.sql.base_col.get(var) for var in variables]))
        tipos = []
        for i in range(len(df_a_unir)):
            try:
                if isinstance(df_a_unir[i], str) and len(df_a_unir[i]) >= 2:
                    tipo = df_a_unir[i][-2:] # devuelve SR o PR
                    tipos.append(tipo)
            except Exception as e:
                print(f"Error con el ítem {i}: {df_a_unir[i]}. Detalle del error: {e}")
        tipos = list(set(tipos))
        tipo = tipos[0] if tipos else None

        
        df_a_unir = [self.sql.base_df.get(archivo) for archivo in df_a_unir] 

        df_base = self.sql.base_df.get(f'level-1_{tipo}')
        for df in df_a_unir:
            if "INDEX" in df.columns:
                df = df.drop('INDEX', axis=1)
            df_base = pd.merge(df_base, df, on='LEVEL-1-ID', how='inner')

        df_cases = self.sql.base_df.get(f'cases_{tipo}')
        df_base = pd.merge(df_base, df_cases[["DELETED", "ID"]], left_on='CASE-ID', right_on='ID', how='inner')
        df_base = df_base.query('DELETED == 0')

        # Si tipo es "PR", agregamos el dataframe "caratula_PR.feather"
        if len(tipos) == 1 and tipos[0] == 'PR':
            # Agregar dataframe con la caratula
            caratula_pr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), 'caratula_PR.feather'))
            caratula_pr_df = columnas_a_mayuscula(caratula_pr_df)
            # Agregar dataframe con las fechas
            tiempo_pr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), "tiempo_control_PR.feather"))
            tiempo_pr_df = columnas_a_mayuscula(tiempo_pr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_pr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX",axis=1)
            df_base = pd.merge(df_base, caratula_pr_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'
        
        # Si tipo es "SR", agregamos el dataframe "estado_de_boleta_SR.feather"
        elif len(tipos) == 1 and tipos[0] == 'SR':
            # Agregar dataframe estado boleta
            estado_boleta_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), 'estado_de_boleta_SR.feather'))
            estado_boleta_df = columnas_a_mayuscula(estado_boleta_df)
            # Agregar dataframe de control de tiempo
            tiempo_sr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision),  "control_tiempo_SR.feather"))
            tiempo_sr_df = columnas_a_mayuscula(tiempo_sr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_sr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX",axis=1)
            df_base = pd.merge(df_base, estado_boleta_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'

        # Si es validacion entre rondas, agregar tablas pertinentes
        elif len(tipos) == 2:
            # Agregar dataframe estado boleta
            estado_boleta_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), 'estado_de_boleta_SR.feather'))
            estado_boleta_df = columnas_a_mayuscula(estado_boleta_df)
            # Agregar dataframe de control de tiempo
            tiempo_sr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision), "tiempo_control_PR.feather"))
            tiempo_sr_df = columnas_a_mayuscula(tiempo_sr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_sr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX_x",axis=1)
            df_base = pd.merge(df_base, estado_boleta_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'
            # Agregar dataframe con la caratula
            caratula_pr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza", str(self.comision),  'caratula_PR.feather'))
            caratula_pr_df = columnas_a_mayuscula(caratula_pr_df)
            # Agregar dataframe con las fechas
            tiempo_pr_df = pd.read_feather(os.path.join(self.ruta_escritorio, "Validador", "db_limpieza",str(self.comision), "tiempo_control_PR.feather"))
            tiempo_pr_df = columnas_a_mayuscula(tiempo_pr_df)
            # Unir a base raiz
            df_base = pd.merge(df_base, tiempo_pr_df, on="LEVEL-1-ID", how="inner")
            df_base = df_base.drop("INDEX_y",axis=1)
            df_base = df_base.drop("INDEX_x",axis=1)
            df_base = pd.merge(df_base, caratula_pr_df, on='LEVEL-1-ID', how='inner')  # Unión por 'LEVEL-1-ID'

        # Validar solo las encuestas terminadas
        if "PPA10" in df_base.columns and "ESTADO_PR" in df_base.columns:      
            df_base = df_base[df_base["PPA10"] == 1]
        elif "ESTADO_SR" in df_base.columns:
            df_base = df_base[df_base["ESTADO_SR"] == 1]
        elif "ESTADO_PR" in df_base.columns and "PPA10" not in df_base.columns:
            df_base = df_base[df_base["ESTADO_PR"] == 1]
        # Validar ambas rondas terminadas en caso de validacion entre rondas para persona
        elif "ESTADO_PR" in df_base.columns and "PPA10" in df_base.columns and "ESTADO_SR" in df_base.columns:
            df_base = df_base[(df_base["PPA10"] == 1 and df_base["ESTADO_SR"] == 1)]

        # Agregar código CP = 0 para las validaciones de hogares
        if "CP" not in df_base.columns:
            df_base["CP"] = 0

        # Agregar filtrado por fecha tomando el capítulo 1 como inicio de la encuesta
        if "FECHA_INICIO_CAP_1" in df_base.columns:
            df_base["FECHA_INICIO_CAP_1"] = pd.to_datetime(df_base["FECHA_INICIO_CAP_1"], format="%d/%m/%y")
            df_base = df_base[(df_base["FECHA_INICIO_CAP_1"] >= fecha_inicio) & (df_base["FECHA_INICIO_CAP_1"] <= fecha_final)]
            df_base["FECHA"] = df_base["FECHA_INICIO_CAP_1"]
            
        if "FECHA_INICIO_CAPXIIIA" in df_base.columns:
            df_base["FECHA_INICIO_CAPXIIIA"] = pd.to_datetime(df_base["FECHA_INICIO_CAPXIIIA"], format="%d/%m/%y")
            df_base = df_base[(df_base["FECHA_INICIO_CAPXIIIA"] >= fecha_inicio) & (df_base["FECHA_INICIO_CAPXIIIA"] <= fecha_final)]
            df_base["FECHA"] = df_base["FECHA_INICIO_CAPXIIIA"]
            

        for columna in df_base.columns:
            if columna[-2:] == "_y":
                df_base.drop(columns=columna, inplace=True)
            if columna[-2:] == "_x":
                df_base.rename(columns={columna : columna[0:-2]}, inplace=True)
            if columna[-3:] == "_SR":
                df_base.rename(columns={columna : columna[0:-3]}, inplace=True)
            if columna[-3:] == "_PR":
                df_base.rename(columns={columna : columna[0:-3]}, inplace=True)

        return df_base
    
    def escribir_query_sq(self, archivo, comision, usuario, fecha_inicio:datetime="2023-1-1", fecha_final:datetime="2100-12-31") -> str:
        now = datetime.now()
        date_str = now.strftime("%d-%m-%Y")
        ruta_sintaxis = os.path.join(self.ruta_escritorio, "Validador", "Sintaxis en SQL", f"output{date_str}")
        if not os.path.exists(ruta_sintaxis):
            os.makedirs(ruta_sintaxis)
        nombre = os.path.basename(archivo).split(".")[0]
        ruta_archivo = os.path.join(ruta_sintaxis, f"{nombre}.txt")
        self.ruta_archivo_query = ruta_archivo
        # Tomar el df original subido por el analista y obtener el valor en la llave primaria a editar
        df_original = pd.read_excel(archivo)
        variables_a_editar = list(var.split(".")[1].upper() for var in df_original["variable"])
        deptos = list(df_original["depto"])
        mupios = list(df_original["mupio"])
        sectores = list(df_original["sector"])
        estructuras = list(df_original["estructura"])
        viviendas = list(df_original["vivienda"])
        hogares = list(df_original["hogar"])
        cps = list(df_original["cp"])
        # Hacer de la identificación cartográfica un filtro
        condiciones = list(zip(variables_a_editar, deptos, mupios, sectores, estructuras, viviendas, hogares, cps))
        dfs = []
        filtros = []
        for idx, (var, depto, mupio, sec, estru, vivi, hog, cp) in enumerate(condiciones):
            filtro = f"DEPTO = {depto} & MUPIO = {mupio} & SECTOR = {sec} & ESTRUCTURA = {estru} & VIVIENDA = {vivi} & HOGAR = {hog} & CP = {cp}"
            filtro.replace("& CP = 0","").replace("& CP = nan","")
            llave_primaria = self.sql.base_col.get(var).replace("_PR","").replace("_SR","").upper() + "-ID"
            df_query = self.filtrar_base_limpieza(filtro, [llave_primaria], fecha_inicio, fecha_final)
            filtros.append(filtro)
            # Agregar columnas "variable" y "valor_nuevo" a df_query
            df_query["variable"] = df_original.at[idx, "variable"]
            df_query["valor nuevo"] = df_original.at[idx, "valor nuevo"]
            dfs.append(df_query)
        df_queries = pd.concat(dfs)
        # Escribir sintaxis SQL usando el df_queries para tomar la llave primaria de la tabla a editar
        # Pendiente arreglar qué pasa si se quieren editar variables de diferentes tablas
        
        df_queries = df_queries.dropna(subset=["variable"])
        df_queries = df_queries.dropna(subset=["valor nuevo"])
        vars = list(df_queries["variable"])
        id_columns = [col for col in df_queries.columns if col.endswith('-ID')]
        ids = list(df_queries[id_columns[0]])
        ids_y_vars = list(zip(ids, variables_a_editar))
        valores_antiguos = []
        for i in range(len(df_original) - 1):
            valores_antiguos.append(df_original.loc[i, vars[i]])
        # print(valores_antiguos)

        tablas = []
        for i in vars:
            if isinstance(i, str) and "." in i:
                tablas.append(self.sql.base_col.get(i.split(".", 1)[1].upper()))
        valores_nuevos = list(df_queries["valor nuevo"])
        ronda = [f"ENCOVI_{tablas[0][-2:]}"] * len(tablas)
        comisiones = [comision] * len(tablas)
        vars = [var.split(".", 1)[1] for var in vars]
        tablas = [tabla[:-3] for tabla in tablas]
        cuadruplas = list(zip(ronda, tablas, vars, valores_nuevos, filtros, ids, comisiones, valores_antiguos))
        for id, var in ids_y_vars:
            tabla = self.sql.base_col.get(var).replace("_PR","").replace("_SR","") + "-id"
            filtros.append(f"{tabla} = {id}")
        fecha = datetime.now()
        for rond, tabla, variable, valor_nuevo, filtro, id, comision, valor_viejo in cuadruplas:
            with open(ruta_archivo, "a") as archivo:
                # archivo.write(f"UPDATE {rond}.{tabla} AS {tabla} JOIN `level-1` ON {tabla}.`level-1-id` = `level-1`.`level-1-id` SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")
                # archivo.write(f"INSERT INTO {base_datos}.{tabla (bitácora)} SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")
                archivo.write(f"UPDATE {rond}_COM{comision}.{tabla} AS {tabla}  SET {variable} = {valor_nuevo} WHERE {tabla}.`{tabla}-id` = {id}; \n")
                archivo.write(f"UPDATE ine_encovi.bitacora AS bitacora  SET usuario = {usuario} and base_datos = {rond}_COM{comision} and tabla = {tabla} and variable = {variable} and valor_anterior = {valor_viejo} and valor_nuevo = {valor_nuevo} and id_registro = {id} and fecha_creacion = {fecha}; \n")
        
        return ruta_archivo