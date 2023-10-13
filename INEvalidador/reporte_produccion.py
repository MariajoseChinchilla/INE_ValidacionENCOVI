from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import pkg_resources
import os
from datetime import datetime
import time
import matplotlib.pyplot as plt
from io import StringIO
import io
import pandas as pd
from datetime import datetime
from INEvalidador.conexionSQL import baseSQL
from INEvalidador.limpieza import Limpieza

class GestorConteos:
    def __init__(self):
        self.sql = baseSQL(False)
        self.limpieza = Limpieza()
        self.ruta_escritorio = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
        self.ruta_limpieza = os.path.join(self.ruta_escritorio, "Limpieza")
        if not os.path.exists(self.ruta_limpieza):
            os.mkdir(self.ruta_limpieza)
        # Autenticación con Google Drive
        gauth = GoogleAuth()
        gauth.LoadClientConfigFile(pkg_resources.resource_filename(__name__, "archivos\CredencialesCompartido.json"))
        gauth.LocalWebserverAuth()
        self.drive = GoogleDrive(gauth)
        self.FOLDER_ID = '1xC2oCDbSiVCRVeilCK7SLyRhNCdQ8yaY'
        
    def inicializar_archivo(self):
        """Crea y sube un archivo CSV inicial para los conteos de analistas a Google Drive."""
        ruta_conteo_analista = os.path.join(self.ruta_limpieza, 'conteo_analistas.csv')
        with open(ruta_conteo_analista, 'w') as file:
            file.write("Analista,Conteo\n")  # Cabecera del CSV
        file_drive = self.drive.CreateFile({'title': 'conteo_analistas.csv', 'parents': [{'id': self.FOLDER_ID}]})
        file_drive.SetContentFile(ruta_conteo_analista)
        file_drive.Upload()

    def obtener_conteo_desde_archivo(self, analista):
        """Devuelve el conteo de líneas desde el archivo del analista del día actual."""
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        ruta_archivo = os.path.join(self.ruta_limpieza, f"{analista}_{fecha_actual}\{analista}.txt")
        os.mkdir(os.path.join(self.ruta_limpieza, f"{analista}_{fecha_actual}"))
        if not os.path.exists(ruta_archivo):
            return 0
        with open(ruta_archivo, 'r') as file:
            return sum(1 for _ in file)

    def actualizar_conteo(self, analista):
        """Actualiza el conteo del analista en el archivo CSV en Google Drive."""
        lineas = self.obtener_conteo_desde_archivo(analista)
        file_list = self.drive.ListFile({'q': f"'{self.FOLDER_ID}' in parents"}).GetList()
        file_data = [f for f in file_list if f['title'] == 'conteo_analistas.csv'][0]
        content = file_data.GetContentString()
        lines = content.split("\n")

        updated = False
        for i, line in enumerate(lines[1:]):
            if line.startswith(analista + ','):
                current_count = int(line.split(',')[1])
                lines[i + 1] = f"{analista},{current_count + lineas}"
                updated = True
                break

        if not updated:
            lines.append(f"{analista},{lineas}")

        new_content = "\n".join(lines)
        file_data.SetContentString(new_content)
        file_data.Upload()

    def create_lockfile(self):
        """Crea un archivo de bloqueo en Google Drive. Devuelve False si ya existe."""
        lock_files = [f for f in self.drive.ListFile({'q': f"'{self.FOLDER_ID}' in parents"}).GetList() if f['title'] == 'lockfile.txt']
        if lock_files:
            return False
        lock_file = self.drive.CreateFile({'title': 'lockfile.txt', 'parents': [{'id': self.FOLDER_ID}]})
        lock_file.Upload()
        return True

    def remove_lockfile(self):
        """Elimina el archivo de bloqueo de Google Drive si existe."""
        lock_files = [f for f in self.drive.ListFile({'q': f"'{self.FOLDER_ID}' in parents"}).GetList() if f['title'] == 'lockfile.txt']
        if lock_files:
            lock_files[0].Trash()

    def actualizar_conteo_con_bloqueo(self, analista):
        """Actualiza el conteo del analista en el archivo CSV en Drive, utilizando bloqueo."""
        while not self.create_lockfile():
            time.sleep(10)
        try:
            self.actualizar_conteo(analista)
        finally:
            self.remove_lockfile()

    def generar_histograma(self):
        """Genera un histograma con la productividad de cada analista y lo guarda en el escritorio."""
        # Obtener datos del archivo en Google Drive
        file_list = self.drive.ListFile({'q': f"'{self.FOLDER_ID}' in parents"}).GetList()
        file_data = [f for f in file_list if f['title'] == 'conteo_analistas.xlsx'][0]
        
        # Descargar el archivo temporalmente
        temp_file_path = "temp_conteo_analistas.xlsx"
        file_data.GetContentFile(temp_file_path)
        
        # Leer el archivo Excel con pandas
        df = pd.read_excel(temp_file_path)
        
        os.remove(temp_file_path)  # Eliminar el archivo temporal
        
        analistas = df['Analista'].tolist()
        conteos = df['Conteo'].tolist()

        # Crear el histograma
        plt.figure(figsize=(10, 6))
        colores = plt.cm.Paired(range(len(analistas)))
        plt.bar(analistas, conteos, color=colores)
        plt.title("Productividad por Analista")
        plt.xlabel("Analistas")
        plt.ylabel("Líneas generadas")
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Guardar la imagen
        fecha_actual = datetime.now().strftime('%d-%m-%Y')
        ruta_carpeta = os.path.join(self.ruta_limpieza, "ReporteProductividad")
        if not os.path.exists(ruta_carpeta):
            os.makedirs(ruta_carpeta)
        ruta_imagen = os.path.join(ruta_carpeta, f"Productividad_{fecha_actual}.png")
        plt.savefig(ruta_imagen)

        # Mostrar el histograma (opcional)
        plt.show()

    def escribir_query_sq(self, archivo, nombre, comision, fecha_inicio:datetime="2023-1-1", fecha_final:datetime="2023-12-31"):
        now = datetime.now()
        date_str = now.strftime("%d-%m-%Y")
        ruta_sintaxis = os.path.join(self.ruta_limpieza, "Sintaxis en SQL", f"output{date_str}")
        if not os.path.exists(ruta_sintaxis):
            os.makedirs(ruta_sintaxis)
        ruta_archivo = os.path.join(ruta_sintaxis, f"Sintaxis_{nombre}.txt")
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
            df_query = self.limpieza.filtrar_base_limpieza(filtro, [llave_primaria], fecha_inicio, fecha_final)
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
        

        tablas = []
        for i in vars:
            if isinstance(i, str) and "." in i:
                tablas.append(self.sql.base_col.get(i.split(".", 1)[1].upper()))
        valores_nuevos = list(df_queries["valor nuevo"])
        ronda = [f"ENCOVI_{tablas[0][-2:]}"] * len(tablas)
        comisiones = [comision] * len(tablas)
        vars = [var.split(".", 1)[1] for var in vars]
        tablas = [tabla[:-3] for tabla in tablas]
        cuadruplas = list(zip(ronda, tablas, vars, valores_nuevos, filtros, ids, comisiones))
        for id, var in ids_y_vars:
            tabla = self.sql.base_col.get(var).replace("_PR","").replace("_SR","") + "-id"
            filtros.append(f"{tabla} = {id}")
        fecha = datetime.now()
        for rond, tabla, variable, valor_nuevo, filtro, id, comision in cuadruplas:
            with open(ruta_archivo, "a") as archivo:
                # archivo.write(f"UPDATE {rond}.{tabla} AS {tabla} JOIN `level-1` ON {tabla}.`level-1-id` = `level-1`.`level-1-id` SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")
                # archivo.write(f"INSERT INTO {base_datos}.{tabla (bitácora)} SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")
                archivo.write(f"UPDATE {rond}_COM{comision}.{tabla} AS {tabla}  SET {variable} = {valor_nuevo} WHERE {tabla}.`{tabla}-id` = {id}; \n")
                archivo.write(f"UPDATE ine_encovi.bitacora AS bitacora  SET usuario = {tabla} and base_datos = {rond}_COM{comision} and tabla = {tabla} and variable = {variable} and valor_anterior = {tabla} and valor_nuevo = {valor_nuevo} and id_registro = {id} and fecha_creacion = {fecha}; \n")

        


    def obtener_lista_analistas(self):
        """Devuelve una lista con todos los analistas del archivo conteo_analistas.csv en Google Drive."""
        
        # Buscar el archivo en Google Drive
        file_list = self.drive.ListFile({'q': f"'{self.FOLDER_ID}' in parents"}).GetList()
        file_data = [f for f in file_list if f['title'] == 'conteo_analistas.xlsx'][0]
        
        # Obtener contenido del archivo y convertirlo a un DataFrame de pandas
        content = file_data.GetContentString()
        df = pd.read_excel(StringIO(content))
        
        # Retornar la columna "Analista" como una lista
        return df["Analista"].tolist()