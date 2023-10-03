from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import os
from datetime import datetime
import time
import matplotlib as plt
from INEvalidador.conexionSQL import baseSQL
sql = baseSQL(False)
import pandas as pd

class GestorConteos:
    def __init__(self):
        # Autenticación con Google Drive
        gauth = GoogleAuth()
        gauth.LoadClientConfigFile("CredencialesCompartido.json")
        gauth.LocalWebserverAuth()
        self.drive = GoogleDrive(gauth)
        self.FOLDER_ID = '1xC2oCDbSiVCRVeilCK7SLyRhNCdQ8yaY'

    def escribir_query_sql(self, archivo, nombre_analista):
        folder_path = os.path.join(os.getcwd(), nombre_analista)
        nombre_sintaxis = archivo.replace(".xlsx", "")
        
        # Crea el directorio si aún no existe
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        
        df_queries = pd.read_excel(archivo)
        now = datetime.now()
        date_str = now.strftime("%d-%m-%Y")
        
        # Modifica la ruta de la variable sintaxis
        sintaxis = os.path.join(folder_path, f"{nombre_sintaxis}_{date_str}.txt")
        # Iniciar ciclo de escritura de las sintaxis SQL
        vars = list(df_queries["variable"])
        tablas = [sql.base_col.get(i.split(".", 1)[1].upper()) for i in vars if "." in i]
        valores_nuevos = list(df_queries["valor nuevo"])
        ronda = [f"ENCOVI_{tablas[0][-2:]}"] * len(tablas)
        vars = [var.split(".", 1)[1] for var in vars]
        tablas = [tabla[:-3] for tabla in tablas]
        datos_cart = list(df_queries[["depto","mupio","sector","estructura","vivienda","hogar","cp"]].itertuples(index=False))
        filtros = []
        for dep, mup, sec, estr, viv, hog, cpp in datos_cart:
            filtros.append(f"'level-1'.depto = {dep} and 'level-1'.mupio = {mup} and 'level-1'.sector = {sec} and 'level-1'.estructura = {estr} and 'level-1'.vivienda = {viv} and 'level-1'.hogar = {hog} and 'level-1'.cp = {cpp}")
        cuadruplas = list(zip(ronda, tablas, vars, valores_nuevos, filtros))
        for rond, tabla, variable, valor_nuevo, filtro in cuadruplas:
            with open(sintaxis, "a") as archivo:
                archivo.write(f"UPDATE {rond}.{tabla} AS {tabla} SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")
        for rond, tabla, variable, valor_nuevo, filtro in cuadruplas:
            # Reemplazar la subcadena " and 'level-1'.cp = 0" por la cadena vacía
            filtro = filtro.replace(" and 'level-1'.cp = 0", "")
            with open(sintaxis, "a") as archivo:
                archivo.write(f"UPDATE {rond}.{tabla} AS {tabla} SET {tabla}.{variable} = {valor_nuevo} WHERE {filtro}; \n")  # \n para una nueva línea después de cada instrucción

        with open(sintaxis, 'r') as archivo:
            contenido = archivo.read()
            contenido = contenido.replace(" and 'level-1'.cp = 0", "")

        with open(sintaxis, 'w') as archivo:
            archivo.write(contenido)

        
    def inicializar_archivo(self):
        """Crea y sube un archivo CSV inicial para los conteos de analistas a Google Drive."""
        with open('conteo_analistas.csv', 'w') as file:
            file.write("Analista,Conteo\n")  # Cabecera del CSV
        file_drive = self.drive.CreateFile({'title': 'conteo_analistas.csv', 'parents': [{'id': self.FOLDER_ID}]})
        file_drive.SetContentFile('conteo_analistas.csv')
        file_drive.Upload()

    def obtener_conteo_desde_archivo(self, analista, ruta_archivo):
            """Devuelve el conteo de líneas desde el archivo del analista."""
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
        file_data = [f for f in file_list if f['title'] == 'conteo_analistas.csv'][0]
        content = file_data.GetContentString()
        lines = content.split("\n")[1:]  # Omitir encabezado

        analistas = []
        conteos = []
        for line in lines:
            if line:  # Verificar si la línea no está vacía
                analista, conteo = line.split(',')
                analistas.append(analista)
                conteos.append(int(conteo))

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
        ruta_carpeta = os.path.join(os.path.expanduser("~"), "Desktop", "ReporteProductividad")
        if not os.path.exists(ruta_carpeta):
            os.makedirs(ruta_carpeta)
        ruta_imagen = os.path.join(ruta_carpeta, f"Productividad_{fecha_actual}.png")
        plt.savefig(ruta_imagen)

        # Mostrar el histograma (opcional)
        plt.show()

    def procesar_analista(self, analista, ruta_archivo):
            """Procesa el archivo del analista y actualiza el conteo en Drive."""
            # Obtener conteo de líneas del archivo del analista
            self.obtener_conteo_desde_archivo(analista, ruta_archivo)

            # Actualizar el conteo del analista en el archivo CSV en Google Drive
            self.actualizar_conteo_con_bloqueo(analista)