from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import pkg_resources
import os
from datetime import datetime
import time
import matplotlib.pyplot as plt
from io import StringIO
from io import BytesIO
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

    def obtener_conteo_desde_archivo(self, archivo, ruta):
        """Devuelve el conteo de líneas desde el archivo del analista del día actual."""
        # fecha_actual = datetime.now().strftime('%Y-%m-%d')
        # ruta_archivo = os.path.join(self.ruta_limpieza, f"{analista}_{fecha_actual}\{analista}.txt")
        # os.mkdir(os.path.join(self.ruta_limpieza, f"{analista}_{fecha_actual}"))
        # if not os.path.exists(ruta_archivo):
            # return 0
        with open(archivo, 'r') as file:
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

    def obtener_lista_analistas(self):
        # Buscar el archivo en Google Drive
        file_list = self.drive.ListFile({'q': f"'{self.FOLDER_ID}' in parents"}).GetList()
        
        # Filtrar para obtener el archivo deseado
        filtered_files = [f for f in file_list if f['title'] == 'conteo_analistas.xlsx']
        if not filtered_files:
            raise ValueError("No se encontró el archivo 'conteo_analistas.xlsx' en la carpeta especificada.")
        file_data = filtered_files[0]
        
        # Descargar el archivo temporalmente
        temp_file_path = "temp_conteo_analistas.xlsx"
        file_data.GetContentFile(temp_file_path)
        
        # Leer el archivo descargado con pandas
        df = pd.read_excel(temp_file_path)
        
        # Eliminar el archivo temporal
        os.remove(temp_file_path)
        
        # Retornar la columna "Analista" como una lista
        return df["Analista"].tolist()
