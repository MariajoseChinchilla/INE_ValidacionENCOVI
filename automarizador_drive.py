import pickle
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from datetime import datetime

# Configuración inicial
SCOPES = ['https://www.googleapis.com/auth/drive']

# Autenticación
creds = None
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

service = build('drive', 'v3', credentials=creds)

# Función para subir un archivo a una carpeta específica
def upload_to_folder(folder_id, filename):
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

# Usando la función anterior para subir 38 archivos a sus respectivas carpetas
files = []
folder_ids = ["ID_Carpeta1", "ID_Carpeta2", ...]  # Debes conocer los IDs de las carpetas donde quieras subir los archivos

for file, folder_id in zip(files, folder_ids):
    upload_to_folder(folder_id, file)


# Colocar el nombre de los archivos en files y los ids en folder_ids
# colocar los ids en la carpeta de folders