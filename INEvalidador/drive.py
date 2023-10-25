import os
import pickle
import re
import pkg_resources
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

def subir_a_drive(ruta):

    SCOPES = ['https://www.googleapis.com/auth/drive']

    # ruta_archivos_exportar = self.obtener_carpeta_mas_reciente("Salidas_Finales")
    # Autenticación

    ruta_token = pkg_resources.resource_filename(__name__, "archivos/token.pickle")
    creds = None
    if os.path.exists(ruta_token):
        with open(ruta_token, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        ruta_credencial = pkg_resources.resource_filename(__name__, "archivos/CredencialesCompartido.json")
        flow = InstalledAppFlow.from_client_secrets_file(ruta_credencial, SCOPES)
        creds = flow.run_local_server(port=0)
        with open(ruta_token, 'wb') as token:
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
            "1FRz1FK4ogxvzcFSQUjIiMaBPjUqH7lvA","1IdZdQ6Y8ExDs4XhdQmDJjnyp1JrnrRFW", "1dxjV8y7J7nXNhTWWDRpc5LkMpQ047Wys",
            "11tORy_uT7l8-zFLww3R9aTD4jhszKaoE"]

    # Recorremos la lista de IDs de carpeta
    for i, folder_id in enumerate(folder_ids):
        # Buscamos si hay un archivo que corresponde al número de grupo actual (i + 1)
        filename = files_dict.get(i + 1)
        
        if filename:  # Si hay un archivo, lo subimos
            upload_to_folder(folder_id, os.path.join(ruta, filename))
        else:
            print(f"No se encontró archivo para el grupo {i + 1}. Pasando al siguiente grupo.")
