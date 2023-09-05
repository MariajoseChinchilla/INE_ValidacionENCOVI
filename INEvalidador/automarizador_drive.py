import os
import re
import pickle
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from datetime import datetime

def subir_a_drive(path):
    dia = datetime.now().day
    mes = datetime.now().month
    año = datetime.now().year

    SCOPES = ['https://www.googleapis.com/auth/drive']

    # Autenticación
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file('client_secret_915678628628-e8vekd1kcmhi008jphhrs6dsaflmfia2.apps.googleusercontent.com.json', SCOPES)
        creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)

    # Función para subir un archivo a una carpeta específica
    def upload_to_folder(folder_id, filename):
        df = pd.read_excel(filename)
        if df.empty:
            # print(f"El archivo {filename} está vacío y no se subirá.")
            return

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
    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

    # Ordenar la lista de archivos
    files = list(sorted(files, key=lambda x: int(re.search(r'GRUPO(\d+)', x).group(1)) if re.search(r'GRUPO(\d+)', x) else float('inf')))

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
    

    for file, folder_id in zip(files, folder_ids):
        upload_to_folder(folder_id, file)