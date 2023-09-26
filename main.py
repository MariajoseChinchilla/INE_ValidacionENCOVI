import os
os.environ["R_HOME"] = r"C:\Program Files\R\R-4.3.1" # change as needed
from INEvalidador import Validador

validador = Validador(descargar=0)

#validador.validar_encuesta("2023-9-23", "2023-9-25")