import os
import mysql.connector
import pandas as pd
from openpyxl import Workbook
import pkg_resources

class ScripR:
    def __init__(self, host: str = '20.10.8.4', port: int = 3307, user: str = 'mchinchilla', 
                 password: str = 'mchinchilla$2023', database: str = 'ENCOVI_PR') -> None:
        """
        Constructor de la clase ScripR. 
        Se conecta a la base de datos MySQL y recupera datos basados en el SQL proporcionado.
        
        Args:
        - host: dirección del host de la base de datos.
        - port: puerto para la conexión.
        - user: nombre de usuario para la base de datos.
        - password: contraseña del usuario.
        - database: nombre de la base de datos.
        """
        self.sql = self._generate_sql()
        self.data = self._connect_to_db(host, port, user, password, database)
        
    def _generate_sql(self) -> str:
        """
        Genera la consulta SQL.
        
        Returns:
        - Consulta SQL.
        """
        sql = """
        SELECT encuestador ENCUESTADOR, depto DEPTO, mupio MUPIO, sector SECTOR, estructura ESTRUCTURA, vivienda VIVIENDA, hogar HOGAR, p.cp CP, 
            10 CAPITULO, 'C' SECCION, 2 PREGUNTA,
            'P10C02 Ocupación principal falta o insuficientemente descrita' AS 'DEFINICION DE INCONSISTENCIA',
            '10C00251' AS 'CODIGO ERROR', '' COMENTARIOS,
            p.p10c02 VALOR
        FROM `level-1` l
            INNER JOIN cases c ON c.id=l.`case-id`
            INNER JOIN personas p ON p.`level-1-id` = l.`level-1-id` 
            INNER JOIN caratula r ON r.`level-1-id`=l.`level-1-id`
        WHERE c.deleted=0 AND p.p10c01>=1 AND r.estado_pr=1 AND (LENGTH(IFNULL(p.p10c02,''))<=3 OR UPPER(IFNULL(p.p10c02,'')) IN ('NO','NA','N/A','NADA','ESTUDIA','ESTUDIAR','NINGUNO') ) AND 
            DATEDIFF(CURDATE(),(SELECT STR_TO_DATE(MAX(v.r1_fecha_inicial),'%d/%m/%y') FROM registro_de_visitas_pr v WHERE v.`level-1-id`=l.`level-1-id`))<=4
        UNION
        SELECT encuestador ENCUESTADOR, depto DEPTO, mupio MUPIO, sector SECTOR, estructura ESTRUCTURA, vivienda VIVIENDA, hogar HOGAR, p.cp CP, 
            10 CAPITULO, 'C' SECCION, 3 PREGUNTA,
            'P10C03 Actividad principal falta o insuficientemente descrita' AS 'DEFINICION DE INCONSISTENCIA',
            '10C00351' AS 'CODIGO ERROR', '' COMENTARIOS,
            p.p10c03 VALOR
        FROM `level-1` l
            INNER JOIN cases c ON c.id=l.`case-id`
            INNER JOIN personas p ON p.`level-1-id` = l.`level-1-id` 
            INNER JOIN caratula r ON r.`level-1-id`=l.`level-1-id`
        WHERE c.deleted=0 AND p.p10c01>=1 AND r.estado_pr=1 AND (LENGTH(IFNULL(p.p10c03,''))<=3 OR UPPER(IFNULL(p.p10c03,'')) IN ('NO','NA','N/A','NADA','ESTUDIA','ESTUDIAR','NINGUNA','NINGUNO') ) AND 
            DATEDIFF(CURDATE(),(SELECT STR_TO_DATE(MAX(v.r1_fecha_inicial),'%d/%m/%y') FROM registro_de_visitas_pr v WHERE v.`level-1-id`=l.`level-1-id`))<=4
        UNION
        SELECT encuestador ENCUESTADOR, depto DEPTO, mupio MUPIO, sector SECTOR, estructura ESTRUCTURA, vivienda VIVIENDA, hogar HOGAR, p.cp CP, 
            10 CAPITULO, 'C' SECCION, 4 PREGUNTA,
            'P10C04 Empresa falta o insuficientemente descrita' AS 'DEFINICION DE INCONSISTENCIA',
            '10C00451' AS 'CODIGO ERROR', '' COMENTARIOS,
            p.p10c04 VALOR
        FROM `level-1` l
            INNER JOIN cases c ON c.id=l.`case-id`
            INNER JOIN personas p ON p.`level-1-id` = l.`level-1-id` 
            INNER JOIN caratula r ON r.`level-1-id`=l.`level-1-id`
        WHERE c.deleted=0 AND p.p10c01>=1 AND r.estado_pr=1 AND (LENGTH(IFNULL(p.p10c04,''))<=2 OR UPPER(IFNULL(p.p10c04,'')) IN ('NO','NA','N/A','POR','NADA','ESTUDIA','ESTUDIAR') ) AND 
            DATEDIFF(CURDATE(),(SELECT STR_TO_DATE(MAX(v.r1_fecha_inicial),'%d/%m/%y') FROM registro_de_visitas_pr v WHERE v.`level-1-id`=l.`level-1-id`))<=4
        UNION
        SELECT encuestador ENCUESTADOR, depto DEPTO, mupio MUPIO, sector SECTOR, estructura ESTRUCTURA, vivienda VIVIENDA, hogar HOGAR, p.cp CP, 
            10 CAPITULO, 'C' SECCION, 7 PREGUNTA,
            'P10C07 Productos falta o insuficientemente descrita' AS 'DEFINICION DE INCONSISTENCIA',
            '10C00751' AS 'CODIGO ERROR', '' COMENTARIOS,
            p.p10c07 VALOR
        FROM `level-1` l
            INNER JOIN cases c ON c.id=l.`case-id`
            INNER JOIN personas p ON p.`level-1-id` = l.`level-1-id` 
            INNER JOIN caratula r ON r.`level-1-id`=l.`level-1-id`
        WHERE c.deleted=0 AND p.p10c01>=1 AND r.estado_pr=1 AND (LENGTH(IFNULL(p.p10c07,''))<=2 OR UPPER(IFNULL(p.p10c07,'')) IN ('NO','NA','N/A','POR','NADA','ESTUDIA','ESTUDIAR') ) AND 
            DATEDIFF(CURDATE(),(SELECT STR_TO_DATE(MAX(v.r1_fecha_inicial),'%d/%m/%y') FROM registro_de_visitas_pr v WHERE v.`level-1-id`=l.`level-1-id`))<=4;
        """
        return sql

    def _connect_to_db(self, host: str, port: int, user: str, password: str, database: str) -> pd.DataFrame:
        """
        Conecta con la base de datos y recupera datos basados en el SQL.
        
        Args:
        - host: dirección del host de la base de datos.
        - port: puerto para la conexión.
        - user: nombre de usuario para la base de datos.
        - password: contraseña del usuario.
        - database: nombre de la base de datos.
        
        Returns:
        - DataFrame con los datos recuperados.
        """
        try:
            cnn = mysql.connector.connect(user=user, password=password, host=host, port=port, database=database)
            data = pd.read_sql(self.sql, cnn)
            return data
        except Exception as e:
            print(e)
            return pd.DataFrame()
        finally:
            cnn.close()

    def procesar_datos(self, ruta_salida: str = "", archivo_grupos: str = "") -> pd.DataFrame:
        """
        Procesa los datos recuperados de la base de datos y los guarda en Excel.
        
        Args:
        - ruta_salida: ruta donde se guardarán los archivos.
        - archivo_grupos: ruta del archivo de grupos.
        
        Returns:
        - DataFrame con los datos procesados.
        """
        if not archivo_grupos:
            grupos = pkg_resources.resource_filename(__name__, 'archivos\GruposC2.xlsx')
        
        # Leer el archivo de grupos
        gs = pd.read_excel(grupos)
        
        # Unir con el DataFrame 'data'
        data = pd.merge(self.data, gs, on=['DEPTO', 'MUPIO', 'SECTOR'], how='left')
        
        print(f"Total de inconsistencias: {data.shape[0]}")
        
        # Crear carpeta si no existe
        if not os.path.exists(ruta_salida):
            os.mkdir(ruta_salida)
            
        # Crear carpeta con marca temporal
        timestamp_folder = os.path.join(ruta_salida, f"Inconsistencias_R")
        os.mkdir(timestamp_folder)
        
        # Guardar el archivo Excel de inconsistencias totales
        data.to_excel(os.path.join(timestamp_folder, "Inconsistencias.xlsx"), index=False)
        
        # Guardar los archivos de inconsistencias por grupo
        lista = data['GRUPO'].unique()
        for item in lista:
            cuadro = data[data['GRUPO'] == item].drop(columns=['VALOR', 'GRUPO'])
            nombre = f"InconsistenciasGRUPO{item}.xlsx"
            
            print(f">> {nombre} -> {cuadro.shape[0]}")
            
            cuadro.to_excel(os.path.join(timestamp_folder, nombre), index=False)
