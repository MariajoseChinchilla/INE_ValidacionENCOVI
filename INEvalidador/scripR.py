import mysql.connector
import pandas as pd
import os
from openpyxl import Workbook
import pkg_resources

class ScripR:
    def __init__(self) -> None:
        # SQL query
        self.sql = """
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

        # Conectar a la base de datos MySQL
        try:
            cnn = mysql.connector.connect(
                user='mchinchilla',
                password='mchinchilla$2023',
                host='20.10.8.4',
                port=3307,
                database='ENCOVI_PR'
            )
            cursor = cnn.cursor()

            # Obtener datos de ocupaciones
            self.data = pd.read_sql(self.sql, cnn)
            
        except Exception as e:
            print(e)
        finally:
            cnn.close()

    def procesar_datos(self, ruta_salida: str="", archivo_grupos: str="") -> pd.DataFrame:
        if not archivo_grupos:
            grupos = pkg_resources.resource_filename(__name__, 'archivos\GruposC2.xlsx')
        
        # Leer el archivo de grupos
        gs = pd.read_excel(grupos)
        
        # Unir con el DataFrame 'data'
        data = pd.merge(self.data, gs, on=['DEPTO', 'MUPIO', 'SECTOR'], how='left')
        
        print(f"Total de inconsistencias: {data.shape[0]}")
        
        ruta = os.path.join(ruta_salida, "Mario")
        # Crear carpeta si no existe
        if not os.path.exists(ruta):
            os.mkdir(ruta)
            
        # Crear carpeta con marca temporal
        timestamp = pd.Timestamp.now().strftime("%d-%m-%H-%M")
        timestamp_folder = os.path.join(ruta, f"Inconsistencias_{timestamp}")
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
