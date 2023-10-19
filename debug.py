from INEvalidador.limpieza import Limpieza
limpieza = Limpieza(1,descargar=False)

limpieza.escribir_query_sq(archivo=r"C:\Users\mchinchilla\Desktop\Validador\Datos para Revisión\output_17-10-2023\Prueba Majo.xlsx",comision=1,usuario="mchinchilla")
#limpieza.df_para_limpieza("'DEPTO = 12 & MUPIO = 15 & SECTOR = 14649 & ESTRUCTURA = 133 & VIVIENDA = 121 & HOGAR = 4 '")