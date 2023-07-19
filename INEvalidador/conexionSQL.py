import mysql.connector

class baseSQL:
    def __init__(self):
        # Parámetros de conexión
        usuario = 'mchinchilla'
        contraseña = 'Mchinchilla$2023'
        host = '10.0.0.90'
        puerto = '3307'

        # Establecer las conexiones
        self.__conexion_PR = mysql.connector.connect(
            user=usuario,
            password=contraseña,
            host=host,
            port=puerto,
            database='ENCOVI_PR'
        )
        self.__conexion_SR = mysql.connector.connect(
            user=usuario,
            password=contraseña,
            host=host,
            port=puerto,
            database='ENCOVI_SR'
        )

        # Crear los cursores
        self.cursor_PR = self.__conexion_PR.cursor()
        self.cursor_SR = self.__conexion_SR.cursor()
        
    def info_tablas(self, tipo: str='PR'):
        cursor = self.cursor_PR if tipo == 'PR' else self.cursor_SR

        cursor.execute("SHOW TABLES")
        tablas = cursor.fetchall()

        # Obtener la forma de cada tabla
        i = 0
        for tabla in tablas:
            try:
                tabla_nombre = tabla[0]

                # Obtener el número de filas
                cursor.execute(f"SELECT COUNT(*) FROM {tabla_nombre}")
                filas = cursor.fetchone()[0]

                # Obtener el número de columnas
                cursor.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{tabla_nombre}'")
                columnas = cursor.fetchone()[0]

                # Imprimir la forma de la tabla
                i+=1
                print(f"> {tabla_nombre}({i})\n   Filas: {filas} - Columnas: {columnas}")
            except Exception as e:
                print(f'> Error "{e}" al obtener la forma de la tabla {tabla_nombre}')

p = baseSQL()
p.info_tablas('PR')