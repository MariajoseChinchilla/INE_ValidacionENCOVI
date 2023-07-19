import mysql.connector

# Parámetros de conexión
usuario = 'mchinchilla'
contraseña = 'Mchinchilla$2023'
puerto = '3307'
esquema = 'ENCOVI_PR' #ENCOVI_SR

# Establecer la conexión
conexion = mysql.connector.connect(
    user=usuario,
    password=contraseña,
    host='10.0.0.90',
    port=puerto,
    database=esquema
)

# Hacer algo con la conexión, por ejemplo, ejecutar una consulta
cursor = conexion.cursor()
cursor.execute("SHOW TABLES") # aqui va el query
resultado = cursor.fetchall()

# Imprimir los resultados
for fila in resultado:
    print(fila)

# Cerrar la conexión
conexion.close()
