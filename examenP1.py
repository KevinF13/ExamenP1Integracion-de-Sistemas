import pandas as pd
import pyodbc
import shutil  # Importar la librería shutil para mover archivos
from datetime import datetime
import time  # Importar la librería time para pausar la ejecución

# Parámetros de conexión a la base de datos
server = 'DESKTOP-5SRJD1M\\SQLEXPRESS'
database = 'Ventas_Consolidadas'
username = 'kcUser'
password = '13demayo'

# Cadena de conexión
connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Ruta de la carpeta de respaldo
backup_folder = 'ExamenP1\\Respaldo\\'

# Nombre de los archivos CSV y sus respectivos IDs de local
files = {
    'ExamenP1\\Origen\\Local1.csv': 1,
    'ExamenP1\\Origen\\Local2.csv': 2,
    'ExamenP1\\Origen\\Local3.csv': 3,
    'ExamenP1\\Origen\\Local4.csv': 4
}

while True:
    try:
        # Establecer la conexión a la base de datos dentro del bucle
        conn = pyodbc.connect(connection_string)

        # Iterar sobre cada archivo CSV y cargar los datos a la tabla Consolidacion
        for file_name, id_local in files.items():
            # Leer el archivo CSV
            df = pd.read_csv(file_name)

            # Agregar el ID de local como una columna
            df['IdLocal'] = id_local

            # Iterar sobre cada fila del DataFrame para insertar en la tabla Consolidacion
            for index, row in df.iterrows():
                # Seleccionar las columnas necesarias para la inserción
                values_to_insert = (
                    row['IdTransaccion'],
                    row['IdLocal'],
                    row['Fecha'],
                    row['IdCategoria'],
                    row['IdProducto'],
                    row['Cantidad'],
                    row['PrecioUnitario'],
                    row['TotalVenta']
                )

                # Preparar la consulta SQL de inserción
                sql = '''
                    INSERT INTO Consolidacion (IdTransaccion, IdLocal, Fecha, IdCategoria, IdProducto, Cantidad, PrecioUnitario, TotalVenta)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                '''

                # Crear un cursor para ejecutar la consulta
                cursor = conn.cursor()

                # Ejecutar la consulta SQL con los valores seleccionados
                cursor.execute(sql, values_to_insert)

                # Confirmar la transacción
                conn.commit()

                print(f'Datos de "{file_name}" ingresados correctamente en la tabla Consolidacion con IdLocal = {id_local}')

            # Obtener la fecha y hora actual en el formato dd/mm/aaaa_hh/mm/ss para el nombre de archivo de respaldo
            current_datetime = datetime.now().strftime('%d-%m-%Y_%H-%M-%S')  # Formato dd/mm/aaaa_hh/mm/ss

            # Construir el nuevo nombre de archivo con la fecha y hora actual como sufijo
            new_file_name = f'Local{id_local}_{current_datetime}.csv'

            # Mover el archivo CSV a la carpeta de respaldo con el nuevo nombre
            shutil.move(file_name, backup_folder + new_file_name)
            print(f'Archivo "{file_name}" movido a la carpeta de respaldo con nombre "{new_file_name}".')

        # Cerrar la conexión al finalizar el proceso dentro del bucle
        conn.close()

        print('Proceso completado. Esperando 10 segundos antes de la próxima ejecución...')
        time.sleep(5)  # Pausar la ejecución por 10 segundos antes de la próxima iteración

    except Exception as e:
        print('BUSCANDO ARCHIVOS DE LA CARPETA ORIGEN')
        time.sleep(5)  # Pausar la ejecución por 10 segundos en caso de error
