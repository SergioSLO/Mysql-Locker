import boto3
import pymysql
import time
from loguru import logger
import os

# Configuración del loguru
id = "etl_mysql"  # Identificador único del proceso
log_dir = "/var/log/ciencia_datos"  # Directorio común de logs en la máquina virtual

# Crear el directorio si no existe
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Nombre del archivo de log basado en el contenedor
container_name = os.getenv('HOSTNAME', 'container_name')  # Usando el nombre del contenedor o default
log_filename = f"{log_dir}/{container_name}_log.log"

# Configurar loguru para que los logs se escriban en el archivo y tengan el formato necesario
logger.add(log_filename,
           format="{time:YYYY-MM-DD HH:mm:ss.SSS} {level} {name} {message}",
           level="INFO")
# Conexión a Athena
athena_client = boto3.client('athena', region_name='us-east-1')

def execute_athena_query(query, database, output_location):
    try:
        logger.info(f"Ejecutando consulta Athena: {query}")
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )
        query_execution_id = response['QueryExecutionId']
        logger.info(f"Consulta iniciada con ID: {query_execution_id}")
        return query_execution_id
    except Exception as e:
        logger.error(f"Error ejecutando consulta Athena: {e}")
        raise

def get_query_results(query_execution_id):
    try:
        logger.info(f"Esperando resultados para la consulta Athena con ID: {query_execution_id}")
        all_results = []  # Lista para almacenar todos los resultados

        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                logger.info("Consulta Athena completada exitosamente")
                
                # Recupera los resultados paginados
                next_token = None
                query_results = athena_client.get_query_results(
                            QueryExecutionId=query_execution_id
                )
                all_results.extend(query_results['ResultSet']['Rows'][1:])  # Ignorar la primera fila (encabezados)
                        
                # Si hay más resultados, obtener el siguiente bloque
                next_token = query_results.get('NextToken', None)
                while True:
                    logger.info(f"Recuperando resultados adicionales con NextToken: {next_token}")
                    if not next_token:  # No hay más resultados
                        break
                    if next_token:
                        query_results = athena_client.get_query_results(
                            QueryExecutionId=query_execution_id,
                            NextToken=next_token
                        )
                        all_results.extend(query_results['ResultSet']['Rows'][1:])  # Ignorar la primera fila (encabezados)
                        
                        # Si hay más resultados, obtener el siguiente bloque
                        next_token = query_results.get('NextToken', None)
                    

                return all_results

            elif status == 'FAILED':
                logger.error("La consulta Athena falló")
                raise Exception("Query failed")
            time.sleep(5)
    except Exception as e:
        logger.error(f"Error obteniendo resultados de Athena: {e}")
        raise


# Conexión a MySQL
mysql_connection = pymysql.connect(
    host='54.87.145.182',  # Dirección IP del host de MySQL
    port=8005,  # Puerto donde está escuchando MySQL
    user='root',  # Asegúrate de usar el usuario correcto
    password='utec',  # Asegúrate de usar la contrasea correcta
    cursorclass=pymysql.cursors.DictCursor
)


# Función para ejecutar SQL en MySQL
def execute_mysql_sql(sql):
    try:
        with mysql_connection.cursor() as cursor:
            logger.info(f"Ejecutando SQL en MySQL: {sql}")
            cursor.execute(sql)
            mysql_connection.commit()
            logger.info("SQL ejecutado exitosamente")
    except Exception as e:
        logger.error(f"Error ejecutando SQL en MySQL: {e}")
        raise

# Conexión a MySQL después de crear la base de datos
mysql_connection.select_db('looker')  # Usamos la base de datos 'looker'

# Función para cargar datos de Athena a MySQL
def load_to_mysql(data, table):
    try:
        logger.info(f"Cargando datos a MySQL en la tabla {table}...")
        with mysql_connection.cursor() as cursor:
            for row in data:
                tenant_id = row['Data'][0]['VarCharValue']
                student_id = row['Data'][1]['VarCharValue']
                student_email = row['Data'][2]['VarCharValue']
                fecha_ingreso = row['Data'][3]['VarCharValue']
                nombre = row['Data'][4]['VarCharValue']
                contrasea = row['Data'][5]['VarCharValue']
                fecha_nacimiento = row['Data'][6]['VarCharValue']
                genero = row['Data'][7]['VarCharValue']
                telefono = row['Data'][8]['VarCharValue']
                rockie_coins = row['Data'][9]['VarCharValue']
                rockie_gems = row['Data'][10]['VarCharValue']
                student_promos = row['Data'][11]['VarCharValue']

                sql = """
                INSERT INTO {table} (tenant_id, student_id, student_email, fecha_ingreso, nombre, contrasea,
                                     fecha_nacimiento, genero, telefono, rockie_coins, rockie_gems, student_promos)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """.format(table=table)

                cursor.execute(sql, (tenant_id, student_id, student_email, fecha_ingreso, nombre, contrasea,
                                     fecha_nacimiento, genero, telefono, rockie_coins, rockie_gems, student_promos))
                logger.info(f"Datos cargados para el estudiante {student_id}")

            mysql_connection.commit()
            logger.info("Datos cargados correctamente en MySQL.")
    except Exception as e:
        logger.error(f"Error cargando datos a MySQL: {e}")
        raise

# Ejemplo de uso con Athena
try:
    query = "SELECT * FROM \"t_students\" WHERE fecha_nacimiento > '2003-01-01'"
    database = "rockie_database_prod"
    output_location = 's3://output-athena-rockie/ETL/'

    query_execution_id = execute_athena_query(query, database, output_location)
    results = get_query_results(query_execution_id)

    # Extraer las filas de resultados (saltando la primera fila que es el encabezado)
    data = results  # Ignoramos la fila de encabezado
    load_to_mysql(data, 'student_mas_21')  # Usamos la nueva tabla 'student_mas_21'
except Exception as e:
    logger.error(f"Proceso fallido: {e}")