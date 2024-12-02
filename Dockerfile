FROM python:3.8-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar los archivos del proyecto
COPY . /app

# Instalar las dependencias necesarias
RUN pip install boto3 pymysql loguru

# Comando para ejecutar el script ETL
CMD ["python", "etl.py"]
