import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)

JOB_NAME = "nt_cnx_mysql_pe"
job.init(JOB_NAME,{})

print('inicio de job')
# Configurar compatibilidad de fechas
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
ssm   = boto3.client("ssm")
amb   = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
user  = ssm.get_parameter(Name='p_user', WithDecryption=True)['Parameter']['Value']
clave = ssm.get_parameter(Name='p_clave', WithDecryption=True)['Parameter']['Value']
bdmysql    = ssm.get_parameter(Name='p_mysql_db', WithDecryption=True)['Parameter']['Value']

# 1. Leer archivo TXT desde S3 con lista de tablas
ruta_txt = f"s3://ue1stg{amb}as3cob001/ArchivosTXT/ListaTablasParqueEterno.txt"

tablas_df = spark.read.text(ruta_txt)   # cada fila = una tabla #spark.read.option("delimiter", "|").format("csv").option("header", "false").load(ruta_txt) #
tablas = [fila.value.strip() for fila in tablas_df.collect() if fila.value.strip()]
print('✅ Lectura de parametros realizada con éxito')
# Recorrer las tablas y exportar a S3
for tabla in tablas:
    print(f"Procesando tabla: {tabla}")

    # Lee la tabla desde MySQL usando Glue DynamicFrame
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        # Define connection options for MySQL
        connection_options = {
            "url": f"jdbc:mysql://datalakeparqueeterno.cw3lrxtghsj1.us-west-2.rds.amazonaws.com:3306/{bdmysql}",
            "user": f"{user}",
            "password": f"{clave}",
            "dbtable": f"{tabla}"  # Or use "query" for a custom SQL query
        }
    )
    
    df = dynamic_frame.toDF()
    
    # Obtener el esquema de la tabla
    #schema = dynamic_frame.schema()
    #schema = dynamic_frame.schema().fields
    #df = dynamic_frame.toDF(schema)
    #df = spark.createDataFrame(dynamic_frame, schema)
    # Si la tabla no está vacía
    if df.count() > 0:
        
        #df = dynamic_frame.toDF()

        output_path = f"s3://ue1stg{amb}as3cob001/TABLAS/{tabla}"

        # Guardar en Parquet
        #df.write.mode("overwrite").parquet(output_path)
        #Guardamos la tabla 
        df.write.mode("overwrite").parquet(f"s3://ue1stg{amb}as3cob001/TABLAS/{tabla}/")
        #additional_options = {
        # "path": f"s3://ue1stg{amb}as3cob001/TABLAS/{tabla}/"
        #}
        #df.write \
        #    .format("parquet") \
        #    .options(**additional_options) \
        #    .mode("overwrite") 
        #    #.saveAsTable(f"db_prueba.{tabla}")

        print(f"✅ Tabla {tabla} exportada a {output_path}")
    else:
        print(f"⚠️ Tabla {tabla} vacía, se omite")
#import requests
#import json
#from pyspark.sql import SparkSession
#
## Inicializar Spark
#spark = SparkSession.builder.appName("GlueAPItoS3").getOrCreate()
#
## Parámetros del API
#base_url = "https://reqres.in/api/users"#"https://api.ejemplo.com/v1/data"
##headers = {"Authorization": "Bearer TU_TOKEN"}
#
## Parámetros de paginación
#page = 1
#limit = 100
#all_data = []
#
#while True:
#    print(f"📥 Consultando página {page}")
#    response = requests.get(f"{base_url}?page={page}&limit={limit}")
#    #response = requests.get(f"{base_url}?page={page}&limit={limit}", headers=headers)
#
#    if response.status_code != 200:
#        print(f"❌ Error en API: {response.status_code} {response.text}")
#        break
#
#    data = response.json()
#
#    # Si no hay más datos, terminamos
#    if not data or len(data) == 0:
#        print("✅ Fin de datos")
#        break
#
#    all_data.extend(data)
#    page += 1
#
## Convertir lista de JSONs a DataFrame Spark
#if all_data:
#    df1 = spark.read.json(spark.sparkContext.parallelize([json.dumps(row) for row in all_data]))
#
#    # Guardar en S3 como Parquet
#    output_path = f"s3://ue1stgdesaas3cob001/TABLAS/api/"
#    df1.write.mode("overwrite").parquet(output_path)
#
#    print(f"✅ {df1.count()} registros exportados a {output_path}")
#else:
#    print("⚠️ No se obtuvo data del API")
#
#import requests
#import json
#from pyspark.sql import SparkSession
#
## Inicializar Spark
#spark = SparkSession.builder.appName("GlueAPItoS3").getOrCreate()
#
## Parámetros del API
#base_url = "https://api.covidtracking.com/v1/us/daily.json"
#per_page = 2
#page = 1
#all_data = []
#
#while True:
#    print(f"📥 Consultando página {page}")
#    response = requests.get(f"{base_url}?page={page}&per_page={per_page}")
#
#    if response.status_code != 200:
#        print(f"❌ Error en API: {response.status_code} {response.text}")
#        break
#
#    data = response.json()
#    users = data.get("data", [])
#    #for row in data:   # ✅ data es list
#    #    print(row["id"], row["name"])
#
#    if not users:  # cuando no hay más registros
#        print("✅ Fin de datos")
#        break
#
#    all_data.extend(users)
#    page += 1
#
## Convertir lista de JSONs a DataFrame Spark
#if all_data:
#    # Creamos un RDD con los JSONs
#    rdd = spark.sparkContext.parallelize([json.dumps(row) for row in all_data])
#
#    # Leemos el RDD como JSON y lo convertimos en DataFrame
#    df1 = spark.read.json(rdd)
#
#    # Mostrar datos
#    df1.show()
#
#    # Guardar en S3 como Parquet
#    output_path = "s3://ue1stgdesaas3cob001/TABLAS/api/"
#    df1.write.mode("overwrite").parquet(output_path)
#
#    print(f"✅ {df1.count()} registros exportados a {output_path}")
#else:
#    print("⚠️ No se obtuvo data del API")
#
job.commit()
print('fin de job')
job.commit()
