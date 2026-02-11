import sys
from awsglue.transforms import *
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from typing import Dict, Any, Optional
import boto3

# Inicializar Glue Context
#args = getResolvedOptions(sys.argv, ['Job_Conexion'])
#sc = SparkContext()
#glueContext = GlueContext(sc)
#spark = glueContext.spark_session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

job = Job(glueContext)
#job.init(args['Job_Conexion'], args)
JOB_NAME = "nt_pe_cob_landing_to_bronze"
job.init(JOB_NAME,{})

print('inicio de job')
# Configurar compatibilidad de fechas
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
#spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
#spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
#spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
#spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
#spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
#spark.conf.set("spark.sql.parquet.mergeSchema", "false")


ssm   = boto3.client("ssm")
amb   = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
bdawscobbr    = ssm.get_parameter(Name='p_aws_cob_br_db', WithDecryption=True)['Parameter']['Value']
bdawscobtmp    = ssm.get_parameter(Name='p_aws_cob_tmp_db', WithDecryption=True)['Parameter']['Value']


# 1. Leer archivo TXT desde S3 con lista de tablas
ruta_txt  = f"s3://ue1stg{amb}as3cob001/ArchivosTXT/ListaTablasParqueEternoConDataEntry.txt"
ruta_orig = f"s3://ue1stg{amb}as3cob001/TABLAS"
ruta_dest = f"s3://ue1stg{amb}as3cob002/TABLAS_BRONZE"
tablas_df = spark.read.text(ruta_txt)   # cada fila = una tabla #spark.read.option("delimiter", "|").format("csv").option("header", "false").load(ruta_txt) #
tablas = [fila.value.strip() for fila in tablas_df.collect() if fila.value.strip()]
print('✅ Lectura de parametros realizado con éxito ')
# Recorrer las tablas y exportar a S3
for tabla in tablas:
    print(f"Procesando tabla: {tabla}")
    #table = f"{tabla}"
    
    path_source = f"{ruta_orig}/{tabla}/"
    path_target = f"{ruta_dest}/{tabla}/"
    
    
    try:
        # Verificar si la tabla gold ya existe
        try:
            df_existentes = spark.read.format("parquet").load(path_target)
            datos_existentes = True
            logger.info(f"Datos existentes cargados: {df_existentes.count()} registros")
            print(f'✅ Si existe la tabla {tabla}')
        except:
            datos_existentes = False
            logger.info(f"No se encontraron datos existentes ")
            print(f'⚠️ No existe la tabla {tabla}')
            
            
            
        if datos_existentes:
            
            df_nuevos = spark.read.format("parquet").load(path_source)
            
            final_data = df_nuevos
            
            ## Escribir los resultados en ruta temporal
            #additional_options = {
            #    "path": f"{ruta_dest}/Temp/{tabla}"
            #}
            #final_data.write \
            #    .format("parquet") \
            #    .options(**additional_options) \
            #    .mode("overwrite") \
            #    .saveAsTable(f"{bdawscobtmp}.{tabla}")
            #
            #final_data2 = spark.read.format("parquet").load(f"{ruta_dest}/Temp/{tabla}")
            
            additional_options = {
                "path": f"{path_target}"
            }
            final_data.write \
                .format("parquet") \
                .options(**additional_options) \
                .mode("overwrite") \
                .saveAsTable(f"{bdawscobbr}.{tabla}")
            
            cant_ingreso = df_nuevos.count()
            
            print(f"✅ Total de registros desde landing a la tabla bronze {tabla} : {cant_ingreso}")
            
             #Limpia la ubicación temporal
            #glueContext.purge_s3_path(f"{ruta_dest}/Temp/{tabla}", {"retentionPeriod": 0})
            #print(f"✅ parquet temporal {tabla} eliminada correctamente de la ruta temporal {ruta_dest}/Temp/{tabla}.")
            #glue_client.delete_table(DatabaseName=bdawscobtmp, Name=tabla)
            #print(f"✅ Tabla {tabla} eliminada correctamente de la base de datos '{bdawscobtmp}'.")
                
        else:
            
            df_nuevos = spark.read.format("parquet").load(path_source)
            
            additional_options = {
                "path": f"{path_target}"
            }
            df_nuevos.write \
                .format("parquet") \
                .options(**additional_options) \
                .mode("overwrite") \
                .saveAsTable(f"{bdawscobbr}.{tabla}")
            
            cant_ingreso = df_nuevos.count()
            
            print(f"✅ Total de registros desde landing de la tabla bronze {tabla} : {cant_ingreso}")
            
    except Exception as e:
        error_msg = f"Error en el proceso: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
   
job.commit()
print('fin de job')
job.commit()