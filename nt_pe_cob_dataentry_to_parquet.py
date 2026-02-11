import sys
from awsglue.transforms import *
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from typing import Dict, Any, Optional
import boto3
from pyspark.sql import SparkSession

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

job = Job(glueContext)
#job.init(args['Job_Conexion'], args)
JOB_NAME = "nt_pe_cob_dataentry_to_parquet"
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
bucket = f"ue1stg{amb}as3cobdataentry"
ruta_txt  = f"s3://{bucket}/ArchivosTXT/ListaDataEntryPE.txt"
ruta_orig = f"s3://{bucket}/DataEntryCobranza"
ruta_dest = f"s3://ue1stg{amb}as3cob001/TABLAS"

dataentries_df = spark.read.text(ruta_txt)   # cada fila = una tabla #spark.read.option("delimiter", "|").format("csv").option("header", "false").load(ruta_txt) #
dataentries = [fila.value.strip() for fila in dataentries_df.collect() if fila.value.strip()]
print('✅ Lectura de parametros realizado con éxito ')
# Recorrer las tablas y exportar a S3
for dataentry in dataentries:
    print(f"Procesando datentry: {dataentry}.csv")
    #table = f"{tabla}"
    excel_key = f"DataEntryCobranza/{dataentry}.csv"
    path_source = f"{ruta_orig}/{dataentry}.csv"
    path_target = f"{ruta_dest}/{dataentry}"
    

    try:
        df_existentes = spark.read.format("csv").option("header", "true").load(path_source)
        datos_existentes = True
    except:
        datos_existentes = False
        
        
    
    if datos_existentes:
        logger.info(f"Datos existentes cargados: {df_existentes.count()} registros")
        print(f'✅ Si existe el dataentry {dataentry}.csv')
        
        df_nuevos = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path_source)
        
        final_data = df_nuevos
        
        final_data.write.mode("overwrite").parquet(path_target)
        
        cant_ingreso = df_nuevos.count()
        
        print(f"✅ Total de registros del dataentry {dataentry} : {cant_ingreso}")
            
    else:
        logger.info(f"No se encontraron datos existentes ")
        print(f'⚠️ No existe el dataentry {dataentry}.csv')
            
        
       
   
job.commit()
print('fin de job')
job.commit()