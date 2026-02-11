import sys
from awsglue.transforms import *
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from typing import Dict, Any, Optional
import boto3

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

job = Job(glueContext)
#job.init(args['Job_Conexion'], args)
JOB_NAME = "nt_pe_cob_create_table_dim_fecha"
job.init(JOB_NAME,{})

print('inicio de job')
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import date, timedelta

ssm   = boto3.client("ssm")
amb   = ssm.get_parameter(Name='p_ambiente', WithDecryption=True)['Parameter']['Value']
#bdawscobbr    = ssm.get_parameter(Name='p_aws_cob_br_db', WithDecryption=True)['Parameter']['Value']
#bdawscobsi    = ssm.get_parameter(Name='p_aws_cob_si_db', WithDecryption=True)['Parameter']['Value']
bdawscobgl    = ssm.get_parameter(Name='p_aws_cob_gl_db', WithDecryption=True)['Parameter']['Value']
#bdawscobtmp   = ssm.get_parameter(Name='p_aws_cob_tmp_db', WithDecryption=True)['Parameter']['Value']


CONST_OUTPUT_TABLA_FECHA = "DIM_FECHA"
fecha_inicio = date(1999, 1, 1)
fecha_fin = date(2030, 12, 31)

ruta_dest = f"s3://ue1stg{amb}as3con004/TABLAS_GOLD/{CONST_OUTPUT_TABLA_FECHA}/"

print('✅ Lectura de parametros realizado con éxito ')

# === Generar rango de fechas ===
rango_dias = (fecha_fin - fecha_inicio).days + 1
fechas = [(fecha_inicio + timedelta(days=i),) for i in range(rango_dias)]

df_fecha = spark.createDataFrame(fechas, ["fecha"]).withColumn("fecha", F.col("fecha").cast(DateType()))

# === Agregar atributos de dimensión ===
df_dim_fecha = (
    df_fecha
    .withColumn("anio", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("dia", F.dayofmonth("fecha"))
    .withColumn("nombre_mes", F.date_format("fecha", "MMMM"))
    .withColumn("nombre_dia", F.date_format("fecha", "EEEE"))
    .withColumn("num_semana", F.weekofyear("fecha"))
    .withColumn("trimestre", F.quarter("fecha"))
    .withColumn("es_fin_semana", F.when(F.dayofweek("fecha").isin("6","7"), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("Pk_fecha", F.date_format("fecha", "yyyyMMdd").cast("int"))
    .select(
        "Pk_fecha", "fecha", "anio", "mes", "dia", "nombre_mes",
        "nombre_dia", "num_semana", "trimestre", "es_fin_semana"
    )
)

# === Guardar en formato parquet en S3 ===
#df_dim_fecha.write.format("parquet").mode("overwrite").save(ruta_dest)
df_dim_fecha.write.format("parquet").mode("overwrite").option("path", ruta_dest).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA}")

print('tabla dim_fecha creada y cargada')

job.commit()
print('fin de job')
job.commit()