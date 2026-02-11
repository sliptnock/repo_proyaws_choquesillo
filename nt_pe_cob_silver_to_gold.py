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
JOB_NAME = "nt_pe_cob_silver_to_gold"
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
bdawscobsi    = ssm.get_parameter(Name='p_aws_cob_si_db', WithDecryption=True)['Parameter']['Value']
bdawscobgl    = ssm.get_parameter(Name='p_aws_cob_gl_db', WithDecryption=True)['Parameter']['Value']
bdawscobtmp   = ssm.get_parameter(Name='p_aws_cob_tmp_db', WithDecryption=True)['Parameter']['Value']

#tablas input SILVER
CONST_INPUT_TABLA_ADENDA_ESTADO = "ADENDA_ESTADO"
CONST_INPUT_TABLA_ADENDA_OPERACIONALTERNO = "ADENDA_OPERACIONALTERNO"
CONST_INPUT_TABLA_ADENDA_TIPO = "ADENDA_TIPO"
CONST_INPUT_TABLA_CLIENTE = "CLIENTE"
CONST_INPUT_TABLA_CLIENTE_ESTADOCIVIL = "CLIENTE_ESTADO_CIVIL"
CONST_INPUT_TABLA_CLIENTE_SEXO = "CLIENTE_SEXO"
CONST_INPUT_TABLA_CLIENTE_TIPODOCUMENTO = "CLIENTE_TIPO_DOCUMENTO"
CONST_INPUT_TABLA_CLIENTE_TIPOOCUPACION = "CLIENTE_TIPO_OCUPACION"
CONST_INPUT_TABLA_CLIENTE_TIPORELIGION = "CLIENTE_TIPO_RELIGION"
CONST_INPUT_TABLA_CONTRATO = "CONTRATO"
CONST_INPUT_TABLA_CONTRATO_ESTADO = "CONTRATO_ESTADO"
CONST_INPUT_TABLA_CONTRATO_TIPO = "CONTRATO_TIPO"
CONST_INPUT_TABLA_CONTRATO_TIPOCLASIFICACION = "CONTRATO_TIPO_CLASIFICACION"
CONST_INPUT_TABLA_CONTRATO_TIPOESPACIO = "CONTRATO_TIPO_ESPACIO"
CONST_INPUT_TABLA_CONTRATO_TIPOMONEDA = "CONTRATO_TIPO_MONEDA"
CONST_INPUT_TABLA_CONTRATO_TIPOSERVICIO = "CONTRATO_TIPO_SERVICIO"
CONST_INPUT_TABLA_CRONOGRAMA_ESTADO = "CRONOGRAMA_ESTADO" 
CONST_INPUT_TABLA_FINANCIAMIENTO_MODOPAGO = "FINANCIAMIENTO_MODO_PAGO"
CONST_INPUT_TABLA_FINANCIAMIENTO_ESTADO = "FINANCIAMIENTO_ESTADO"
CONST_INPUT_TABLA_LIQUIDACION_MOTIVO = "LIQUIDACION_MOTIVO"
CONST_INPUT_TABLA_LIQUIDACION_REPRESENTANTE = "LIQUIDACION_REPRESENTANTE"
CONST_INPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION = "LIQUIDACION_TIPO_LIQUIDACION"
CONST_INPUT_TABLA_PAGOS_ESTADO = "PAGOS_ESTADO"
CONST_INPUT_TABLA_PAGOS_TIPOCOMPROBANTE = "PAGOS_TIPO_COMPROBANTE"
CONST_INPUT_TABLA_PAQUETE_TIPO = "PAQUETE_TIPO"
CONST_INPUT_TABLA_PAQUETE_TIPOSERVICIO = "PAQUETE_TIPO_SERVICIO"
CONST_INPUT_TABLA_FINANCIAMIENTO_PRODUCTO = "F_FINANCIAMIENTO_PRODUCTO"
CONST_INPUT_TABLA_PAQUETE = "PAQUETE"
CONST_INPUT_TABLA_PIEDRAS_ESTADO = "PLACA_ESTADO"
CONST_INPUT_TABLA_PIEDRAS_TIPOLIBERACION = "PLACA_TIPO_LIBERACION"
CONST_INPUT_TABLA_PIEDRAS_TIPOORDEN = "PLACA_TIPO_ORDEN"
CONST_INPUT_TABLA_PIEDRAS_TIPO = "PLACA_TIPO"
CONST_INPUT_TABLA_F_ADENDAS = "F_ADENDAS"
CONST_INPUT_TABLA_F_CRONOGRAMA = "F_CRONOGRAMA"
CONST_INPUT_TABLA_F_FINANCIAMIENTO = "F_FINANCIAMIENTO"
CONST_INPUT_TABLA_F_LIQUIDACIONES = "F_LIQUIDACIONES"
CONST_INPUT_TABLA_F_ORDEN_PLACA = "F_ORDEN_PLACA"
CONST_INPUT_TABLA_F_PAGOS = "F_PAGOS"
CONST_INPUT_TABLA_CHURN = "CHURN"



#tablas output GOLD
CONST_OUTPUT_TABLA_ADENDA_ESTADO = "DIM_ADENDA_ESTADO"
CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO = "DIM_ADENDA_OPERACIONALTERNO"
CONST_OUTPUT_TABLA_ADENDA_TIPO = "DIM_ADENDA_TIPO"
CONST_OUTPUT_TABLA_CLIENTE = "DIM_CLIENTE"
CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL = "DIM_CLIENTE_ESTADO_CIVIL"
CONST_OUTPUT_TABLA_CLIENTE_SEXO = "DIM_CLIENTE_SEXO"
CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO = "DIM_CLIENTE_TIPO_DOCUMENTO"
CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION = "DIM_CLIENTE_TIPO_OCUPACION"
CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION = "DIM_CLIENTE_TIPO_RELIGION"
CONST_OUTPUT_TABLA_CONTRATO = "DIM_CONTRATO"
CONST_OUTPUT_TABLA_CONTRATO_ESTADO = "DIM_CONTRATO_ESTADO"
CONST_OUTPUT_TABLA_CONTRATO_TIPO = "DIM_CONTRATO_TIPO"
CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION = "DIM_CONTRATO_TIPO_CLASIFICACION"
CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO = "DIM_CONTRATO_TIPO_ESPACIO"
CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA = "DIM_CONTRATO_TIPO_MONEDA"
CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO = "DIM_CONTRATO_TIPO_SERVICIO"
CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO = "DIM_CRONOGRAMA_ESTADO" 
CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO = "DIM_FINANCIAMIENTO_MODO_PAGO"
CONST_OUTPUT_TABLA_FINANCIAMIENTO_ESTADO = "DIM_FINANCIAMIENTO_ESTADO"
CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO = "DIM_LIQUIDACION_MOTIVO"
CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE = "DIM_LIQUIDACION_REPRESENTANTE"
CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION = "DIM_LIQUIDACION_TIPO_LIQUIDACION"
CONST_OUTPUT_TABLA_PAGOS_ESTADO = "DIM_PAGOS_ESTADO"
CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE = "DIM_PAGOS_TIPO_COMPROBANTE"
CONST_OUTPUT_TABLA_PAQUETE_TIPO = "DIM_PAQUETE_TIPO"
CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO = "DIM_PAQUETE_TIPO_SERVICIO"
CONST_OUTPUT_TABLA_FINANCIAMIENTO_PRODUCTO = "FT_FINANCIAMIENTO_PRODUCTO"
CONST_OUTPUT_TABLA_PAQUETE = "DIM_PAQUETE"
CONST_OUTPUT_TABLA_PIEDRAS_ESTADO = "DIM_PLACA_ESTADO"
CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION = "DIM_PLACA_TIPO_LIBERACION"
CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN = "DIM_PLACA_TIPO_ORDEN"
CONST_OUTPUT_TABLA_PIEDRAS_TIPO = "DIM_PLACA_TIPO"
CONST_OUTPUT_TABLA_FECHA = "DIM_FECHA"
CONST_OUTPUT_TABLA_FT_ADENDAS = "FT_ADENDAS"
CONST_OUTPUT_TABLA_FT_CRONOGRAMA = "FT_CRONOGRAMA"
CONST_OUTPUT_TABLA_FT_FINANCIAMIENTO = "FT_FINANCIAMIENTO"
CONST_OUTPUT_TABLA_FT_LIQUIDACIONES = "FT_LIQUIDACIONES"
CONST_OUTPUT_TABLA_FT_ORDEN_PLACA = "FT_ORDEN_PLACA"
CONST_OUTPUT_TABLA_FT_PAGOS = "FT_PAGOS"
CONST_OUTPUT_TABLA_CHURN = "CHURN"


ruta_dest = f"s3://ue1stg{amb}as3cob004/TABLAS_GOLD"

print('✅ Lectura de parametros realizado con éxito ')
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_ADENDA_ESTADO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_ADENDA_ESTADO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Estado".lower())).collect()[0][0] or 0

    df_adenda_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_ADENDA_ESTADO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_adenda_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "left_anti"
    ).withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)") + max_pk ).select(col("Pk_Estado").alias("Pk_Estado"), df_adenda_estado["Cod_Estado"].alias("Cod_Estado"), df_adenda_estado["Descripcion"].alias("Estado"))
    # Inner join para actualizaciones
    updates = df_adenda_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "inner"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), df_adenda_estado["Descripcion"].alias("Estado"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_adenda_estado, 
        ["Cod_Estado"], 
        "left_anti"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), target_df["Estado"].alias("Estado"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_ESTADO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_adenda_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_ADENDA_ESTADO}""") 
    source_df = df_adenda_estado.withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)")).select(col("Pk_Estado").alias("Pk_Estado"), col("Cod_Estado").alias("Cod_Estado"), col("Descripcion").alias("Estado"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_ESTADO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_OperacionAlterno".lower())).collect()[0][0] or 0

    df_adenda_operacionalterno = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_ADENDA_OPERACIONALTERNO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_adenda_operacionalterno.join(
        target_df, 
        ["Cod_OperacionAlterno"], 
        "left_anti"
    ).withColumn(f"Pk_OperacionAlterno", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Operacionalterno)") + max_pk ).select(col("Pk_OperacionAlterno").alias("Pk_OperacionAlterno"), df_adenda_operacionalterno["Cod_OperacionAlterno"].alias("Cod_OperacionAlterno"), df_adenda_operacionalterno["Descripcion"].alias("OperacionAlterno"))
    # Inner join para actualizaciones
    updates = df_adenda_operacionalterno.join(
        target_df, 
        ["Cod_OperacionAlterno"], 
        "inner"
    ).select(target_df["Pk_OperacionAlterno"].alias("Pk_OperacionAlterno"), target_df["Cod_OperacionAlterno"].alias("Cod_OperacionAlterno"), df_adenda_operacionalterno["Descripcion"].alias("OperacionAlterno"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_adenda_operacionalterno, 
        ["Cod_OperacionAlterno"], 
        "left_anti"
    ).select(target_df["Pk_OperacionAlterno"].alias("Pk_OperacionAlterno"), target_df["Cod_OperacionAlterno"].alias("Cod_OperacionAlterno"), target_df["OperacionAlterno"].alias("OperacionAlterno"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_adenda_operacionalterno = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_ADENDA_OPERACIONALTERNO}""") 
    source_df = df_adenda_operacionalterno.withColumn(f"Pk_OperacionAlterno", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_OperacionAlterno)")).select(col("Pk_OperacionAlterno").alias("Pk_OperacionAlterno"), col("Cod_OperacionAlterno").alias("Cod_OperacionAlterno"), col("Descripcion").alias("OperacionAlterno"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_ADENDA_TIPO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_ADENDA_TIPO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Tipo".lower())).collect()[0][0] or 0

    df_adenda_tipo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_ADENDA_TIPO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_adenda_tipo.join(
        target_df, 
        ["Cod_Tipo"], 
        "left_anti"
    ).withColumn(f"Pk_Tipo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Tipo)") + max_pk ).select(col("Pk_Tipo").alias("Pk_Tipo"), df_adenda_tipo["Cod_Tipo"].alias("Cod_Tipo"), df_adenda_tipo["Descripcion"].alias("Tipo"))
    # Inner join para actualizaciones
    updates = df_adenda_tipo.join(
        target_df, 
        ["Cod_Tipo"], 
        "inner"
    ).select(target_df["Pk_Tipo"].alias("Pk_Tipo"), target_df["Cod_Tipo"].alias("Cod_Tipo"), df_adenda_tipo["Descripcion"].alias("Tipo"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_adenda_tipo, 
        ["Cod_Tipo"], 
        "left_anti"
    ).select(target_df["Pk_Tipo"].alias("Pk_Tipo"), target_df["Cod_Tipo"].alias("Cod_Tipo"), target_df["Tipo"].alias("Tipo"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_TIPO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_adenda_tipo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_ADENDA_TIPO}""") 
    source_df = df_adenda_tipo.withColumn(f"Pk_Tipo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Tipo)")).select(col("Pk_Tipo").alias("Pk_Tipo"), col("Cod_Tipo").alias("Cod_Tipo"), col("Descripcion").alias("Tipo"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_TIPO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_EstadoCivil".lower())).collect()[0][0] or 0

    df_cliente_estadocivil = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_ESTADOCIVIL}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_cliente_estadocivil.join(
        target_df, 
        ["Cod_EstadoCivil"], 
        "left_anti"
    ).withColumn(f"Pk_EstadoCivil", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_EstadoCivil)") + max_pk ).select(col("Pk_EstadoCivil").alias("Pk_EstadoCivil"), df_cliente_estadocivil["Cod_EstadoCivil"].alias("Cod_EstadoCivil"), df_cliente_estadocivil["Descripcion"].alias("EstadoCivil"))
    # Inner join para actualizaciones
    updates = df_cliente_estadocivil.join(
        target_df, 
        ["Cod_EstadoCivil"], 
        "inner"
    ).select(target_df["Pk_EstadoCivil"].alias("Pk_EstadoCivil"), target_df["Cod_EstadoCivil"].alias("Cod_EstadoCivil"), df_cliente_estadocivil["Descripcion"].alias("EstadoCivil"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_cliente_estadocivil, 
        ["Cod_EstadoCivil"], 
        "left_anti"
    ).select(target_df["Pk_EstadoCivil"].alias("Pk_EstadoCivil"), target_df["Cod_EstadoCivil"].alias("Cod_EstadoCivil"), target_df["EstadoCivil"].alias("EstadoCivil"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_cliente_estadocivil = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_ESTADOCIVIL}""") 
    source_df = df_cliente_estadocivil.withColumn(f"Pk_EstadoCivil", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_EstadoCivil)")).select(col("Pk_EstadoCivil").alias("Pk_EstadoCivil"), col("Cod_EstadoCivil").alias("Cod_EstadoCivil"), col("Descripcion").alias("EstadoCivil"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_SEXO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CLIENTE_SEXO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Sexo".lower())).collect()[0][0] or 0

    df_cliente_sexo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_SEXO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_cliente_sexo.join(
        target_df, 
        ["Cod_Sexo"], 
        "left_anti"
    ).withColumn(f"Pk_Sexo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Sexo)") + max_pk ).select(col("Pk_Sexo").alias("Pk_Sexo"), df_cliente_sexo["Cod_Sexo"].alias("Cod_Sexo"), df_cliente_sexo["Descripcion"].alias("Sexo"))
    # Inner join para actualizaciones
    updates = df_cliente_sexo.join(
        target_df, 
        ["Cod_Sexo"], 
        "inner"
    ).select(target_df["Pk_Sexo"].alias("Pk_Sexo"), target_df["Cod_Sexo"].alias("Cod_Sexo"), df_cliente_sexo["Descripcion"].alias("Sexo"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_cliente_sexo, 
        ["Cod_Sexo"], 
        "left_anti"
    ).select(target_df["Pk_Sexo"].alias("Pk_Sexo"), target_df["Cod_Sexo"].alias("Cod_Sexo"), target_df["Sexo"].alias("Sexo"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_SEXO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_cliente_sexo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_SEXO}""") 
    source_df = df_cliente_sexo.withColumn(f"Pk_Sexo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Sexo)")).select(col("Pk_Sexo").alias("Pk_Sexo"), col("Cod_Sexo").alias("Cod_Sexo"), col("Descripcion").alias("Sexo"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_SEXO}")

from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoDocumento".lower())).collect()[0][0] or 0

    df_cliente_tipodocumento = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_TIPODOCUMENTO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_cliente_tipodocumento.join(
        target_df, 
        ["Cod_TipoDocumento"], 
        "left_anti"
    ).withColumn(f"Pk_TipoDocumento", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoDocumento)") + max_pk ).select(col("Pk_TipoDocumento").alias("Pk_TipoDocumento"), df_cliente_tipodocumento["Cod_TipoDocumento"].alias("Cod_TipoDocumento"), df_cliente_tipodocumento["Descripcion"].alias("TipoDocumento"))
    # Inner join para actualizaciones
    updates = df_cliente_tipodocumento.join(
        target_df, 
        ["Cod_TipoDocumento"], 
        "inner"
    ).select(target_df["Pk_TipoDocumento"].alias("Pk_TipoDocumento"), target_df["Cod_TipoDocumento"].alias("Cod_TipoDocumento"), df_cliente_tipodocumento["Descripcion"].alias("TipoDocumento"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_cliente_tipodocumento, 
        ["Cod_TipoDocumento"], 
        "left_anti"
    ).select(target_df["Pk_TipoDocumento"].alias("Pk_TipoDocumento"), target_df["Cod_TipoDocumento"].alias("Cod_TipoDocumento"), target_df["TipoDocumento"].alias("TipoDocumento"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_cliente_tipodocumento = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_TIPODOCUMENTO}""") 
    source_df = df_cliente_tipodocumento.withColumn(f"Pk_TipoDocumento", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoDocumento)")).select(col("Pk_TipoDocumento").alias("Pk_TipoDocumento"), col("Cod_TipoDocumento").alias("Cod_TipoDocumento"), col("Descripcion").alias("TipoDocumento"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoOcupacion".lower())).collect()[0][0] or 0

    df_cliente_tipoocupacion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_TIPOOCUPACION}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_cliente_tipoocupacion.join(
        target_df, 
        ["Cod_TipoOcupacion"], 
        "left_anti"
    ).withColumn(f"Pk_TipoOcupacion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoOcupacion)") + max_pk ).select(col("Pk_TipoOcupacion").alias("Pk_TipoOcupacion"), df_cliente_tipoocupacion["Cod_TipoOcupacion"].alias("Cod_TipoOcupacion"), df_cliente_tipoocupacion["Descripcion"].alias("TipoOcupacion"))
    # Inner join para actualizaciones
    updates = df_cliente_tipoocupacion.join(
        target_df, 
        ["Cod_TipoOcupacion"], 
        "inner"
    ).select(target_df["Pk_TipoOcupacion"].alias("Pk_TipoOcupacion"), target_df["Cod_TipoOcupacion"].alias("Cod_TipoOcupacion"), df_cliente_tipoocupacion["Descripcion"].alias("TipoOcupacion"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_cliente_tipoocupacion, 
        ["Cod_TipoOcupacion"], 
        "left_anti"
    ).select(target_df["Pk_TipoOcupacion"].alias("Pk_TipoOcupacion"), target_df["Cod_TipoOcupacion"].alias("Cod_TipoOcupacion"), target_df["TipoOcupacion"].alias("TipoOcupacion"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_cliente_tipoocupacion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_TIPOOCUPACION}""") 
    source_df = df_cliente_tipoocupacion.withColumn(f"Pk_TipoOcupacion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoOcupacion)")).select(col("Pk_TipoOcupacion").alias("Pk_TipoOcupacion"), col("Cod_TipoOcupacion").alias("Cod_TipoOcupacion"), col("Descripcion").alias("TipoOcupacion"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoReligion".lower())).collect()[0][0] or 0

    df_cliente_tiporeligion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_TIPORELIGION}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_cliente_tiporeligion.join(
        target_df, 
        ["Cod_TipoReligion"], 
        "left_anti"
    ).withColumn(f"Pk_TipoReligion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoReligion)") + max_pk ).select(col("Pk_TipoReligion").alias("Pk_TipoReligion"), df_cliente_tiporeligion["Cod_TipoReligion"].alias("Cod_TipoReligion"), df_cliente_tiporeligion["Descripcion"].alias("TipoReligion"))
    # Inner join para actualizaciones
    updates = df_cliente_tiporeligion.join(
        target_df, 
        ["Cod_TipoReligion"], 
        "inner"
    ).select(target_df["Pk_TipoReligion"].alias("Pk_TipoReligion"), target_df["Cod_TipoReligion"].alias("Cod_TipoReligion"), df_cliente_tiporeligion["Descripcion"].alias("TipoReligion"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_cliente_tiporeligion, 
        ["Cod_TipoReligion"], 
        "left_anti"
    ).select(target_df["Pk_TipoReligion"].alias("Pk_TipoReligion"), target_df["Cod_TipoReligion"].alias("Cod_TipoReligion"), target_df["TipoReligion"].alias("TipoReligion"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_cliente_tiporeligion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE_TIPORELIGION}""") 
    source_df = df_cliente_tiporeligion.withColumn(f"Pk_TipoReligion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoReligion)")).select(col("Pk_TipoReligion").alias("Pk_TipoReligion"), col("Cod_TipoReligion").alias("Cod_TipoReligion"), col("Descripcion").alias("TipoReligion"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CLIENTE}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Cliente".lower())).collect()[0][0] or 0

    df_cliente = spark.sql(f"""
    select 
     A.cod_cliente       
    ,B.pk_tipodocumento   
    ,A.nrodocumento        
    ,A.nombrecompleto      
    ,A.nrotelefonocel    
    ,A.nrotelefonocasa     
    ,A.nombre              
    ,A.apellidopat         
    ,A.apellidomat         
    ,A.apellidocasado      
    ,A.correopersonal      
    ,A.fechanacimiento     
    ,A.fechadefuncion      
    ,C.pk_estadocivil     
    ,D.pk_tiporeligion    
    ,E.pk_tipoocupacion   
    ,F.pk_sexo            
    ,A.direccion           
    ,A.urbanizacion        
    ,A.ciudad              
    ,A.distrito            
    ,A.provincia           
    ,A.referenciadireccion 
    ,A.lugartrabajo        
    ,A.direcciontrabajo    
    ,A.correotrabajo       
    ,A.nrotelefonotrabajo
    from {bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE} A 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO} B on A.cod_tipodocumento=B.cod_tipodocumento 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL} C on A.cod_estadocivil=C.cod_estadocivil 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION} D on CASE WHEN A.cod_tiporeligion = -1 THEN 0 ELSE A.cod_tiporeligion END =D.cod_tiporeligion
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION} E on A.cod_tipoocupacion=E.cod_tipoocupacion
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_SEXO} F on A.cod_sexo=F.cod_sexo 
    union all
    select 0,1,'00000000','','','','','','','','','','',2,1,5,1,'','','','','','','','','','' """) 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_cliente.join(
        target_df, 
        ["Cod_Cliente"], 
        "left_anti"
    ).withColumn(f"Pk_Cliente", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Cliente)") + max_pk ).select(col("Pk_Cliente").alias("Pk_Cliente"), df_cliente["Cod_Cliente"].alias("Cod_Cliente"), df_cliente["Pk_tipodocumento"].alias("Pk_tipodocumento"), \
                                                                                                   df_cliente["nrodocumento"].alias("nrodocumento"),df_cliente["nombrecompleto"].alias("nombrecompleto"),df_cliente["nrotelefonocel"].alias("nrotelefonocel"), \
                                                                                                   df_cliente["nrotelefonocasa"].alias("nrotelefonocasa"),df_cliente["nombre"].alias("nombre"),df_cliente["apellidopat"].alias("apellidopat"), \
                                                                                                   df_cliente["apellidomat"].alias("apellidomat"),df_cliente["apellidocasado"].alias("apellidocasado"),df_cliente["correopersonal"].alias("correopersonal"), \
                                                                                                   df_cliente["fechanacimiento"].alias("fechanacimiento"),df_cliente["fechadefuncion"].alias("fechadefuncion"),df_cliente["Pk_estadocivil"].alias("Pk_estadocivil"), \
                                                                                                   df_cliente["Pk_tiporeligion"].alias("Pk_tiporeligion"),df_cliente["Pk_tipoocupacion"].alias("Pk_tipoocupacion"),df_cliente["Pk_sexo"].alias("Pk_sexo"), \
                                                                                                   df_cliente["direccion"].alias("direccion"),df_cliente["urbanizacion"].alias("urbanizacion"),df_cliente["ciudad"].alias("ciudad"), \
                                                                                                   df_cliente["distrito"].alias("distrito"),df_cliente["provincia"].alias("provincia"),df_cliente["referenciadireccion"].alias("referenciadireccion"), \
                                                                                                   df_cliente["lugartrabajo"].alias("lugartrabajo"),df_cliente["direcciontrabajo"].alias("direcciontrabajo"),df_cliente["correotrabajo"].alias("correotrabajo"), \
                                                                                                   df_cliente["nrotelefonotrabajo"].alias("nrotelefonotrabajo"))
    # Inner join para actualizaciones
    updates = df_cliente.join(
        target_df, 
        ["Cod_Cliente"], 
        "inner"
    ).select(target_df["Pk_Cliente"].alias("Pk_Cliente"), target_df["Cod_Cliente"].alias("Cod_Cliente"), df_cliente["Pk_tipodocumento"].alias("Pk_tipodocumento"), \
                                                                                                   df_cliente["nrodocumento"].alias("nrodocumento"),df_cliente["nombrecompleto"].alias("nombrecompleto"),df_cliente["nrotelefonocel"].alias("nrotelefonocel"), \
                                                                                                   df_cliente["nrotelefonocasa"].alias("nrotelefonocasa"),df_cliente["nombre"].alias("nombre"),df_cliente["apellidopat"].alias("apellidopat"), \
                                                                                                   df_cliente["apellidomat"].alias("apellidomat"),df_cliente["apellidocasado"].alias("apellidocasado"),df_cliente["correopersonal"].alias("correopersonal"), \
                                                                                                   df_cliente["fechanacimiento"].alias("fechanacimiento"),df_cliente["fechadefuncion"].alias("fechadefuncion"),df_cliente["Pk_estadocivil"].alias("Pk_estadocivil"), \
                                                                                                   df_cliente["Pk_tiporeligion"].alias("Pk_tiporeligion"),df_cliente["Pk_tipoocupacion"].alias("Pk_tipoocupacion"),df_cliente["Pk_sexo"].alias("Pk_sexo"), \
                                                                                                   df_cliente["direccion"].alias("direccion"),df_cliente["urbanizacion"].alias("urbanizacion"),df_cliente["ciudad"].alias("ciudad"), \
                                                                                                   df_cliente["distrito"].alias("distrito"),df_cliente["provincia"].alias("provincia"),df_cliente["referenciadireccion"].alias("referenciadireccion"), \
                                                                                                   df_cliente["lugartrabajo"].alias("lugartrabajo"),df_cliente["direcciontrabajo"].alias("direcciontrabajo"),df_cliente["correotrabajo"].alias("correotrabajo"), \
                                                                                                   df_cliente["nrotelefonotrabajo"].alias("nrotelefonotrabajo"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_cliente, 
        ["Cod_Cliente"], 
        "left_anti"
    ).select(target_df["Pk_Cliente"].alias("Pk_Cliente"), target_df["Cod_Cliente"].alias("Cod_Cliente"), target_df["Pk_tipodocumento"].alias("Pk_tipodocumento"), \
                                                                                                   target_df["nrodocumento"].alias("nrodocumento"),target_df["nombrecompleto"].alias("nombrecompleto"),target_df["nrotelefonocel"].alias("nrotelefonocel"), \
                                                                                                   target_df["nrotelefonocasa"].alias("nrotelefonocasa"),target_df["nombre"].alias("nombre"),target_df["apellidopat"].alias("apellidopat"), \
                                                                                                   target_df["apellidomat"].alias("apellidomat"),target_df["apellidocasado"].alias("apellidocasado"),target_df["correopersonal"].alias("correopersonal"), \
                                                                                                   target_df["fechanacimiento"].alias("fechanacimiento"),target_df["fechadefuncion"].alias("fechadefuncion"),target_df["Pk_estadocivil"].alias("Pk_estadocivil"), \
                                                                                                   target_df["Pk_tiporeligion"].alias("Pk_tiporeligion"),target_df["Pk_tipoocupacion"].alias("Pk_tipoocupacion"),target_df["Pk_sexo"].alias("Pk_sexo"), \
                                                                                                   target_df["direccion"].alias("direccion"),target_df["urbanizacion"].alias("urbanizacion"),target_df["ciudad"].alias("ciudad"), \
                                                                                                   target_df["distrito"].alias("distrito"),target_df["provincia"].alias("provincia"),target_df["referenciadireccion"].alias("referenciadireccion"), \
                                                                                                   target_df["lugartrabajo"].alias("lugartrabajo"),target_df["direcciontrabajo"].alias("direcciontrabajo"),target_df["correotrabajo"].alias("correotrabajo"), \
                                                                                                   target_df["nrotelefonotrabajo"].alias("nrotelefonotrabajo"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_cliente = spark.sql(f"""
    select 
     A.cod_cliente       
    ,B.pk_tipodocumento   
    ,A.nrodocumento        
    ,A.nombrecompleto      
    ,A.nrotelefonocel    
    ,A.nrotelefonocasa     
    ,A.nombre              
    ,A.apellidopat         
    ,A.apellidomat         
    ,A.apellidocasado      
    ,A.correopersonal      
    ,A.fechanacimiento     
    ,A.fechadefuncion      
    ,C.pk_estadocivil     
    ,D.pk_tiporeligion    
    ,E.pk_tipoocupacion   
    ,F.pk_sexo            
    ,A.direccion           
    ,A.urbanizacion        
    ,A.ciudad              
    ,A.distrito            
    ,A.provincia           
    ,A.referenciadireccion 
    ,A.lugartrabajo        
    ,A.direcciontrabajo    
    ,A.correotrabajo       
    ,A.nrotelefonotrabajo
    from {bdawscobsi}.{CONST_INPUT_TABLA_CLIENTE} A 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO} B on A.cod_tipodocumento=B.cod_tipodocumento 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL} C on A.cod_estadocivil=C.cod_estadocivil 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION} D on CASE WHEN A.cod_tiporeligion = -1 THEN 0 ELSE A.cod_tiporeligion END=D.cod_tiporeligion
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION} E on A.cod_tipoocupacion=E.cod_tipoocupacion
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_SEXO} F on A.cod_sexo=F.cod_sexo
    union all
    select 0,1,'00000000','','','','','','','','','','',2,1,5,1,'','','','','','','','','','' """) 
    source_df = df_cliente.withColumn(f"Pk_Cliente", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Cliente)")).select(col("Pk_Cliente").alias("Pk_Cliente"), col("Cod_Cliente").alias("Cod_Cliente"), col("Pk_tipodocumento").alias("Pk_tipodocumento"), \
                                                                                                   col("nrodocumento").alias("nrodocumento"),col("nombrecompleto").alias("nombrecompleto"),col("nrotelefonocel").alias("nrotelefonocel"), \
                                                                                                   col("nrotelefonocasa").alias("nrotelefonocasa"),col("nombre").alias("nombre"),col("apellidopat").alias("apellidopat"), \
                                                                                                   col("apellidomat").alias("apellidomat"),col("apellidocasado").alias("apellidocasado"),col("correopersonal").alias("correopersonal"), \
                                                                                                   col("fechanacimiento").alias("fechanacimiento"),col("fechadefuncion").alias("fechadefuncion"),col("Pk_estadocivil").alias("Pk_estadocivil"), \
                                                                                                   col("Pk_tiporeligion").alias("Pk_tiporeligion"),col("Pk_tipoocupacion").alias("Pk_tipoocupacion"),col("Pk_sexo").alias("Pk_sexo"), \
                                                                                                   col("direccion").alias("direccion"),col("urbanizacion").alias("urbanizacion"),col("ciudad").alias("ciudad"), \
                                                                                                   col("distrito").alias("distrito"),col("provincia").alias("provincia"),col("referenciadireccion").alias("referenciadireccion"), \
                                                                                                   col("lugartrabajo").alias("lugartrabajo"),col("direcciontrabajo").alias("direcciontrabajo"),col("correotrabajo").alias("correotrabajo"), \
                                                                                                   col("nrotelefonotrabajo").alias("nrotelefonotrabajo"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_ESTADO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CONTRATO_ESTADO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Estado".lower())).collect()[0][0] or 0

    df_contrato_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_ESTADO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_contrato_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "left_anti"
    ).withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)") + max_pk ).select(col("Pk_Estado").alias("Pk_Estado"), df_contrato_estado["Cod_Estado"].alias("Cod_Estado"), df_contrato_estado["Descripcion"].alias("Estado"))
    # Inner join para actualizaciones
    updates = df_contrato_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "inner"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), df_contrato_estado["Descripcion"].alias("Estado"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_contrato_estado, 
        ["Cod_Estado"], 
        "left_anti"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), target_df["Estado"].alias("Estado"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_ESTADO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_contrato_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_ESTADO}""") 
    source_df = df_contrato_estado.withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)")).select(col("Pk_Estado").alias("Pk_Estado"), col("Cod_Estado").alias("Cod_Estado"), col("Descripcion").alias("Estado"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_ESTADO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CONTRATO_TIPO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Tipo".lower())).collect()[0][0] or 0

    df_contrato_tipo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_contrato_tipo.join(
        target_df, 
        ["Cod_Tipo"], 
        "left_anti"
    ).withColumn(f"Pk_Tipo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Tipo)") + max_pk ).select(col("Pk_Tipo").alias("Pk_Tipo"), df_contrato_tipo["Cod_Tipo"].alias("Cod_Tipo"), df_contrato_tipo["Descripcion"].alias("Tipo"))
    # Inner join para actualizaciones
    updates = df_contrato_tipo.join(
        target_df, 
        ["Cod_Tipo"], 
        "inner"
    ).select(target_df["Pk_Tipo"].alias("Pk_Tipo"), target_df["Cod_Tipo"].alias("Cod_Tipo"), df_contrato_tipo["Descripcion"].alias("Tipo"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_contrato_tipo, 
        ["Cod_Tipo"], 
        "left_anti"
    ).select(target_df["Pk_Tipo"].alias("Pk_Tipo"), target_df["Cod_Tipo"].alias("Cod_Tipo"), target_df["Tipo"].alias("Tipo"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_contrato_tipo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPO}""") 
    source_df = df_contrato_tipo.withColumn(f"Pk_Tipo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Tipo)")).select(col("Pk_Tipo").alias("Pk_Tipo"), col("Cod_Tipo").alias("Cod_Tipo"), col("Descripcion").alias("Tipo"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoClasificacion".lower())).collect()[0][0] or 0

    df_contrato_tipoclasificacion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPOCLASIFICACION}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_contrato_tipoclasificacion.join(
        target_df, 
        ["Cod_TipoClasificacion"], 
        "left_anti"
    ).withColumn(f"Pk_TipoClasificacion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoClasificacion)") + max_pk ).select(col("Pk_TipoClasificacion").alias("Pk_TipoClasificacion"), df_contrato_tipoclasificacion["Cod_TipoClasificacion"].alias("Cod_TipoClasificacion"), df_contrato_tipoclasificacion["Descripcion"].alias("TipoClasificacion"))
    # Inner join para actualizaciones
    updates = df_contrato_tipoclasificacion.join(
        target_df, 
        ["Cod_TipoClasificacion"], 
        "inner"
    ).select(target_df["Pk_TipoClasificacion"].alias("Pk_TipoClasificacion"), target_df["Cod_TipoClasificacion"].alias("Cod_TipoClasificacion"), df_contrato_tipoclasificacion["Descripcion"].alias("TipoClasificacion"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_contrato_tipoclasificacion, 
        ["Cod_TipoClasificacion"], 
        "left_anti"
    ).select(target_df["Pk_TipoClasificacion"].alias("Pk_TipoClasificacion"), target_df["Cod_TipoClasificacion"].alias("Cod_TipoClasificacion"), target_df["TipoClasificacion"].alias("TipoClasificacion"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_contrato_tipoclasificacion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPOCLASIFICACION}""") 
    source_df = df_contrato_tipoclasificacion.withColumn(f"Pk_TipoClasificacion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoClasificacion)")).select(col("Pk_TipoClasificacion").alias("Pk_TipoClasificacion"), col("Cod_TipoClasificacion").alias("Cod_TipoClasificacion"), col("Descripcion").alias("TipoClasificacion"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoEspacio".lower())).collect()[0][0] or 0

    df_contrato_tipoespacio = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPOESPACIO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_contrato_tipoespacio.join(
        target_df, 
        ["Cod_TipoEspacio"], 
        "left_anti"
    ).withColumn(f"Pk_TipoEspacio", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoEspacio)") + max_pk ).select(col("Pk_TipoEspacio").alias("Pk_TipoEspacio"), df_contrato_tipoespacio["Cod_TipoEspacio"].alias("Cod_TipoEspacio"), df_contrato_tipoespacio["Descripcion"].alias("TipoEspacio"))
    # Inner join para actualizaciones
    updates = df_contrato_tipoespacio.join(
        target_df, 
        ["Cod_TipoEspacio"], 
        "inner"
    ).select(target_df["Pk_TipoEspacio"].alias("Pk_TipoEspacio"), target_df["Cod_TipoEspacio"].alias("Cod_TipoEspacio"), df_contrato_tipoespacio["Descripcion"].alias("TipoEspacio"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_contrato_tipoespacio, 
        ["Cod_TipoEspacio"], 
        "left_anti"
    ).select(target_df["Pk_TipoEspacio"].alias("Pk_TipoEspacio"), target_df["Cod_TipoEspacio"].alias("Cod_TipoEspacio"), target_df["TipoEspacio"].alias("TipoEspacio"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_contrato_tipoespacio = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPOESPACIO}""") 
    source_df = df_contrato_tipoespacio.withColumn(f"Pk_TipoEspacio", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoEspacio)")).select(col("Pk_TipoEspacio").alias("Pk_TipoEspacio"), col("Cod_TipoEspacio").alias("Cod_TipoEspacio"), col("Descripcion").alias("TipoEspacio"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoMoneda".lower())).collect()[0][0] or 0

    df_contrato_tipomoneda = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPOMONEDA}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_contrato_tipomoneda.join(
        target_df, 
        ["Cod_TipoMoneda"], 
        "left_anti"
    ).withColumn(f"Pk_TipoMoneda", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoMoneda)") + max_pk ).select(col("Pk_TipoMoneda").alias("Pk_TipoMoneda"), df_contrato_tipomoneda["Cod_TipoMoneda"].alias("Cod_TipoMoneda"), df_contrato_tipomoneda["Descripcion"].alias("TipoMoneda"))
    # Inner join para actualizaciones
    updates = df_contrato_tipomoneda.join(
        target_df, 
        ["Cod_TipoMoneda"], 
        "inner"
    ).select(target_df["Pk_TipoMoneda"].alias("Pk_TipoMoneda"), target_df["Cod_TipoMoneda"].alias("Cod_TipoMoneda"), df_contrato_tipomoneda["Descripcion"].alias("TipoMoneda"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_contrato_tipomoneda, 
        ["Cod_TipoMoneda"], 
        "left_anti"
    ).select(target_df["Pk_TipoMoneda"].alias("Pk_TipoMoneda"), target_df["Cod_TipoMoneda"].alias("Cod_TipoMoneda"), target_df["TipoMoneda"].alias("TipoMoneda"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_contrato_tipomoneda = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPOMONEDA}""") 
    source_df = df_contrato_tipomoneda.withColumn(f"Pk_TipoMoneda", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoMoneda)")).select(col("Pk_TipoMoneda").alias("Pk_TipoMoneda"), col("Cod_TipoMoneda").alias("Cod_TipoMoneda"), col("Descripcion").alias("TipoMoneda"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoServicio".lower())).collect()[0][0] or 0

    df_contrato_tiposervicio = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPOSERVICIO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_contrato_tiposervicio.join(
        target_df, 
        ["Cod_TipoServicio"], 
        "left_anti"
    ).withColumn(f"Pk_TipoServicio", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoServicio)") + max_pk ).select(col("Pk_TipoServicio").alias("Pk_TipoServicio"), df_contrato_tiposervicio["Cod_TipoServicio"].alias("Cod_TipoServicio"), df_contrato_tiposervicio["Descripcion"].alias("TipoServicio"))
    # Inner join para actualizaciones
    updates = df_contrato_tiposervicio.join(
        target_df, 
        ["Cod_TipoServicio"], 
        "inner"
    ).select(target_df["Pk_TipoServicio"].alias("Pk_TipoServicio"), target_df["Cod_TipoServicio"].alias("Cod_TipoServicio"), df_contrato_tiposervicio["Descripcion"].alias("TipoServicio"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_contrato_tiposervicio, 
        ["Cod_TipoServicio"], 
        "left_anti"
    ).select(target_df["Pk_TipoServicio"].alias("Pk_TipoServicio"), target_df["Cod_TipoServicio"].alias("Cod_TipoServicio"), target_df["TipoServicio"].alias("TipoServicio"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_contrato_tiposervicio = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO_TIPOSERVICIO}""") 
    source_df = df_contrato_tiposervicio.withColumn(f"Pk_TipoServicio", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoServicio)")).select(col("Pk_TipoServicio").alias("Pk_TipoServicio"), col("Cod_TipoServicio").alias("Cod_TipoServicio"), col("Descripcion").alias("TipoServicio"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CONTRATO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Contrato".lower())).collect()[0][0] or 0

    df_contrato = spark.sql(f"""
    select          
      A.cod_contrato        
     ,A.nrocontrato         
     ,B.pk_cliente          
     ,C.pk_estado           
     ,D.pk_tipo             
     ,E.pk_tiposervicio     
     ,A.capacidad           
     ,A.actualcapacidad     
     ,A.nrousos             
     ,F.pk_tipomoneda       
     ,A.preciolista         
     ,A.descuento           
     ,A.descuentoadi        
     ,A.descuentoprom1      
     ,A.descuentoprom2      
     ,A.anticipo            
     ,A.fechafirma          
     ,A.usuarioregistro     
     ,A.fechaventa          
     ,G.pk_tipoespacio      
     ,A.flagperiodocarencia 
     ,H.pk_tipoclasificacion
     ,A.diascarencia        
     ,A.fechaanulacion
    from {bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO} A 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE} B on A.cod_cliente=B.cod_cliente 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_ESTADO} C on A.cod_estado=C.cod_estado 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPO} D on A.cod_tipo=D.cod_tipo
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO} E on A.cod_tiposervicio=E.cod_tiposervicio
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA} F on A.cod_tipomoneda=F.cod_tipomoneda
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO} G on A.cod_tipoespacio=G.cod_tipoespacio
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION} H on A.cod_tipoclasificacion=H.cod_tipoclasificacion
    union all
    select 0,0,166102,7,3,4,0,0,0,2,0.0,0.0,0.0,0.0,0.0,0.0,'','','',1,false,2,0,'' """) 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_contrato.join(
        target_df, 
        ["Cod_Contrato"], 
        "left_anti"
    ).withColumn(f"Pk_Contrato", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Contrato)") + max_pk ).select(col("Pk_Contrato").alias("Pk_Contrato"), df_contrato["cod_contrato"].alias("cod_contrato"), df_contrato["nrocontrato"].alias("nrocontrato"), \
                                                                                                   df_contrato["pk_cliente"].alias("pk_cliente"),df_contrato["pk_estado"].alias("pk_estado"),df_contrato["pk_tipo"].alias("pk_tipo"), \
                                                                                                   df_contrato["pk_tiposervicio"].alias("pk_tiposervicio"),df_contrato["capacidad"].alias("capacidad"),df_contrato["actualcapacidad"].alias("actualcapacidad"), \
                                                                                                   df_contrato["nrousos"].alias("nrousos"),df_contrato["pk_tipomoneda"].alias("pk_tipomoneda"),df_contrato["preciolista"].alias("preciolista"), \
                                                                                                   df_contrato["descuento"].alias("descuento"),df_contrato["descuentoadi"].alias("descuentoadi"),df_contrato["descuentoprom1"].alias("descuentoprom1"), \
                                                                                                   df_contrato["descuentoprom2"].alias("descuentoprom2"),df_contrato["anticipo"].alias("anticipo"),df_contrato["fechafirma"].alias("fechafirma"), \
                                                                                                   df_contrato["usuarioregistro"].alias("usuarioregistro"),df_contrato["fechaventa"].alias("fechaventa"),df_contrato["pk_tipoespacio"].alias("pk_tipoespacio"), \
                                                                                                   df_contrato["flagperiodocarencia"].alias("flagperiodocarencia"),df_contrato["pk_tipoclasificacion"].alias("pk_tipoclasificacion"),df_contrato["diascarencia"].alias("diascarencia"), \
                                                                                                   df_contrato["fechaanulacion"].alias("fechaanulacion"))
    # Inner join para actualizaciones
    updates = df_contrato.join(
        target_df, 
        ["Cod_Contrato"], 
        "inner"
    ).select(target_df["Pk_Contrato"].alias("Pk_Contrato"), target_df["Cod_Contrato"].alias("Cod_Contrato"), df_contrato["nrocontrato"].alias("nrocontrato"), \
                                                                                                   df_contrato["pk_cliente"].alias("pk_cliente"),df_contrato["pk_estado"].alias("pk_estado"),df_contrato["pk_tipo"].alias("pk_tipo"), \
                                                                                                   df_contrato["pk_tiposervicio"].alias("pk_tiposervicio"),df_contrato["capacidad"].alias("capacidad"),df_contrato["actualcapacidad"].alias("actualcapacidad"), \
                                                                                                   df_contrato["nrousos"].alias("nrousos"),df_contrato["pk_tipomoneda"].alias("pk_tipomoneda"),df_contrato["preciolista"].alias("preciolista"), \
                                                                                                   df_contrato["descuento"].alias("descuento"),df_contrato["descuentoadi"].alias("descuentoadi"),df_contrato["descuentoprom1"].alias("descuentoprom1"), \
                                                                                                   df_contrato["descuentoprom2"].alias("descuentoprom2"),df_contrato["anticipo"].alias("anticipo"),df_contrato["fechafirma"].alias("fechafirma"), \
                                                                                                   df_contrato["usuarioregistro"].alias("usuarioregistro"),df_contrato["fechaventa"].alias("fechaventa"),df_contrato["pk_tipoespacio"].alias("pk_tipoespacio"), \
                                                                                                   df_contrato["flagperiodocarencia"].alias("flagperiodocarencia"),df_contrato["pk_tipoclasificacion"].alias("pk_tipoclasificacion"),df_contrato["diascarencia"].alias("diascarencia"), \
                                                                                                   df_contrato["fechaanulacion"].alias("fechaanulacion"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_contrato, 
        ["Cod_Contrato"], 
        "left_anti"
    ).select(target_df["Pk_Contrato"].alias("Pk_Contrato"), target_df["Cod_Contrato"].alias("Cod_Contrato"), target_df["nrocontrato"].alias("nrocontrato"), \
                                                                                                   target_df["pk_cliente"].alias("pk_cliente"),target_df["pk_estado"].alias("pk_estado"),target_df["pk_tipo"].alias("pk_tipo"), \
                                                                                                   target_df["pk_tiposervicio"].alias("pk_tiposervicio"),target_df["capacidad"].alias("capacidad"),target_df["actualcapacidad"].alias("actualcapacidad"), \
                                                                                                   target_df["nrousos"].alias("nrousos"),target_df["pk_tipomoneda"].alias("pk_tipomoneda"),target_df["preciolista"].alias("preciolista"), \
                                                                                                   target_df["descuento"].alias("descuento"),target_df["descuentoadi"].alias("descuentoadi"),target_df["descuentoprom1"].alias("descuentoprom1"), \
                                                                                                   target_df["descuentoprom2"].alias("descuentoprom2"),target_df["anticipo"].alias("anticipo"),target_df["fechafirma"].alias("fechafirma"), \
                                                                                                   target_df["usuarioregistro"].alias("usuarioregistro"),target_df["fechaventa"].alias("fechaventa"),target_df["pk_tipoespacio"].alias("pk_tipoespacio"), \
                                                                                                   target_df["flagperiodocarencia"].alias("flagperiodocarencia"),target_df["pk_tipoclasificacion"].alias("pk_tipoclasificacion"),target_df["diascarencia"].alias("diascarencia"), \
                                                                                                   target_df["fechaanulacion"].alias("fechaanulacion"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_contrato = spark.sql(f"""
    select 
      A.cod_contrato        
     ,A.nrocontrato         
     ,B.pk_cliente          
     ,C.pk_estado           
     ,D.pk_tipo             
     ,E.pk_tiposervicio     
     ,A.capacidad           
     ,A.actualcapacidad     
     ,A.nrousos             
     ,F.pk_tipomoneda       
     ,A.preciolista         
     ,A.descuento           
     ,A.descuentoadi        
     ,A.descuentoprom1      
     ,A.descuentoprom2      
     ,A.anticipo            
     ,A.fechafirma          
     ,A.usuarioregistro     
     ,A.fechaventa          
     ,G.pk_tipoespacio      
     ,A.flagperiodocarencia 
     ,H.pk_tipoclasificacion
     ,A.diascarencia        
     ,A.fechaanulacion
    from {bdawscobsi}.{CONST_INPUT_TABLA_CONTRATO} A 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE} B on A.cod_cliente=B.cod_cliente 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_ESTADO} C on A.cod_estado=C.cod_estado 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPO} D on A.cod_tipo=D.cod_tipo
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO} E on A.cod_tiposervicio=E.cod_tiposervicio
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA} F on A.cod_tipomoneda=F.cod_tipomoneda
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO} G on A.cod_tipoespacio=G.cod_tipoespacio
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION} H on A.cod_tipoclasificacion=H.cod_tipoclasificacion
    union all
    select 0,0,166102,7,3,4,0,0,0,2,0.0,0.0,0.0,0.0,0.0,0.0,'','','',1,false,2,0,'' """) 
    source_df = df_contrato.withColumn(f"Pk_Contrato", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Contrato)")).select(col("Pk_Contrato").alias("Pk_Contrato"), col("Cod_Contrato").alias("Cod_Contrato"), col("nrocontrato").alias("nrocontrato"), \
                                                                                                   col("pk_cliente").alias("pk_cliente"),col("pk_estado").alias("pk_estado"),col("pk_tipo").alias("pk_tipo"), \
                                                                                                   col("pk_tiposervicio").alias("pk_tiposervicio"),col("capacidad").alias("capacidad"),col("actualcapacidad").alias("actualcapacidad"), \
                                                                                                   col("nrousos").alias("nrousos"),col("pk_tipomoneda").alias("pk_tipomoneda"),col("preciolista").alias("preciolista"), \
                                                                                                   col("descuento").alias("descuento"),col("descuentoadi").alias("descuentoadi"),col("descuentoprom1").alias("descuentoprom1"), \
                                                                                                   col("descuentoprom2").alias("descuentoprom2"),col("anticipo").alias("anticipo"),col("fechafirma").alias("fechafirma"), \
                                                                                                   col("usuarioregistro").alias("usuarioregistro"),col("fechaventa").alias("fechaventa"),col("pk_tipoespacio").alias("pk_tipoespacio"), \
                                                                                                   col("flagperiodocarencia").alias("flagperiodocarencia"),col("pk_tipoclasificacion").alias("pk_tipoclasificacion"),col("diascarencia").alias("diascarencia"), \
                                                                                                   col("fechaanulacion").alias("fechaanulacion"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Estado".lower())).collect()[0][0] or 0

    df_cronograma_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CRONOGRAMA_ESTADO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_cronograma_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "left_anti"
    ).withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)") + max_pk ).select(col("Pk_Estado").alias("Pk_Estado"), df_cronograma_estado["Cod_Estado"].alias("Cod_Estado"), df_cronograma_estado["Descripcion"].alias("Estado"))
    # Inner join para actualizaciones
    updates = df_cronograma_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "inner"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), df_cronograma_estado["Descripcion"].alias("Estado"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_cronograma_estado, 
        ["Cod_Estado"], 
        "left_anti"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), target_df["Estado"].alias("Estado"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_cronograma_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_CRONOGRAMA_ESTADO}""") 
    source_df = df_cronograma_estado.withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)")).select(col("Pk_Estado").alias("Pk_Estado"), col("Cod_Estado").alias("Cod_Estado"), col("Descripcion").alias("Estado"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_ModoPago".lower())).collect()[0][0] or 0

    df_financiamiento_modopago = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_FINANCIAMIENTO_MODOPAGO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_financiamiento_modopago.join(
        target_df, 
        ["Cod_ModoPago"], 
        "left_anti"
    ).withColumn(f"Pk_ModoPago", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_ModoPago)") + max_pk ).select(col("Pk_ModoPago").alias("Pk_ModoPago"), df_financiamiento_modopago["Cod_ModoPago"].alias("Cod_ModoPago"), df_financiamiento_modopago["Descripcion"].alias("ModoPago"))
    # Inner join para actualizaciones
    updates = df_financiamiento_modopago.join(
        target_df, 
        ["Cod_ModoPago"], 
        "inner"
    ).select(target_df["Pk_ModoPago"].alias("Pk_ModoPago"), target_df["Cod_ModoPago"].alias("Cod_ModoPago"), df_financiamiento_modopago["Descripcion"].alias("ModoPago"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_financiamiento_modopago, 
        ["Cod_ModoPago"], 
        "left_anti"
    ).select(target_df["Pk_ModoPago"].alias("Pk_ModoPago"), target_df["Cod_ModoPago"].alias("Cod_ModoPago"), target_df["ModoPago"].alias("ModoPago"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_financiamiento_modopago = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_FINANCIAMIENTO_MODOPAGO}""") 
    source_df = df_financiamiento_modopago.withColumn(f"Pk_ModoPago", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_ModoPago)")).select(col("Pk_ModoPago").alias("Pk_ModoPago"), col("Cod_ModoPago").alias("Cod_ModoPago"), col("Descripcion").alias("ModoPago"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_FINANCIAMIENTO_ESTADO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_FINANCIAMIENTO_ESTADO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Estado".lower())).collect()[0][0] or 0

    df_financiamiento_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_FINANCIAMIENTO_ESTADO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_financiamiento_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "left_anti"
    ).withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)") + max_pk ).select(col("Pk_Estado").alias("Pk_Estado"), df_financiamiento_estado["Cod_Estado"].alias("Cod_Estado"), df_financiamiento_estado["Descripcion"].alias("Estado"))
    # Inner join para actualizaciones
    updates = df_financiamiento_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "inner"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), df_financiamiento_estado["Descripcion"].alias("Estado"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_financiamiento_estado, 
        ["Cod_Estado"], 
        "left_anti"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), target_df["Estado"].alias("Estado"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_ESTADO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_financiamiento_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_FINANCIAMIENTO_ESTADO}""") 
    source_df = df_financiamiento_estado.withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)")).select(col("Pk_Estado").alias("Pk_Estado"), col("Cod_Estado").alias("Cod_Estado"), col("Descripcion").alias("Estado"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_ESTADO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Motivo".lower())).collect()[0][0] or 0

    df_liquidacion_motivo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_LIQUIDACION_MOTIVO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_liquidacion_motivo.join(
        target_df, 
        ["Cod_Motivo"], 
        "left_anti"
    ).withColumn(f"Pk_Motivo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Motivo)") + max_pk ).select(col("Pk_Motivo").alias("Pk_Motivo"), df_liquidacion_motivo["Cod_Motivo"].alias("Cod_Motivo"), df_liquidacion_motivo["Descripcion"].alias("Motivo"))
    # Inner join para actualizaciones
    updates = df_liquidacion_motivo.join(
        target_df, 
        ["Cod_Motivo"], 
        "inner"
    ).select(target_df["Pk_Motivo"].alias("Pk_Motivo"), target_df["Cod_Motivo"].alias("Cod_Motivo"), df_liquidacion_motivo["Descripcion"].alias("Motivo"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_liquidacion_motivo, 
        ["Cod_Motivo"], 
        "left_anti"
    ).select(target_df["Pk_Motivo"].alias("Pk_Motivo"), target_df["Cod_Motivo"].alias("Cod_Motivo"), target_df["Motivo"].alias("Motivo"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_liquidacion_motivo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_LIQUIDACION_MOTIVO}""") 
    source_df = df_liquidacion_motivo.withColumn(f"Pk_Motivo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Motivo)")).select(col("Pk_Motivo").alias("Pk_Motivo"), col("Cod_Motivo").alias("Cod_Motivo"), col("Descripcion").alias("Motivo"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Representante".lower())).collect()[0][0] or 0

    df_liquidacion_representante = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_LIQUIDACION_REPRESENTANTE}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_liquidacion_representante.join(
        target_df, 
        ["Cod_Representante"], 
        "left_anti"
    ).withColumn(f"Pk_Representante", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Representante)") + max_pk ).select(col("Pk_Representante").alias("Pk_Representante"), df_liquidacion_representante["Cod_Representante"].alias("Cod_Representante"), df_liquidacion_representante["Descripcion"].alias("Representante"))
    # Inner join para actualizaciones
    updates = df_liquidacion_representante.join(
        target_df, 
        ["Cod_Representante"], 
        "inner"
    ).select(target_df["Pk_Representante"].alias("Pk_Representante"), target_df["Cod_Representante"].alias("Cod_Representante"), df_liquidacion_representante["Descripcion"].alias("Representante"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_liquidacion_representante, 
        ["Cod_Representante"], 
        "left_anti"
    ).select(target_df["Pk_Representante"].alias("Pk_Representante"), target_df["Cod_Representante"].alias("Cod_Representante"), target_df["Representante"].alias("Representante"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_liquidacion_representante = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_LIQUIDACION_REPRESENTANTE}""") 
    source_df = df_liquidacion_representante.withColumn(f"Pk_Representante", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Representante)")).select(col("Pk_Representante").alias("Pk_Representante"), col("Cod_Representante").alias("Cod_Representante"), col("Descripcion").alias("Representante"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoLiquidacion".lower())).collect()[0][0] or 0

    df_liquidacion_tipoliquidacion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_liquidacion_tipoliquidacion.join(
        target_df, 
        ["Cod_TipoLiquidacion"], 
        "left_anti"
    ).withColumn(f"Pk_TipoLiquidacion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoLiquidacion)") + max_pk ).select(col("Pk_TipoLiquidacion").alias("Pk_TipoLiquidacion"), df_liquidacion_tipoliquidacion["Cod_TipoLiquidacion"].alias("Cod_TipoLiquidacion"), df_liquidacion_tipoliquidacion["Descripcion"].alias("TipoLiquidacion"))
    # Inner join para actualizaciones
    updates = df_liquidacion_tipoliquidacion.join(
        target_df, 
        ["Cod_TipoLiquidacion"], 
        "inner"
    ).select(target_df["Pk_TipoLiquidacion"].alias("Pk_TipoLiquidacion"), target_df["Cod_TipoLiquidacion"].alias("Cod_TipoLiquidacion"), df_liquidacion_tipoliquidacion["Descripcion"].alias("TipoLiquidacion"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_liquidacion_tipoliquidacion, 
        ["Cod_TipoLiquidacion"], 
        "left_anti"
    ).select(target_df["Pk_TipoLiquidacion"].alias("Pk_TipoLiquidacion"), target_df["Cod_TipoLiquidacion"].alias("Cod_TipoLiquidacion"), target_df["TipoLiquidacion"].alias("TipoLiquidacion"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_liquidacion_tipoliquidacion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION}""") 
    source_df = df_liquidacion_tipoliquidacion.withColumn(f"Pk_TipoLiquidacion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoLiquidacion)")).select(col("Pk_TipoLiquidacion").alias("Pk_TipoLiquidacion"), col("Cod_TipoLiquidacion").alias("Cod_TipoLiquidacion"), col("Descripcion").alias("TipoLiquidacion"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_PAGOS_ESTADO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_PAGOS_ESTADO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Estado".lower())).collect()[0][0] or 0

    df_pagos_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PAGOS_ESTADO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_pagos_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "left_anti"
    ).withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)") + max_pk ).select(col("Pk_Estado").alias("Pk_Estado"), df_pagos_estado["Cod_Estado"].alias("Cod_Estado"), df_pagos_estado["Descripcion"].alias("Estado"))
    # Inner join para actualizaciones
    updates = df_pagos_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "inner"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), df_pagos_estado["Descripcion"].alias("Estado"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_pagos_estado, 
        ["Cod_Estado"], 
        "left_anti"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), target_df["Estado"].alias("Estado"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_ESTADO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_pagos_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PAGOS_ESTADO}""") 
    source_df = df_pagos_estado.withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)")).select(col("Pk_Estado").alias("Pk_Estado"), col("Cod_Estado").alias("Cod_Estado"), col("Descripcion").alias("Estado"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_ESTADO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoComprobante".lower())).collect()[0][0] or 0

    df_pagos_tipocomprobante = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PAGOS_TIPOCOMPROBANTE}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_pagos_tipocomprobante.join(
        target_df, 
        ["Cod_TipoComprobante"], 
        "left_anti"
    ).withColumn(f"Pk_TipoComprobante", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoComprobante)") + max_pk ).select(col("Pk_TipoComprobante").alias("Pk_TipoComprobante"), df_pagos_tipocomprobante["Cod_TipoComprobante"].alias("Cod_TipoComprobante"), df_pagos_tipocomprobante["Descripcion"].alias("TipoComprobante"))
    # Inner join para actualizaciones
    updates = df_pagos_tipocomprobante.join(
        target_df, 
        ["Cod_TipoComprobante"], 
        "inner"
    ).select(target_df["Pk_TipoComprobante"].alias("Pk_TipoComprobante"), target_df["Cod_TipoComprobante"].alias("Cod_TipoComprobante"), df_pagos_tipocomprobante["Descripcion"].alias("TipoComprobante"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_pagos_tipocomprobante, 
        ["Cod_TipoComprobante"], 
        "left_anti"
    ).select(target_df["Pk_TipoComprobante"].alias("Pk_TipoComprobante"), target_df["Cod_TipoComprobante"].alias("Cod_TipoComprobante"), target_df["TipoComprobante"].alias("TipoComprobante"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_pagos_tipocomprobante = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PAGOS_TIPOCOMPROBANTE}""") 
    source_df = df_pagos_tipocomprobante.withColumn(f"Pk_TipoComprobante", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoComprobante)")).select(col("Pk_TipoComprobante").alias("Pk_TipoComprobante"), col("Cod_TipoComprobante").alias("Cod_TipoComprobante"), col("Descripcion").alias("TipoComprobante"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_PAQUETE_TIPO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_PAQUETE_TIPO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Tipo".lower())).collect()[0][0] or 0

    df_paquete_tipo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PAQUETE_TIPO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_paquete_tipo.join(
        target_df, 
        ["Cod_Tipo"], 
        "left_anti"
    ).withColumn(f"Pk_Tipo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Tipo)") + max_pk ).select(col("Pk_Tipo").alias("Pk_Tipo"), df_paquete_tipo["Cod_Tipo"].alias("Cod_Tipo"), df_paquete_tipo["Descripcion"].alias("Tipo"))
    # Inner join para actualizaciones
    updates = df_paquete_tipo.join(
        target_df, 
        ["Cod_Tipo"], 
        "inner"
    ).select(target_df["Pk_Tipo"].alias("Pk_Tipo"), target_df["Cod_Tipo"].alias("Cod_Tipo"), df_paquete_tipo["Descripcion"].alias("Tipo"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_paquete_tipo, 
        ["Cod_Tipo"], 
        "left_anti"
    ).select(target_df["Pk_Tipo"].alias("Pk_Tipo"), target_df["Cod_Tipo"].alias("Cod_Tipo"), target_df["Tipo"].alias("Tipo"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_paquete_tipo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PAQUETE_TIPO}""") 
    source_df = df_paquete_tipo.withColumn(f"Pk_Tipo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Tipo)")).select(col("Pk_Tipo").alias("Pk_Tipo"), col("Cod_Tipo").alias("Cod_Tipo"), col("Descripcion").alias("Tipo"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoServicio".lower())).collect()[0][0] or 0

    df_paquete_tiposervicio = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PAQUETE_TIPOSERVICIO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_paquete_tiposervicio.join(
        target_df, 
        ["Cod_TipoServicio"], 
        "left_anti"
    ).withColumn(f"Pk_TipoServicio", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoServicio)") + max_pk ).select(col("Pk_TipoServicio").alias("Pk_TipoServicio"), df_paquete_tiposervicio["Cod_TipoServicio"].alias("Cod_TipoServicio"), df_paquete_tiposervicio["Descripcion"].alias("TipoServicio"))
    # Inner join para actualizaciones
    updates = df_paquete_tiposervicio.join(
        target_df, 
        ["Cod_TipoServicio"], 
        "inner"
    ).select(target_df["Pk_TipoServicio"].alias("Pk_TipoServicio"), target_df["Cod_TipoServicio"].alias("Cod_TipoServicio"), df_paquete_tiposervicio["Descripcion"].alias("TipoServicio"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_paquete_tiposervicio, 
        ["Cod_TipoServicio"], 
        "left_anti"
    ).select(target_df["Pk_TipoServicio"].alias("Pk_TipoServicio"), target_df["Cod_TipoServicio"].alias("Cod_TipoServicio"), target_df["TipoServicio"].alias("TipoServicio"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_paquete_tiposervicio = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PAQUETE_TIPOSERVICIO}""") 
    source_df = df_paquete_tiposervicio.withColumn(f"Pk_TipoServicio", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoServicio)")).select(col("Pk_TipoServicio").alias("Pk_TipoServicio"), col("Cod_TipoServicio").alias("Cod_TipoServicio"), col("Descripcion").alias("TipoServicio"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_FINANCIAMIENTO_PRODUCTO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_FINANCIAMIENTO_PRODUCTO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    #max_pk = target_df.agg(F.max(f"Pk_PaqueteProducto".lower())).collect()[0][0] or 0

    df_paquete_producto = spark.sql(f"""
    Select 
     Cod_PaqueteProducto as Pk_PaqueteProducto
    ,Cod_Producto
    ,NombreProducto
    ,Cantidad
    ,Precio
    ,Monto
    ,Tipo
    from
    {bdawscobsi}.{CONST_INPUT_TABLA_FINANCIAMIENTO_PRODUCTO} order by 1""") 

    
    df_paquete_producto.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_PRODUCTO}")
    print('Crea y Carga tabla FT_FINANCIAMIENTO_PRODUCTO')

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_paquete_producto = spark.sql(f"""Select 
    Cod_PaqueteProducto as Pk_PaqueteProducto
    ,Cod_Producto
    ,NombreProducto
    ,Cantidad
    ,Precio
    ,Monto
    ,Tipo
    from
    {bdawscobsi}.{CONST_INPUT_TABLA_FINANCIAMIENTO_PRODUCTO} order by 1""") 
   
    final_df = df_paquete_producto
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_PRODUCTO}")
    print('Crea y Carga tabla FT_FINANCIAMIENTO_PRODUCTO')
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_PAQUETE}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_PAQUETE}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Paquete".lower())).collect()[0][0] or 0

    df_paquete = spark.sql(f"""select 
         A.cod_paquete         
        ,A.descripcion         
        ,B.pk_tipo            
        ,C.pk_tiposervicio    
        ,A.precioventa         
        ,A.preciolista         
        ,A.flagactivo    
        from 
        {bdawscobsi}.{CONST_INPUT_TABLA_PAQUETE} A INNER JOIN 
        {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPO} B ON A.cod_tipo = B.cod_tipo INNER JOIN
        {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO} C ON A.cod_tiposervicio = C.cod_tiposervicio """) 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_paquete.join(
        target_df, 
        ["Cod_Paquete"], 
        "left_anti"
    ).withColumn(f"Pk_Paquete", expr(f"ROW_NUMBER() OVER (ORDER BY cod_paquete)") + max_pk ).select(col("Pk_Paquete").alias("Pk_Paquete"), df_paquete["Cod_Paquete"].alias("Cod_Paquete"), df_paquete["Descripcion"].alias("Descripcion"), df_paquete["Pk_Tipo"].alias("Pk_Tipo"), \
                                                                                                   df_paquete["Pk_TipoServicio"].alias("Pk_TipoServicio"), df_paquete["PrecioVenta"].alias("PrecioVenta"), df_paquete["PrecioLista"].alias("PrecioLista"),df_paquete["FlagActivo"].alias("FlagActivo"))
    # Inner join para actualizaciones
    updates = df_paquete.join(
        target_df, 
        ["Cod_Paquete"], 
        "inner"
    ).select(target_df["Pk_Paquete"].alias("Pk_Paquete"), target_df["Cod_Paquete"].alias("Cod_Paquete"), df_paquete["Descripcion"].alias("Descripcion"), df_paquete["Pk_Tipo"].alias("Pk_Tipo"), \
            df_paquete["Pk_TipoServicio"].alias("Pk_TipoServicio"), df_paquete["PrecioVenta"].alias("PrecioVenta"), df_paquete["PrecioLista"].alias("PrecioLista"),df_paquete["FlagActivo"].alias("FlagActivo"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_paquete, 
        ["Cod_Paquete"], 
        "left_anti"
    ).select(target_df["Pk_Paquete"].alias("Pk_Paquete"), target_df["Cod_Paquete"].alias("Cod_Paquete"), target_df["Descripcion"].alias("Descripcion"), target_df["Pk_Tipo"].alias("Pk_Tipo"), \
            target_df["Pk_TipoServicio"].alias("Pk_TipoServicio"), target_df["PrecioVenta"].alias("PrecioVenta"), target_df["PrecioLista"].alias("PrecioLista"),target_df["FlagActivo"].alias("FlagActivo"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_paquete = spark.sql(f"""select 
         A.cod_paquete         
        ,A.descripcion         
        ,B.pk_tipo            
        ,C.pk_tiposervicio    
        ,A.precioventa         
        ,A.preciolista         
        ,A.flagactivo    
        from 
        {bdawscobsi}.{CONST_INPUT_TABLA_PAQUETE} A INNER JOIN 
        {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPO} B ON A.cod_tipo = B.cod_tipo INNER JOIN
        {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO} C ON A.cod_tiposervicio = C.cod_tiposervicio """) 
    source_df = df_paquete.withColumn(f"Pk_Paquete", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Paquete)")).select(col("Pk_Paquete").alias("Pk_Paquete"), col("Cod_Paquete").alias("Cod_Paquete"), col("Descripcion").alias("Descripcion"), \
                                                                                                              col("pk_Tipo").alias("Pk_Tipo"),col("Pk_TipoServicio").alias("Pk_TipoServicio"),col("PrecioVenta").alias("PrecioVenta"),col("PrecioLista").alias("PrecioLista"), \
                                                                                                              col("FlagActivo").alias("FlagActivo"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_PIEDRAS_ESTADO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_PIEDRAS_ESTADO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Estado".lower())).collect()[0][0] or 0

    df_piedras_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PIEDRAS_ESTADO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_piedras_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "left_anti"
    ).withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)") + max_pk ).select(col("Pk_Estado").alias("Pk_Estado"), df_piedras_estado["Cod_Estado"].alias("Cod_Estado"), df_piedras_estado["Descripcion"].alias("Estado"))
    # Inner join para actualizaciones
    updates = df_piedras_estado.join(
        target_df, 
        ["Cod_Estado"], 
        "inner"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), df_piedras_estado["Descripcion"].alias("Estado"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_piedras_estado, 
        ["Cod_Estado"], 
        "left_anti"
    ).select(target_df["Pk_Estado"].alias("Pk_Estado"), target_df["Cod_Estado"].alias("Cod_Estado"), target_df["Estado"].alias("Estado"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_ESTADO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_piedras_estado = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PIEDRAS_ESTADO}""") 
    source_df = df_piedras_estado.withColumn(f"Pk_Estado", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Estado)")).select(col("Pk_Estado").alias("Pk_Estado"), col("Cod_Estado").alias("Cod_Estado"), col("Descripcion").alias("Estado"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_ESTADO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_PIEDRAS_TIPO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_PIEDRAS_TIPO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_Tipo".lower())).collect()[0][0] or 0

    df_piedras_tipo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PIEDRAS_TIPO}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_piedras_tipo.join(
        target_df, 
        ["Cod_Tipo"], 
        "left_anti"
    ).withColumn(f"Pk_Tipo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Tipo)") + max_pk ).select(col("Pk_Tipo").alias("Pk_Tipo"), df_piedras_tipo["Cod_Tipo"].alias("Cod_Tipo"), df_piedras_tipo["Descripcion"].alias("Tipo"))
    # Inner join para actualizaciones
    updates = df_piedras_tipo.join(
        target_df, 
        ["Cod_Tipo"], 
        "inner"
    ).select(target_df["Pk_Tipo"].alias("Pk_Tipo"), target_df["Cod_Tipo"].alias("Cod_Tipo"), df_piedras_tipo["Descripcion"].alias("Tipo"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_piedras_tipo, 
        ["Cod_Tipo"], 
        "left_anti"
    ).select(target_df["Pk_Tipo"].alias("Pk_Tipo"), target_df["Cod_Tipo"].alias("Cod_Tipo"), target_df["Tipo"].alias("Tipo"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPO}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_piedras_tipo = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PIEDRAS_TIPO}""") 
    source_df = df_piedras_tipo.withColumn(f"Pk_Tipo", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_Tipo)")).select(col("Pk_Tipo").alias("Pk_Tipo"), col("Cod_Tipo").alias("Cod_Tipo"), col("Descripcion").alias("Tipo"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPO}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoLiberacion".lower())).collect()[0][0] or 0

    df_piedras_tipoliberacion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PIEDRAS_TIPOLIBERACION}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_piedras_tipoliberacion.join(
        target_df, 
        ["Cod_TipoLiberacion"], 
        "left_anti"
    ).withColumn(f"Pk_TipoLiberacion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoLiberacion)") + max_pk ).select(col("Pk_TipoLiberacion").alias("Pk_TipoLiberacion"), df_piedras_tipoliberacion["Cod_TipoLiberacion"].alias("Cod_TipoLiberacion"), df_piedras_tipoliberacion["Descripcion"].alias("TipoLiberacion"))
    # Inner join para actualizaciones
    updates = df_piedras_tipoliberacion.join(
        target_df, 
        ["Cod_TipoLiberacion"], 
        "inner"
    ).select(target_df["Pk_TipoLiberacion"].alias("Pk_TipoLiberacion"), target_df["Cod_TipoLiberacion"].alias("Cod_TipoLiberacion"), df_piedras_tipoliberacion["Descripcion"].alias("TipoLiberacion"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_piedras_tipoliberacion, 
        ["Cod_TipoLiberacion"], 
        "left_anti"
    ).select(target_df["Pk_TipoLiberacion"].alias("Pk_TipoLiberacion"), target_df["Cod_TipoLiberacion"].alias("Cod_TipoLiberacion"), target_df["TipoLiberacion"].alias("TipoLiberacion"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_piedras_tipoliberacion = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PIEDRAS_TIPOLIBERACION}""") 
    source_df = df_piedras_tipoliberacion.withColumn(f"Pk_TipoLiberacion", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoLiberacion)")).select(col("Pk_TipoLiberacion").alias("Pk_TipoLiberacion"), col("Cod_TipoLiberacion").alias("Cod_TipoLiberacion"), col("Descripcion").alias("TipoLiberacion"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    max_pk = target_df.agg(F.max(f"Pk_TipoOrden".lower())).collect()[0][0] or 0

    df_piedras_tipoorden = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PIEDRAS_TIPOORDEN}""") 
    # Realizar uniones basadas en la clave primaria
    # Anti-join para nuevos registros
    new_records = df_piedras_tipoorden.join(
        target_df, 
        ["Cod_TipoOrden"], 
        "left_anti"
    ).withColumn(f"Pk_TipoOrden", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoOrden)") + max_pk ).select(col("Pk_TipoOrden").alias("Pk_TipoOrden"), df_piedras_tipoorden["Cod_TipoOrden"].alias("Cod_TipoOrden"), df_piedras_tipoorden["Descripcion"].alias("TipoOrden"))
    # Inner join para actualizaciones
    updates = df_piedras_tipoorden.join(
        target_df, 
        ["Cod_TipoOrden"], 
        "inner"
    ).select(target_df["Pk_TipoOrden"].alias("Pk_TipoOrden"), target_df["Cod_TipoOrden"].alias("Cod_TipoOrden"), df_piedras_tipoorden["Descripcion"].alias("TipoOrden"))  # Priorizar Silver
    # Registros que solo están en Gold
    gold_only = target_df.join(
        df_piedras_tipoorden, 
        ["Cod_TipoOrden"], 
        "left_anti"
    ).select(target_df["Pk_TipoOrden"].alias("Pk_TipoOrden"), target_df["Cod_TipoOrden"].alias("Cod_TipoOrden"), target_df["TipoOrden"].alias("TipoOrden"))
    # Union de todos
    final_df = new_records.union(updates).union(gold_only)
    final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    df_temp = spark.read.parquet(path_temp)
    df_temp.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN}")

else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_piedras_tipoorden = spark.table(f"""{bdawscobsi}.{CONST_INPUT_TABLA_PIEDRAS_TIPOORDEN}""") 
    source_df = df_piedras_tipoorden.withColumn(f"Pk_TipoOrden", expr(f"ROW_NUMBER() OVER (ORDER BY Cod_TipoOrden)")).select(col("Pk_TipoOrden").alias("Pk_TipoOrden"), col("Cod_TipoOrden").alias("Cod_TipoOrden"), col("Descripcion").alias("TipoOrden"))
    final_df = source_df
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN}")
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_FT_ADENDAS}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_FT_ADENDAS}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    #max_pk = target_df.agg(F.max(f"Pk_Cliente".lower())).collect()[0][0] or 0

    df_ft_adendas = spark.sql(f"""
    select 
      A.cod_adenda as pk_adenda           
     ,B.pk_contrato         
     ,C.pk_tipo             
     ,D.pk_estado           
     ,E.pk_paquete as pk_paquetenew        
     ,F.pk_paquete          
     ,A.diascarencia        
     ,A.precioadenda        
     ,G.pk_operacionalterno 
     ,H.pk_fecha         
     ,A.comentario
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_ADENDAS} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_TIPO} C on A.cod_tipoadenda=C.cod_tipo 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_ESTADO} D on A.cod_estado=D.cod_estado
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE} E on  case when A.cod_paquete=0 then 5 else  A.cod_paquete end=E.cod_paquete
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE} F on  case when A.cod_paquete=0 then 5 else  A.cod_paquete end=F.cod_paquete
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO} G on A.cod_operacionalterno=G.cod_operacionalterno
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} H on cast(A.fechaadenda as date)= H.fecha""") 
   
    #final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    #df_temp = spark.read.parquet(path_temp)
    df_ft_adendas.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_ADENDAS}")
    print('Crea y Carga tabla FT_ADENDAS')
else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_ft_adendas = spark.sql(f"""
    select 
      A.cod_adenda as pk_adenda           
     ,B.pk_contrato         
     ,C.pk_tipo             
     ,D.pk_estado           
     ,E.pk_paquete as pk_paquetenew       
     ,F.pk_paquete          
     ,A.diascarencia        
     ,A.precioadenda        
     ,G.pk_operacionalterno 
     ,H.pk_fecha         
     ,A.comentario
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_ADENDAS} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_TIPO} C on A.cod_tipoadenda=C.cod_tipo 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_ESTADO} D on A.cod_estado=D.cod_estado
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE} E on  case when A.cod_paquete=0 then 5 else  A.cod_paquete end=E.cod_paquete
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE} F on  case when A.cod_paquete=0 then 5 else  A.cod_paquete end=F.cod_paquete
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO} G on A.cod_operacionalterno=G.cod_operacionalterno
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} H on cast(A.fechaadenda as date)= H.fecha
    """) 
    
    final_df = df_ft_adendas
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_ADENDAS}")
    print('Crea y Carga tabla FT_ADENDAS')
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_FT_LIQUIDACIONES}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_FT_LIQUIDACIONES}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    #max_pk = target_df.agg(F.max(f"Pk_Cliente".lower())).collect()[0][0] or 0

    df_ft_liquidaciones = spark.sql(f"""
    select 
       A.cod_liquidacion as Pk_Liquidacion    
      ,B.Pk_TipoLiquidacion           
      ,C.Pk_Contrato        
      ,D.Pk_Motivo          
      ,E.Pk_Representante   
      ,A.listaprecio         
      ,A.montopagado         
      ,A.interesmoratorio    
      ,A.montopenalidad      
      ,A.gastoadministrativo 
      ,A.saldoafavor         
      ,A.comentario          
      ,F.Pk_Fecha    
      ,A.nombreusuario     
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_LIQUIDACIONES} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION} B on A.cod_tipo=B.cod_tipoliquidacion
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} C on A.cod_contrato=C.cod_contrato 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO} D on A.cod_motivo=D.cod_motivo
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE} E on A.cod_representante=E.cod_representante
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} F on cast(A.fechaliquidacion as date)= F.fecha""") 
   
    #final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    #df_temp = spark.read.parquet(path_temp)
    df_ft_liquidaciones.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_LIQUIDACIONES}")
    print('Crea y Carga tabla FT_LIQUIDACIONES')
else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_ft_liquidaciones = spark.sql(f"""
    select 
       A.cod_liquidacion as Pk_Liquidacion    
      ,B.Pk_TipoLiquidacion           
      ,C.Pk_Contrato        
      ,D.Pk_Motivo          
      ,E.Pk_Representante   
      ,A.listaprecio         
      ,A.montopagado         
      ,A.interesmoratorio    
      ,A.montopenalidad      
      ,A.gastoadministrativo 
      ,A.saldoafavor         
      ,A.comentario          
      ,F.Pk_Fecha    
      ,A.nombreusuario     
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_LIQUIDACIONES} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION} B on A.cod_tipo=B.cod_tipoliquidacion
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} C on A.cod_contrato=C.cod_contrato 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO} D on A.cod_motivo=D.cod_motivo
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE} E on A.cod_representante=E.cod_representante
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} F on cast(A.fechaliquidacion as date)= F.fecha
    """) 
    
    final_df = df_ft_liquidaciones
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_LIQUIDACIONES}")
    print('Crea y Carga tabla FT_LIQUIDACIONES')
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_FT_ORDEN_PLACA}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_FT_ORDEN_PLACA}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    #max_pk = target_df.agg(F.max(f"Pk_Cliente".lower())).collect()[0][0] or 0

    df_ft_orden_placa = spark.sql(f"""
    select 
       B.Pk_Contrato        
      ,A.cod_placa as Pk_placa           
      ,C.Pk_Tipoorden       
      ,A.interesmorosoini    
      ,A.montomorosoini      
      ,D.Pk_Estado          
      ,A.mincuotasxpagar     
      ,A.cuotasatrasadas     
      ,A.interesarecuperar   
      ,A.montoarecuperar     
      ,E.Pk_TipoLiberacion  
      ,A.operarioencargado   
      ,A.interespagado       
      ,A.montopagado         
      ,A.fechanotificacion   
      ,F.Pk_Fecha     
      ,A.interesmoratoriototal
      ,A.cantnotificaciones  
      ,A.fechaproceso        
      ,A.comentarios         
      ,A.cod_liquidacion  AS Pk_Liquidacion   
      ,A.fecharegistro       
      ,A.fechaactualizacion  
      ,A.usuarioactualizacion
      ,A.montomoroso       
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_ORDEN_PLACA} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN} C on A.cod_tipoorden=C.cod_tipoorden
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_ESTADO} D on A.cod_estado=D.cod_estado
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION} E on A.cod_tipoliberacion=E.cod_tipoliberacion
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} F on cast(A.fechaultimopago as date)= F.fecha""") 
   
    #final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    #df_temp = spark.read.parquet(path_temp)
    df_ft_orden_placa.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_ORDEN_PLACA}")
    print('Crea y Carga tabla FT_ORDEN_PLACA')
else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_ft_orden_placa = spark.sql(f"""
    select 
       B.Pk_Contrato        
      ,A.cod_placa as Pk_placa           
      ,C.Pk_Tipoorden       
      ,A.interesmorosoini    
      ,A.montomorosoini      
      ,D.Pk_Estado          
      ,A.mincuotasxpagar     
      ,A.cuotasatrasadas     
      ,A.interesarecuperar   
      ,A.montoarecuperar     
      ,E.Pk_TipoLiberacion  
      ,A.operarioencargado   
      ,A.interespagado       
      ,A.montopagado         
      ,A.fechanotificacion   
      ,F.Pk_Fecha     
      ,A.interesmoratoriototal
      ,A.cantnotificaciones  
      ,A.fechaproceso        
      ,A.comentarios         
      ,A.cod_liquidacion  AS Pk_Liquidacion   
      ,A.fecharegistro       
      ,A.fechaactualizacion  
      ,A.usuarioactualizacion
      ,A.montomoroso       
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_ORDEN_PLACA} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN} C on A.cod_tipoorden=C.cod_tipoorden
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_ESTADO} D on A.cod_estado=D.cod_estado
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION} E on A.cod_tipoliberacion=E.cod_tipoliberacion
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} F on cast(A.fechaultimopago as date)= F.fecha
    """) 
    
    final_df = df_ft_orden_placa
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_ORDEN_PLACA}")
    print('Crea y Carga tabla FT_ORDEN_PLACA')
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_FT_PAGOS}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_FT_PAGOS}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    #max_pk = target_df.agg(F.max(f"Pk_Cliente".lower())).collect()[0][0] or 0

    df_ft_pagos = spark.sql(f"""
    select 
      B.Pk_Contrato        
     ,A.cod_pago AS Pk_Pago           
     ,A.monto               
     ,A.montoefectivo       
     ,A.montotarjeta        
     ,A.montodeposito       
     ,A.nombrecajero        
     ,E.Pk_Fecha           
     ,A.comentario          
     ,A.referenciapago      
     ,A.montoreserva        
     ,C.Pk_Estado          
     ,A.onlinecode          
     ,A.tipopago            
     ,D.Pk_TipoComprobante         
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_PAGOS} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_ESTADO} C on A.cod_estado=C.cod_estado
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE} D on A.cod_tipocomprobante=D.cod_tipocomprobante 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} E on cast(A.fechapago as date)= E.fecha""") 
   
    #final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    #df_temp = spark.read.parquet(path_temp)
    df_ft_pagos.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_PAGOS}")
    print('Crea y Carga tabla FT_PAGOS')
else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_ft_pagos = spark.sql(f"""
    select 
      B.Pk_Contrato        
     ,A.cod_pago AS Pk_Pago           
     ,A.monto               
     ,A.montoefectivo       
     ,A.montotarjeta        
     ,A.montodeposito       
     ,A.nombrecajero        
     ,E.Pk_Fecha           
     ,A.comentario          
     ,A.referenciapago      
     ,A.montoreserva        
     ,C.Pk_Estado          
     ,A.onlinecode          
     ,A.tipopago            
     ,D.Pk_TipoComprobante         
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_PAGOS} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_ESTADO} C on A.cod_estado=C.cod_estado
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE} D on A.cod_tipocomprobante=D.cod_tipocomprobante 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} E on cast(A.fechapago as date)= E.fecha
    """) 
    
    final_df = df_ft_pagos
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_PAGOS}")
    print('Crea y Carga tabla FT_PAGOS')
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_FT_FINANCIAMIENTO}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_FT_FINANCIAMIENTO}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    #max_pk = target_df.agg(F.max(f"Pk_Cliente".lower())).collect()[0][0] or 0

    df_ft_financiamiento = spark.sql(f"""
       select 
       A.cod_financiamiento  as Pk_financiamiento
      ,B.Pk_contrato
      ,C.Pk_paquete
      ,A.cod_paqueteproducto as Pk_paqueteproducto 
      ,E.Pk_modopago
      ,D.Pk_estado
      ,A.preciofinal         
      ,A.cuotainicial        
      ,A.montofinanciado     
      ,A.tasainteres         
      ,A.montofcm            
      ,A.nrocuotasinicial    
      ,A.nrocuotas           
      ,A.valorcuota          
      ,A.fechapago           
      ,F.Pk_fecha 
      ,A.fechaprimerpago     
      ,A.observaciones       
      ,A.cuotaspendientes    
      ,A.deudapendiente      
      ,A.cuotasmorosas       
      ,A.carteratotal        
      ,A.interescompagado    
      ,A.totalpagado         
      ,A.fechaultimopago     
      ,A.interesmoroso       
      ,A.diasatraso          
      ,A.cargospagados       
      ,A.cargosinteresmoroso 
      ,A.cargosdiasatrasados 
      ,A.cargoscuotaspendientes
      ,A.cargospendientes    
      ,A.cargoscuotasmorosas 
      ,A.cargosmorosos       
      ,A.fechaultimopagocargos
      ,A.totalmoroso
    from (select distinct * from {bdawscobsi}.{CONST_INPUT_TABLA_F_FINANCIAMIENTO}) A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE} C on case when A.cod_paquete=0 then 5 else  A.cod_paquete end=C.cod_paquete
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO} E on A.cod_modopago=E.cod_modopago 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_ESTADO} D on A.cod_estado = D.cod_estado
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} F on cast(A.fechafinanciamiento as date)= F.fecha""") 
   
    #final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    #df_temp = spark.read.parquet(path_temp)
    df_ft_financiamiento.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_FINANCIAMIENTO}")
    print('Crea y Carga tabla FT_FINANCIAMIENTO')
else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_ft_financiamiento = spark.sql(f"""
       Select 
       A.cod_financiamiento  as Pk_financiamiento
      ,B.Pk_contrato
      ,C.Pk_paquete
      ,A.cod_paqueteproducto as Pk_paqueteproducto
      ,E.Pk_modopago
      ,D.Pk_estado
      ,A.preciofinal         
      ,A.cuotainicial        
      ,A.montofinanciado     
      ,A.tasainteres         
      ,A.montofcm            
      ,A.nrocuotasinicial    
      ,A.nrocuotas           
      ,A.valorcuota          
      ,A.fechapago           
      ,F.Pk_fecha 
      ,A.fechaprimerpago     
      ,A.observaciones       
      ,A.cuotaspendientes    
      ,A.deudapendiente      
      ,A.cuotasmorosas       
      ,A.carteratotal        
      ,A.interescompagado    
      ,A.totalpagado         
      ,A.fechaultimopago     
      ,A.interesmoroso       
      ,A.diasatraso          
      ,A.cargospagados       
      ,A.cargosinteresmoroso 
      ,A.cargosdiasatrasados 
      ,A.cargoscuotaspendientes
      ,A.cargospendientes    
      ,A.cargoscuotasmorosas 
      ,A.cargosmorosos       
      ,A.fechaultimopagocargos
      ,A.totalmoroso
      from (select distinct * from {bdawscobsi}.{CONST_INPUT_TABLA_F_FINANCIAMIENTO}) A 
      left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato
      left join {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE} C on case when A.cod_paquete=0 then 5 else  A.cod_paquete end=C.cod_paquete
      left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO} E on A.cod_modopago=E.cod_modopago
      left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_ESTADO} D on A.cod_estado = D.cod_estado
      left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} F on cast(A.fechafinanciamiento as date)= F.fecha""") 
    
    final_df = df_ft_financiamiento
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_FINANCIAMIENTO}")
    print('Crea y Carga tabla FT_FINANCIAMIENTO')
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_FT_CRONOGRAMA}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_FT_CRONOGRAMA}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    #max_pk = target_df.agg(F.max(f"Pk_Cliente".lower())).collect()[0][0] or 0

    df_ft_cronograma = spark.sql(f"""
    select 
      A.cod_financiamiento  as Pk_Financiamiento
     ,A.nrocuota            
     ,case when A.cod_cronograma is null then 0 else A.cod_cronograma end  as Pk_Cronograma     
     ,case when A.cod_pago is null then 0 else A.cod_pago end as Pk_Pago           
     ,A.descripcionpago     
     ,B.Pk_Estado          
     ,A.descripcion         
     ,A.montocapital        
     ,A.impuestoigv         
     ,C.Pk_Fecha    
     ,A.fechapago           
     ,A.saldodeudor         
     ,A.saldocuota          
     ,A.autpago             
     ,A.interesmanual       
     ,A.valordesagio        
     ,A.montocuota          
     ,A.montopago        
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_CRONOGRAMA} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO} B on A.cod_estado=B.cod_estado 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} C on cast(A.fechavencimiento as date)= C.fecha""") 
   
    #final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    #df_temp = spark.read.parquet(path_temp)
    df_ft_cronograma.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_CRONOGRAMA}")
    print('Crea y Carga tabla FT_CRONOGRAMA')
else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_ft_cronograma = spark.sql(f"""
    select 
      A.cod_financiamiento  as Pk_Financiamiento
     ,A.nrocuota            
     ,case when A.cod_cronograma is null then 0 else A.cod_cronograma end  as Pk_Cronograma     
     ,case when A.cod_pago is null then 0 else A.cod_pago end as Pk_Pago           
     ,A.descripcionpago     
     ,B.Pk_Estado          
     ,A.descripcion         
     ,A.montocapital        
     ,A.impuestoigv         
     ,C.Pk_Fecha    
     ,A.fechapago           
     ,A.saldodeudor         
     ,A.saldocuota          
     ,A.autpago             
     ,A.interesmanual       
     ,A.valordesagio        
     ,A.montocuota          
     ,A.montopago        
    from {bdawscobsi}.{CONST_INPUT_TABLA_F_CRONOGRAMA} A 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO} B on A.cod_estado=B.cod_estado 
    left join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} C on cast(A.fechavencimiento as date)= C.fecha
    """) 
    
    final_df = df_ft_cronograma
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_FT_CRONOGRAMA}")
    print('Crea y Carga tabla FT_CRONOGRAMA')
from pyspark.sql.functions import col, expr, when, concat, lit, rtrim
from pyspark.sql import functions as F
path_target = f"{ruta_dest}/{CONST_OUTPUT_TABLA_CHURN}/"
path_temp = f"{ruta_dest}/Temp/{CONST_OUTPUT_TABLA_CHURN}/"
try:
    df_existentes = spark.read.format("parquet").load(path_target)
    datos_existentes = True
except:
    datos_existentes = False

if datos_existentes:
    print('existe')
    target_df = spark.read.parquet(path_target)
    print(f"Archivo destino cargado desde: {path_target}")
    #max_pk = target_df.agg(F.max(f"Pk_Cliente".lower())).collect()[0][0] or 0

    df_ft_churn = spark.sql(f"""
    select 
      J.Pk_Fecha           
     ,B.Pk_contrato        
     ,A.nrocontrato         
     ,C.Pk_Cliente         
     ,D.Pk_Estado          
     ,E.Pk_Tipo            
     ,F.Pk_TipoServicio    
     ,A.capacidad           
     ,A.actualcapacidad     
     ,A.nrousos             
     ,G.Pk_TipoMoneda      
     ,A.preciolista         
     ,A.descuento           
     ,A.descuentoadi        
     ,A.descuentoprom1      
     ,A.descuentoprom2      
     ,A.anticipo            
     ,A.fechafirma          
     ,A.usuarioregistro     
     ,A.fechaventa          
     ,H.Pk_TipoEspacio     
     ,A.flagperiodocarencia 
     ,I.Pk_TipoClasificacion
     ,A.diascarencia        
     ,A.fechaanulacion          
    from {bdawscobsi}.{CONST_INPUT_TABLA_CHURN} A 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE} C on A.cod_cliente=C.cod_cliente 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_ESTADO} D on A.cod_estado=D.cod_estado 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPO} E on A.cod_tipo=E.cod_tipo
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO} F on A.cod_tiposervicio=F.cod_tiposervicio
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA} G on A.cod_tipomoneda=G.cod_tipomoneda
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO} H on A.cod_tipoespacio=H.cod_tipoespacio
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION} I on A.cod_tipoclasificacion=I.cod_tipoclasificacion
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} J on cast(A.fechafoto as date)= J.fecha""") 
   
    #final_df.write.mode("overwrite").parquet(path_temp)
    # Mover datos de temporal al destino final
    #df_temp = spark.read.parquet(path_temp)
    df_ft_churn.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CHURN}")
    print('Crea y Carga tabla CHURN')
else:
    print(f"Archivo destino no encontrado. Creando uno nuevo: {path_target}")
    df_ft_churn = spark.sql(f"""
    select 
      J.Pk_Fecha           
     ,B.Pk_contrato        
     ,A.nrocontrato         
     ,C.Pk_Cliente         
     ,D.Pk_Estado          
     ,E.Pk_Tipo            
     ,F.Pk_TipoServicio    
     ,A.capacidad           
     ,A.actualcapacidad     
     ,A.nrousos             
     ,G.Pk_TipoMoneda      
     ,A.preciolista         
     ,A.descuento           
     ,A.descuentoadi        
     ,A.descuentoprom1      
     ,A.descuentoprom2      
     ,A.anticipo            
     ,A.fechafirma          
     ,A.usuarioregistro     
     ,A.fechaventa          
     ,H.Pk_TipoEspacio     
     ,A.flagperiodocarencia 
     ,I.Pk_TipoClasificacion
     ,A.diascarencia        
     ,A.fechaanulacion          
    from {bdawscobsi}.{CONST_INPUT_TABLA_CHURN} A 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} B on A.cod_contrato=B.cod_contrato
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE} C on A.cod_cliente=C.cod_cliente 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_ESTADO} D on A.cod_estado=D.cod_estado 
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPO} E on A.cod_tipo=E.cod_tipo
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO} F on A.cod_tiposervicio=F.cod_tiposervicio
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA} G on A.cod_tipomoneda=G.cod_tipomoneda
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO} H on A.cod_tipoespacio=H.cod_tipoespacio
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION} I on A.cod_tipoclasificacion=I.cod_tipoclasificacion
    inner join {bdawscobgl}.{CONST_OUTPUT_TABLA_FECHA} J on cast(A.fechafoto as date)= J.fecha
    """) 
    
    final_df = df_ft_churn
    #final_df.show()
    final_df.write.format("parquet").mode("overwrite").option("path", path_target).saveAsTable(f"{bdawscobgl}.{CONST_OUTPUT_TABLA_CHURN}")
    print('Crea y Carga tabla CHURN')
job.commit()
print('fin de job')
job.commit()