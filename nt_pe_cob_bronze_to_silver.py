import sys
from awsglue.transforms import *
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from typing import Dict, Any, Optional
from pyspark.sql.functions import col, expr, when, to_date, to_timestamp, lit, current_date, current_timestamp
import boto3

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()
glue_client = boto3.client('glue')

job = Job(glueContext)
#job.init(args['Job_Conexion'], args)
JOB_NAME = "nt_pe_cob_bronze_to_silver"
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
bdawscobtmp   = ssm.get_parameter(Name='p_aws_cob_tmp_db', WithDecryption=True)['Parameter']['Value']

#tablas input BRONZE
CONS_TABLA_PE_AGREEMENT_MODIFICATION = "pe_agreement_modification"
CONS_TABLA_PE_AGREEMENT = "pe_agreement" 
CONS_TABLA_GEN_PERSON = "gen_person"
CONS_TABLA_PE_PLATAFORM_ACTIVITY="pe_plataform_activity"
CONS_TABLA_PE_PLATAFORM_DETAIL="pe_plataform_detail"
CONS_TABLA_PE_COLLECTION_ORDER_STONE="pe_collection_order_stone"
CONS_TABLA_PE_CLIENT_SALE_AGREEMENT="pe_client_sale_agreement"
CONS_TABLA_PE_FINANCING="pe_financing"
CONS_TABLA_GEN_PACKAGE_AGREEMENT="gen_package_agreement"
CONS_TABLA_GEN_PACKAGE_AGREEMENTITEM="gen_package_agreement_item"
CONS_TABLA_GEN_PACKAGE="gen_package"
CONS_TABLA_PE_PAYMENT_SPLIT = "pe_payment_split"
CONS_TABLA_PE_PAYMENT = "pe_payment" 
CONS_TABLA_PE_PAYMENT_SCHEDULE = "pe_payment_schedule" 
CONS_TABLA_FACT_AGREEMENT_HISTORY = "fact_agreement_history" 
CONS_TABLA_DE_ADENDA_ESTADO = "de_adenda_estado"
CONS_TABLA_DE_ADENDA_OPERACIONALTERNO = "de_adenda_operacionalterno"
CONS_TABLA_DE_ADENDA_TIPO = "de_adenda_tipo"
CONS_TABLA_DE_CLIENTE_ESTADOCIVIL = "de_cliente_estadocivil" 
CONS_TABLA_DE_CLIENTE_SEXO = "de_cliente_sexo" 
CONS_TABLA_DE_CLIENTE_TIPODOCUMENTO = "de_cliente_tipodocumento" 
CONS_TABLA_DE_CLIENTE_TIPOOCUPACION = "de_cliente_tipoocupacion" 
CONS_TABLA_DE_CLIENTE_TIPORELIGION = "de_cliente_tiporeligion"
CONS_TABLA_DE_CONTRATO_ESTADO = "de_contrato_estado" 
CONS_TABLA_DE_CONTRATO_TIPO = "de_contrato_tipo"
CONS_TABLA_DE_CONTRATO_TIPOCLASIFICACION= "de_contrato_tipoclasificacion"
CONS_TABLA_DE_CONTRATO_TIPOESPACIO = "de_contrato_tipoespacio" 
CONS_TABLA_DE_CONTRATO_TIPOMONEDA = "de_contrato_tipomoneda" 
CONS_TABLA_DE_CONTRATO_TIPOSERVICIO = "de_contrato_tiposervicio"
CONS_TABLA_DE_CRONOGRAMA_ESTADO = "de_cronograma_estado"
CONS_TABLA_DE_FINANCIAMIENTO_MODOPAGO = "de_financiamiento_modopago"
CONS_TABLA_DE_FINANCIAMIENTO_ESTADO = "de_financiamiento_estado"
CONS_TABLA_DE_LIQUIDACION_MOTIVO = "de_liquidacion_motivo"
CONS_TABLA_DE_LIQUIDACION_REPRESENTANTE ="de_liquidacion_representante"
CONS_TABLA_DE_LIQUIDACION_TIPOLIQUIDACION = "de_liquidacion_tipoliquidacion"
CONS_TABLA_DE_PAGOS_ESTADO = "de_pagos_estado"
CONS_TABLA_DE_PAGOS_TIPOCOMPROBANTE = "de_pagos_tipocomprobante"
CONS_TABLA_DE_PAQUETE_TIPO = "de_paquete_tipo"
CONS_TABLA_DE_PAQUETE_TIPOSERVICIO = "de_paquete_tiposervicio"
CONS_TABLA_DE_PIEDRAS_ESTADO = "de_piedras_estado"
CONS_TABLA_DE_PIEDRAS_TIPO = "de_piedras_tipo"
CONS_TABLA_DE_PIEDRAS_TIPOLIBERACION = "de_piedras_tipoliberacion"
CONS_TABLA_DE_PIEDRAS_TIPOORDEN = "de_piedras_tipoorden"



#tablas output SILVER
CONST_OUT_TABLA_ADENDA_ESTADO = "ADENDA_ESTADO"
CONST_OUT_TABLA_ADENDA_OPERACIONALTERNO = "ADENDA_OPERACIONALTERNO"
CONST_OUT_TABLA_ADENDA_TIPO = "ADENDA_TIPO"
CONST_OUT_TABLA_CLIENTE = "CLIENTE"
CONST_OUT_TABLA_CLIENTE_ESTADOCIVIL = "CLIENTE_ESTADO_CIVIL"
CONST_OUT_TABLA_CLIENTE_SEXO = "CLIENTE_SEXO"
CONST_OUT_TABLA_CLIENTE_TIPODOCUMENTO = "CLIENTE_TIPO_DOCUMENTO"
CONST_OUT_TABLA_CLIENTE_TIPOOCUPACION = "CLIENTE_TIPO_OCUPACION"
CONST_OUT_TABLA_CLIENTE_TIPORELIGION  = "CLIENTE_TIPO_RELIGION"
CONST_OUT_TABLA_CONTRATO = "CONTRATO"
CONST_OUT_TABLA_CONTRATO_ESTADO = "CONTRATO_ESTADO"
CONST_OUT_TABLA_CONTRATO_TIPO   = "CONTRATO_TIPO"
CONST_OUT_TABLA_CONTRATO_TIPOSERVICIO = "CONTRATO_TIPO_SERVICIO"
CONST_OUT_TABLA_CONTRATO_TIPOCLASIFICACION = "CONTRATO_TIPO_CLASIFICACION"
CONST_OUT_TABLA_CONTRATO_TIPOESPACIO  = "CONTRATO_TIPO_ESPACIO"
CONST_OUT_TABLA_CONTRATO_TIPOMONEDA   = "CONTRATO_TIPO_MONEDA"
CONST_OUT_TABLA_CRONOGRAMA_ESTADO = "CRONOGRAMA_ESTADO"
CONST_OUT_TABLA_FINANCIAMIENTO_MODOPAGO = "FINANCIAMIENTO_MODO_PAGO"
CONST_OUT_TABLA_FINANCIAMIENTO_ESTADO = "FINANCIAMIENTO_ESTADO"
CONST_OUT_TABLA_LIQUIDACION_MOTIVO = "LIQUIDACION_MOTIVO"
CONST_OUT_TABLA_LIQUIDACION_REPRESENTANTE = "LIQUIDACION_REPRESENTANTE"
CONST_OUT_TABLA_LIQUIDACION_TIPOLIQUIDACION = "LIQUIDACION_TIPO_LIQUIDACION"
CONST_OUT_TABLA_PAGOS_ESTADO = "PAGOS_ESTADO"
CONST_OUT_TABLA_PAGOS_TIPOCOMPROBANTE = "PAGOS_TIPO_COMPROBANTE"
CONST_OUT_TABLA_PAQUETE = "PAQUETE"
CONST_OUT_TABLA_PAQUETE_TIPO = "PAQUETE_TIPO"
CONST_OUT_TABLA_PAQUETE_TIPOSERVICIO = "PAQUETE_TIPO_SERVICIO"
CONST_OUT_TABLA_FINANCIAMIENTO_PRODUCTO = "F_FINANCIAMIENTO_PRODUCTO"
CONST_OUT_TABLA_PIEDRAS_ESTADO    = "PLACA_ESTADO"
CONST_OUT_TABLA_PIEDRAS_TIPO = "PLACA_TIPO"
CONST_OUT_TABLA_PIEDRAS_TIPOLIBERACION = "PLACA_TIPO_LIBERACION"
CONST_OUT_TABLA_PIEDRAS_TIPOORDEN = "PLACA_TIPO_ORDEN"

CONST_OUT_TABLA_F_ADENDAS = "F_ADENDAS"
CONST_OUT_TABLA_F_PAGOS = "F_PAGOS"
CONST_OUT_TABLA_F_CRONOGRAMA = "F_CRONOGRAMA"
CONST_OUT_TABLA_F_FINANCIAMIENTO = "F_FINANCIAMIENTO"
CONST_OUT_TABLA_F_ORDEN_PLACA = "F_ORDEN_PLACA"
CONST_OUT_TABLA_F_LIQUIDACIONES = "F_LIQUIDACIONES"
CONST_OUT_TABLA_CHURN = "CHURN"

#CONST_OUT_TABLA_FINANCIAMIENTO_CONCEPTO = "CONCEPTO_FINANCIAMIENTO"

#CONST_OUT_TABLA_FINANCIAMIENTO_PAQUETE = "PAQUETE_FINANCIAMIENTO"



#CONST_TABLA_KPIS_COBRANZA="F_KPIS_COBRANZA"


# 1. Leer archivo TXT desde S3 con lista de tablas
ruta_txt  = f"s3://ue1stg{amb}as3cob001/ArchivosTXT/ListaTablasParqueEterno.txt"
ruta_orig = f"s3://ue1stg{amb}as3cob002/TABLAS_BRONZE"
ruta_dest = f"s3://ue1stg{amb}as3cob003/TABLAS_SILVER"
tablas_df = spark.read.text(ruta_txt)   # cada fila = una tabla #spark.read.option("delimiter", "|").format("csv").option("header", "false").load(ruta_txt) #
tablas = [fila.value.strip() for fila in tablas_df.collect() if fila.value.strip()]
print('✅ Lectura de parametros realizado con éxito ')
def carga_tabla_silver(dataframe,ruta_dest,tabla_destino):
    
    path_target = f"{ruta_dest}/{tabla_destino}/"
    
    try:
        # Verificar si la tabla gold ya existe
        try:
            df_existentes = spark.read.format("parquet").load(path_target)
            datos_existentes = True
            logger.info(f"Datos existentes cargados: {df_existentes.count()} registros")
            print(f'✅ Si existe la tabla {tabla_destino}')
        except:
            datos_existentes = False
            logger.info(f"No se encontraron datos existentes ")
            print(f'⚠️ No existe la tabla {tabla_destino}')
            
            
            
        if datos_existentes:
            #schema = obtener_esquema_tabla(f"{tabla}",logger)
            #df_nuevos = spark.read.format("parquet").load(path_source)
            
            final_data = dataframe
            
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
                .saveAsTable(f"{bdawscobsi}.{tabla_destino}")
            
            cant_ingreso = final_data.count()
            
            print(f"✅ Total de registros desde bronze a la tabla silver {tabla_destino} : {cant_ingreso}")
            
             #Limpia la ubicación temporal
            #glueContext.purge_s3_path(f"{ruta_dest}/Temp/{tabla}", {"retentionPeriod": 0})
            #print(f"✅ parquet temporal {tabla} eliminada correctamente de la ruta temporal {ruta_dest}/Temp/{tabla}.")
            #glue_client.delete_table(DatabaseName=bdawscobtmp, Name=tabla)
            #print(f"✅ Tabla {tabla} eliminada correctamente de la base de datos '{bdawscobtmp}'.")
                
        else:
            #schema = obtener_esquema_tabla(f"{tabla}",logger)
            #df_nuevos = spark.read.format("parquet").load(path_source)
            
            additional_options = {
                "path": f"{path_target}"
            }
            dataframe.write \
                .format("parquet") \
                .options(**additional_options) \
                .mode("overwrite") \
                .saveAsTable(f"{bdawscobsi}.{tabla_destino}")
            
            cant_ingreso = dataframe.count()
            
            print(f"✅ Total de registros desde bronze a la tabla silver {tabla_destino} : {cant_ingreso}")
            
    except Exception as e:
        error_msg = f"Error en el proceso: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
print('✅ Función carga_tabla_silver')
def carga_tabla_silver_churn(dataframe,ruta_dest,tabla_destino):
    
    path_target = f"{ruta_dest}/{tabla_destino}/"
    
    try:
        # Verificar si la tabla gold ya existe
        try:
            df_existentes = spark.read.format("parquet").load(path_target)
            datos_existentes = True
            logger.info(f"Datos existentes cargados: {df_existentes.count()} registros")
            print(f'✅ Si existe la tabla {tabla_destino}')
        except:
            datos_existentes = False
            logger.info(f"No se encontraron datos existentes ")
            print(f'⚠️ No existe la tabla {tabla_destino}')
            
            
            
        if datos_existentes:
            #schema = obtener_esquema_tabla(f"{tabla}",logger)
            #df_nuevos = spark.read.format("parquet").load(path_source)
            
            final_data = dataframe
            
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
                .mode("append") \
                .saveAsTable(f"{bdawscobsi}.{tabla_destino}")
            
            cant_ingreso = final_data.count()
            
            print(f"✅ Total de registros desde bronze a la tabla silver {tabla_destino} : {cant_ingreso}")
            
             #Limpia la ubicación temporal
            #glueContext.purge_s3_path(f"{ruta_dest}/Temp/{tabla}", {"retentionPeriod": 0})
            #print(f"✅ parquet temporal {tabla} eliminada correctamente de la ruta temporal {ruta_dest}/Temp/{tabla}.")
            #glue_client.delete_table(DatabaseName=bdawscobtmp, Name=tabla)
            #print(f"✅ Tabla {tabla} eliminada correctamente de la base de datos '{bdawscobtmp}'.")
                
        else:
            #schema = obtener_esquema_tabla(f"{tabla}",logger)
            #df_nuevos = spark.read.format("parquet").load(path_source)
            
            additional_options = {
                "path": f"{path_target}"
            }
            dataframe.write \
                .format("parquet") \
                .options(**additional_options) \
                .mode("overwrite") \
                .saveAsTable(f"{bdawscobsi}.{tabla_destino}")
            
            cant_ingreso = dataframe.count()
            
            print(f"✅ Total de registros desde bronze a la tabla silver {tabla_destino} : {cant_ingreso}")
            
    except Exception as e:
        error_msg = f"Error en el proceso: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
print('✅ Función carga_tabla_silver')
df_adenda_estado = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_ADENDA_ESTADO}""") 
df_adenda_estado_sin_duplicados = df_adenda_estado.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Estado"),col("descripcion").alias("Descripcion"))
print('carga df_adenda_estado_sin_duplicados', df_adenda_estado_sin_duplicados.count())
carga_tabla_silver(df_adenda_estado_sin_duplicados,ruta_dest,CONST_OUT_TABLA_ADENDA_ESTADO)
df_adenda_operacionalterno = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_ADENDA_OPERACIONALTERNO}""") 
df_adenda_operacionalterno_sin_duplicados = df_adenda_operacionalterno.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_OperacionAlterno"),col("descripcion").alias("Descripcion"))
print('carga df_adenda_operacionalterno_sin_duplicados', df_adenda_operacionalterno_sin_duplicados.count())
carga_tabla_silver(df_adenda_operacionalterno_sin_duplicados,ruta_dest,CONST_OUT_TABLA_ADENDA_OPERACIONALTERNO)
df_adenda_tipo = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_ADENDA_TIPO}""") 
df_adenda_tipo_sin_duplicados = df_adenda_tipo.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Tipo"),col("descripcion").alias("Descripcion"))
print('carga df_adenda_tipo_sin_duplicados', df_adenda_tipo_sin_duplicados.count())
carga_tabla_silver(df_adenda_tipo_sin_duplicados,ruta_dest,CONST_OUT_TABLA_ADENDA_TIPO)
df_cliente_estadocivil = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CLIENTE_ESTADOCIVIL}""") 
df_cliente_estadocivil_sin_duplicados = df_cliente_estadocivil.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_EstadoCivil"),col("descripcion").alias("Descripcion"))
print('carga df_cliente_estadocivil_sin_duplicados', df_cliente_estadocivil_sin_duplicados.count())
carga_tabla_silver(df_cliente_estadocivil_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CLIENTE_ESTADOCIVIL)
df_cliente_sexo = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CLIENTE_SEXO}""") 
df_cliente_sexo_sin_duplicados = df_cliente_sexo.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Sexo"),col("descripcion").alias("Descripcion"))
print('carga df_cliente_sexo_sin_duplicados', df_cliente_sexo_sin_duplicados.count())
carga_tabla_silver(df_cliente_sexo_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CLIENTE_SEXO)
df_cliente_tipodocumento = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CLIENTE_TIPODOCUMENTO}""") 
df_cliente_tipodocumento_sin_duplicados = df_cliente_tipodocumento.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoDocumento"),col("descripcion").alias("Descripcion"))
print('carga df_cliente_tipodocumento_sin_duplicados', df_cliente_tipodocumento_sin_duplicados.count())
carga_tabla_silver(df_cliente_tipodocumento_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CLIENTE_TIPODOCUMENTO)
df_cliente_tipoocupacion = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CLIENTE_TIPOOCUPACION}""") 
df_cliente_tipoocupacion_sin_duplicados = df_cliente_tipoocupacion.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoOcupacion"),col("descripcion").alias("Descripcion"))
print('carga df_cliente_tipoocupacion_sin_duplicados', df_cliente_tipoocupacion_sin_duplicados.count())
carga_tabla_silver(df_cliente_tipoocupacion_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CLIENTE_TIPOOCUPACION)
df_cliente_tiporeligion = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CLIENTE_TIPORELIGION}""") 
df_cliente_tiporeligion_sin_duplicados = df_cliente_tiporeligion.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoReligion"),col("descripcion").alias("Descripcion"))
print('carga df_cliente_tiporeligion_sin_duplicados', df_cliente_tiporeligion_sin_duplicados.count())
carga_tabla_silver(df_cliente_tiporeligion_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CLIENTE_TIPORELIGION)
df_cliente = spark.table(f"""{bdawscobbr}.{CONS_TABLA_GEN_PERSON}""") 
df_cliente_sin_duplicados = df_cliente.dropDuplicates(["uid"]).select(col("uid").alias("Cod_Cliente"),col("typedocument").alias("Cod_TipoDocumento"), col("numberdocument").alias("NroDocumento"), \
                                                                      col("fullname").alias("NombreCompleto"),col("cellphone").alias("NroTelefonoCel"),col("homephone").alias("NroTelefonoCasa"), \
                                                                      col("name").alias("Nombre"), col("lastnamep").alias("ApellidoPat"),col("lastnamem").alias("ApellidoMat"),col("lastnamemarried").alias("ApellidoCasado"), \
                                                                      col("personalemail").alias("CorreoPersonal"),col("birthdaydate").alias("FechaNacimiento"),col("deathdate").alias("FechaDefuncion"), \
                                                                      col("civilstate").alias("Cod_EstadoCivil"), col("credouid").alias("Cod_TipoReligion"),col("occupationuid").alias("Cod_TipoOcupacion"), \
                                                                      col("sex").alias("Cod_Sexo"),col("address").alias("Direccion"),col("residentialarea").alias("Urbanizacion"),col("city").alias("Ciudad"), \
                                                                      col("district").alias("Distrito"), col("province").alias("Provincia"), col("referenceaddress").alias("ReferenciaDireccion"), \
                                                                      col("workcenter").alias("LugarTrabajo"),col("workaddress").alias("DireccionTrabajo"),col("workemail").alias("CorreoTrabajo"),col("workphone").alias("NroTelefonoTrabajo"))
print('carga df_cliente_sin_duplicados', df_cliente_sin_duplicados.count())
carga_tabla_silver(df_cliente_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CLIENTE)
#Estado del Contrato
df_contrato_estado = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CONTRATO_ESTADO}""") 
df_contrato_estado_sin_duplicados = df_contrato_estado.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Estado"),col("descripcion").alias("Descripcion"))
print('carga df_contrato_estado_sin_duplicados', df_contrato_estado_sin_duplicados.count())
carga_tabla_silver(df_contrato_estado_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CONTRATO_ESTADO)
#Tipo del Contrato
df_contrato_tipo = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CONTRATO_TIPO}""") 
df_contrato_tipo_sin_duplicados = df_contrato_tipo.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Tipo"),col("descripcion").alias("Descripcion"))
print('carga df_contrato_tipo_sin_duplicados', df_contrato_tipo_sin_duplicados.count())
carga_tabla_silver(df_contrato_tipo_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CONTRATO_TIPO)
#Tipo del Contrato
df_contrato_tipoclasificacion = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CONTRATO_TIPOCLASIFICACION}""") 
df_contrato_tipoclasificacion_sin_duplicados = df_contrato_tipoclasificacion.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoClasificacion"),col("descripcion").alias("Descripcion"))
print('carga df_contrato_tipoclasificacion_sin_duplicados', df_contrato_tipoclasificacion_sin_duplicados.count())
carga_tabla_silver(df_contrato_tipoclasificacion_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CONTRATO_TIPOCLASIFICACION)
#Tipo del Contrato
df_contrato_tipoespacio = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CONTRATO_TIPOESPACIO}""") 
df_contrato_tipoespacio_sin_duplicados = df_contrato_tipoespacio.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoEspacio"),col("descripcion").alias("Descripcion"))
print('carga df_contrato_tipoespacio_sin_duplicados', df_contrato_tipoespacio_sin_duplicados.count())
carga_tabla_silver(df_contrato_tipoespacio_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CONTRATO_TIPOESPACIO)
df_contrato_tipomoneda = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CONTRATO_TIPOMONEDA}""") 
df_contrato_tipomoneda_sin_duplicados = df_contrato_tipomoneda.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoMoneda"),col("descripcion").alias("Descripcion"))
print('carga df_contrato_tipomoneda_sin_duplicados', df_contrato_tipomoneda_sin_duplicados.count())
carga_tabla_silver(df_contrato_tipomoneda_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CONTRATO_TIPOMONEDA)
df_contrato_tiposervicio = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CONTRATO_TIPOSERVICIO}""") 
df_contrato_tiposervicio_sin_duplicados = df_contrato_tiposervicio.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoServicio"),col("descripcion").alias("Descripcion"))
print('carga df_contrato_tiposervicio_sin_duplicados', df_contrato_tiposervicio_sin_duplicados.count())
carga_tabla_silver(df_contrato_tiposervicio_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CONTRATO_TIPOSERVICIO)
#Contrato
df_contrato = spark.table(f"""{bdawscobbr}.{CONS_TABLA_PE_AGREEMENT}""") 
df_contrato_sin_duplicados = df_contrato.dropDuplicates(["uid"]).select(col("uid").alias("Cod_Contrato"),col("number").alias("NroContrato"),col("gen_person_uid").alias("Cod_Cliente"),col("status").alias("Cod_Estado"), \
                                                                        col("type").alias("Cod_Tipo"),col("typeservice").alias("Cod_TipoServicio"), col("capacity").alias("Capacidad"), col("currentcapacity").alias("ActualCapacidad"), \
                                                                        col("qtyuses").alias("NroUsos"),col("currencyid").alias("Cod_TipoMoneda"),col("listprice").alias("PrecioLista"),col("discount").alias("Descuento"), \
                                                                        col("additionaldiscount").alias("DescuentoAdi"),col("discount3").alias("DescuentoProm1"),col("discount4").alias("DescuentoProm2"), \
                                                                        col("advancepayment").alias("Anticipo"),col("signaturedate").alias("FechaFirma"),col("appcreationby").alias("UsuarioRegistro"), \
                                                                        col("appcreationdate").alias("FechaVenta"), col("category").alias("Cod_TipoEspacio"),col("skipgraceperiod").alias("FlagPeriodoCarencia"), \
                                                                        col("composetype").alias("Cod_TipoClasificacion"),col("graceperioddays").alias("DiasCarencia"),col("annuldate").alias("FechaAnulacion"))
print('carga df_contrato_sin_duplicados', df_contrato_sin_duplicados.count())
carga_tabla_silver(df_contrato_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CONTRATO)
#Estado del Contrato
df_cronograma_estado = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_CRONOGRAMA_ESTADO}""") 
df_cronograma_estado_sin_duplicados = df_cronograma_estado.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Estado"),col("descripcion").alias("Descripcion"))
print('carga df_cronograma_estado_sin_duplicados', df_cronograma_estado_sin_duplicados.count())
carga_tabla_silver(df_cronograma_estado_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CRONOGRAMA_ESTADO)
df_financiamiento_modopago = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_FINANCIAMIENTO_MODOPAGO}""") 
df_financiamiento_modopago_sin_duplicados = df_financiamiento_modopago.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_ModoPago"),col("descripcion").alias("Descripcion"))
print('carga df_financiamiento_modopago_sin_duplicados', df_financiamiento_modopago_sin_duplicados.count())
carga_tabla_silver(df_financiamiento_modopago_sin_duplicados,ruta_dest,CONST_OUT_TABLA_FINANCIAMIENTO_MODOPAGO)
df_financiamiento_estado = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_FINANCIAMIENTO_ESTADO}""") 
df_financiamiento_estado_sin_duplicados = df_financiamiento_estado.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Estado"),col("descripcion").alias("Descripcion"))
print('carga df_financiamiento_estado_sin_duplicados', df_financiamiento_estado_sin_duplicados.count())
carga_tabla_silver(df_financiamiento_estado_sin_duplicados,ruta_dest,CONST_OUT_TABLA_FINANCIAMIENTO_ESTADO)
df_liquidacion_motivo = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_LIQUIDACION_MOTIVO}""") 
df_liquidacion_motivo_sin_duplicados = df_liquidacion_motivo.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Motivo"),col("descripcion").alias("Descripcion"))
print('carga df_liquidacion_motivo_sin_duplicados', df_liquidacion_motivo_sin_duplicados.count())
carga_tabla_silver(df_liquidacion_motivo_sin_duplicados,ruta_dest,CONST_OUT_TABLA_LIQUIDACION_MOTIVO)
df_liquidacion_representante = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_LIQUIDACION_REPRESENTANTE}""") 
df_liquidacion_representante_sin_duplicados = df_liquidacion_representante.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Representante"),col("descripcion").alias("Descripcion"))
print('carga df_liquidacion_representante_sin_duplicados', df_liquidacion_representante_sin_duplicados.count())
carga_tabla_silver(df_liquidacion_representante_sin_duplicados,ruta_dest,CONST_OUT_TABLA_LIQUIDACION_REPRESENTANTE)
df_liquidacion_tipoliquidacion = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_LIQUIDACION_TIPOLIQUIDACION}""") 
df_liquidacion_tipoliquidacion_sin_duplicados = df_liquidacion_tipoliquidacion.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoLiquidacion"),col("descripcion").alias("Descripcion"))
print('carga df_liquidacion_tipoliquidacion_sin_duplicados', df_liquidacion_tipoliquidacion_sin_duplicados.count())
carga_tabla_silver(df_liquidacion_tipoliquidacion_sin_duplicados,ruta_dest,CONST_OUT_TABLA_LIQUIDACION_TIPOLIQUIDACION)
df_pagos_estado = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_PAGOS_ESTADO}""") 
df_pagos_estado_sin_duplicados = df_pagos_estado.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Estado"),col("descripcion").alias("Descripcion"))
print('carga df_pagos_estado_sin_duplicados', df_pagos_estado_sin_duplicados.count())
carga_tabla_silver(df_pagos_estado_sin_duplicados,ruta_dest,CONST_OUT_TABLA_PAGOS_ESTADO)
df_pagos_tipocomprobante = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_PAGOS_TIPOCOMPROBANTE}""") 
df_pagos_tipocomprobante_sin_duplicados = df_pagos_tipocomprobante.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoComprobante"),col("descripcion").alias("Descripcion"))
print('carga df_pagos_tipocomprobante_sin_duplicados', df_pagos_tipocomprobante_sin_duplicados.count())
carga_tabla_silver(df_pagos_tipocomprobante_sin_duplicados,ruta_dest,CONST_OUT_TABLA_PAGOS_TIPOCOMPROBANTE)
df_paquete_tipo = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_PAQUETE_TIPO}""") 
df_paquete_tipo_sin_duplicados = df_paquete_tipo.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Tipo"),col("descripcion").alias("Descripcion"))
print('carga df_paquete_tipo_sin_duplicados', df_paquete_tipo_sin_duplicados.count())
carga_tabla_silver(df_paquete_tipo_sin_duplicados,ruta_dest,CONST_OUT_TABLA_PAQUETE_TIPO)
df_paquete_tiposervicio = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_PAQUETE_TIPOSERVICIO}""") 
df_paquete_tiposervicio_sin_duplicados = df_paquete_tiposervicio.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoServicio"),col("descripcion").alias("Descripcion"))
print('carga df_paquete_tiposervicio_sin_duplicados', df_paquete_tiposervicio_sin_duplicados.count())
carga_tabla_silver(df_paquete_tiposervicio_sin_duplicados,ruta_dest,CONST_OUT_TABLA_PAQUETE_TIPOSERVICIO)
df_paquete = spark.table(f"""{bdawscobbr}.{CONS_TABLA_GEN_PACKAGE}""") 
df_paquete_sin_duplicados = df_paquete.dropDuplicates(["uid"]).select(col("uid").alias("Cod_Paquete"),col("name").alias("Descripcion"),col("type").alias("Cod_Tipo"),col("typeservice").alias("Cod_TipoServicio"), \
                                                                      col("saleprice").alias("PrecioVenta"),col("listprice").alias("PrecioLista"),col("isactive").alias("FlagActivo"))
print('carga df_paquete_sin_duplicados', df_paquete_sin_duplicados.count())
carga_tabla_silver(df_paquete_sin_duplicados,ruta_dest,CONST_OUT_TABLA_PAQUETE)
df_piedras_estado = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_PIEDRAS_ESTADO}""") 
df_piedras_estado_sin_duplicados = df_piedras_estado.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Estado"),col("descripcion").alias("Descripcion"))
print('carga df_piedras_estado_sin_duplicados', df_piedras_estado_sin_duplicados.count())
carga_tabla_silver(df_piedras_estado_sin_duplicados,ruta_dest,CONST_OUT_TABLA_PIEDRAS_ESTADO)
df_piedras_tipo = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_PIEDRAS_TIPO}""") 
df_piedras_tipo_sin_duplicados = df_piedras_tipo.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_Tipo"),col("descripcion").alias("Descripcion"))
print('carga df_piedras_tipo_sin_duplicados', df_piedras_tipo_sin_duplicados.count())
carga_tabla_silver(df_piedras_tipo_sin_duplicados,ruta_dest,CONST_OUT_TABLA_PIEDRAS_TIPO)
df_piedras_tipoliberacion = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_PIEDRAS_TIPOLIBERACION}""") 
df_piedras_tipoliberacion_sin_duplicados = df_piedras_tipoliberacion.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoLiberacion"),col("descripcion").alias("Descripcion"))
print('carga df_piedras_tipoliberacion_sin_duplicados', df_piedras_tipoliberacion_sin_duplicados.count())
carga_tabla_silver(df_piedras_tipoliberacion_sin_duplicados,ruta_dest,CONST_OUT_TABLA_PIEDRAS_TIPOLIBERACION)
df_piedras_tipoorden = spark.table(f"""{bdawscobbr}.{CONS_TABLA_DE_PIEDRAS_TIPOORDEN}""") 
df_piedras_tipoorden_sin_duplicados = df_piedras_tipoorden.dropDuplicates(["codigo"]).select(col("codigo").alias("Cod_TipoOrden"),col("descripcion").alias("Descripcion"))
print('carga df_piedras_tipoorden_sin_duplicados', df_piedras_tipoorden_sin_duplicados.count())
carga_tabla_silver(df_piedras_tipoorden_sin_duplicados,ruta_dest,CONST_OUT_TABLA_PIEDRAS_TIPOORDEN)
df_paquete_producto= spark.table(f"""{bdawscobbr}.{CONS_TABLA_GEN_PACKAGE_AGREEMENTITEM}""")
#df_paquete_producto_sin_duplicados = df_paquete_producto.dropDuplicates(["gen_package_agreement_uid","gen_additional_items_uid"]).select(col("gen_package_agreement_uid").alias("Cod_PaqueteProducto"), \
#                                                                                           col("gen_additional_items_uid").alias("Cod_Producto"),col("Name").alias("NombreProducto"), \
#                                                                                           col("Price").alias("Precio"),col("Quanty").alias("Cantidad"),col("Amount").alias("Monto"), \
#                                                                                           col("Type").alias("Tipo"))
df_paquete_producto_sin_duplicados = df_paquete_producto.select(col("gen_package_agreement_uid").alias("Cod_PaqueteProducto"), \
                                                                                           col("gen_additional_items_uid").alias("Cod_Producto"),col("Name").alias("NombreProducto"), \
                                                                                           col("Price").alias("Precio"),col("Quanty").alias("Cantidad"),col("Amount").alias("Monto"), \
                                                                                           col("Type").alias("Tipo"))
print('carga df_paquete_producto_sin_duplicados', df_paquete_producto_sin_duplicados.count())
carga_tabla_silver(df_paquete_producto_sin_duplicados,ruta_dest,CONST_OUT_TABLA_FINANCIAMIENTO_PRODUCTO)
#ft_adendas
df_f_adendas = spark.sql(f"""
select Uid Cod_Adenda
        ,AgreementUid Cod_Contrato
        ,type Cod_TipoAdenda --de_Adenda_Tipo
        ,Status Cod_Estado   --de_Adenda_Estado
        ,NewGenPackageAgreementUid Cod_PaqueteNew
        ,GenPackageAgreementUid Cod_Paquete
        ,GracePeriodDays DiasCarencia
        ,Amount PrecioAdenda
        ,EndorsmentOperation Cod_OperacionAlterno    --de_Adenda_OperacionAlterno
        ,AppCreationDate FechaAdenda
        ,Comments Comentario
from {bdawscobbr}.{CONS_TABLA_PE_AGREEMENT_MODIFICATION} """)
print('carga df_f_adendas', df_f_adendas.count())
carga_tabla_silver(df_f_adendas,ruta_dest,CONST_OUT_TABLA_F_ADENDAS)
#ft_liquidaciones 
df_f_liquidaciones = spark.sql(f"""
select   Uid as Cod_Liquidacion
        ,TypeSale as Cod_Tipo --de_Liquidacion_TipoLiquidacion
        ,AgreementUid as Cod_Contrato
        ,ReasonToSaleID as Cod_Motivo  --de_Liquidacion_Motivo                                              
        ,PersonLegalRepresantUid as Cod_Representante    --de_Liquidacion_Representante
        ,ListPrice ListaPrecio
        ,PaidAmount MontoPagado
        ,InterestOverDue InteresMoratorio
        ,PenaltyAmount MontoPenalidad
        ,AdminFees GastoAdministrativo
        ,ToPayClient SaldoaFavor
        ,Comments Comentario
        ,AppCreationDate FechaLiquidacion
        ,AppCreationBy NombreUsuario
from {bdawscobbr}.{CONS_TABLA_PE_CLIENT_SALE_AGREEMENT} """)
print('carga df_f_liquidaciones', df_f_liquidaciones.count())
carga_tabla_silver(df_f_liquidaciones,ruta_dest,CONST_OUT_TABLA_F_LIQUIDACIONES)
#ft_orden_piedras 
df_f_orden_placa = spark.sql(f"""
select   a.AgreementUid as Cod_Contrato
        ,a.Uid as Cod_Placa
        ,a.TYPE as Cod_TipoOrden --de_Piedras_Tipo
        ,a.ToDateInterestOverDue as InteresMorosoIni
        ,a.ToDateAmountOverDue as MontoMorosoIni
        ,a.Status as Cod_Estado  --de_Piedras_Estado
        ,a.ShouldByPayCount as MinCuotasxPagar
        ,a.SchedulesLateCount as CuotasAtrasadas
        ,a.RepoExpectedInterest as InteresaRecuperar
        ,a.RepoExpectedAmount as MontoaRecuperar
        ,a.ReleaseType as Cod_TipoLiberacion --de_Piedras_TipoLiberacion
        ,a.PersonProcessUid as OperarioEncargado
        ,a.PaidInterest as InteresPagado
        ,a.PaidAmount as MontoPagado
        ,a.NotificationDate as FechaNotificacion
        ,a.LastPaymentDate as FechaUltimoPago
        ,a.InterestOverDue as InteresMoratorioTotal
        ,a.CountNotify as CantNotificaciones 
        ,a.CompleteDate as FechaProceso
        ,a.Comments as Comentarios 
        ,a.ClientSaleAgreementUid as Cod_Liquidacion
        ,a.BuildDate as FechaRegistro
        ,a.AppTimeStamp as FechaActualizacion
        ,a.AppLastUpdateBy as UsuarioActualizacion
        ,a.AmountOverDue as MontoMoroso
from {bdawscobbr}.{CONS_TABLA_PE_COLLECTION_ORDER_STONE} a """)
print('carga df_f_orden_placa', df_f_orden_placa.count())
carga_tabla_silver(df_f_orden_placa,ruta_dest,CONST_OUT_TABLA_F_ORDEN_PLACA)
#ft_pagos
df_f_pagos = spark.sql(f"""
select   c.ParentUid Cod_Contrato
        ,c.Uid AS Cod_Pago
        ,c.PaidAmount as Monto
        ,c.CashAmount as MontoEfectivo
        ,c.CreditCardAmount as MontoTarjeta
        ,c.DepositAmount as MontoDeposito
        ,c.CasherUserName as NombreCajero
        ,c.DateReceived as FechaPago
        ,c.Comments as Comentario
        ,c.Reference as ReferenciaPago
        ,c.ReserveAmount as MontoReserva
        ,c.Status as Cod_Estado  --faltan identificar valores 
        ,c.TransactionID as OnlineCode
        ,c.TypePmtDescription as TipoPago
        ,c.VoucherType as Cod_TipoComprobante --de_Pagos_TipoComprobante
from {bdawscobbr}.{CONS_TABLA_PE_PAYMENT} c """)

print('carga df_f_pagos', df_f_pagos.count())
carga_tabla_silver(df_f_pagos,ruta_dest,CONST_OUT_TABLA_F_PAGOS)
#ft_cronograma
df_f_cronograma = spark.sql(f"""
WITH PaymentScheduleWithRowNum AS (
    SELECT
        a.pe_financing_Uid,
        a.number,
        a.Uid AS pe_payment_schedule_Uid_schedule, -- Renombrado para evitar conflicto con el UID del split
        a.PaymentDate,
        a.status,
        a.Description,
        a.ToPrincipal,
        a.TaxAmount,
        a.NextDueDate,
        a.PrincipalAmountDue,
        a.AmountDue,
        a.ForceAllowToPayment,
        a.IsManualInterest,
        a.IsToPayOnlyPrincipal,
        a.Amount,
        a.PaidAmount,
        b.pe_payment_Uid AS Cod_Pago,
        b.Reference AS DescripcionPago,
        b.pe_payment_schedule_Uid AS CodCronogramaSplit, -- Renombrado para claridad
        ROW_NUMBER() OVER (PARTITION BY a.Uid ORDER BY b.pe_payment_Uid) AS rn -- Ordena por un campo del split para definir el "primero"
    FROM
        {bdawscobbr}.{CONS_TABLA_PE_PAYMENT_SCHEDULE} a
    LEFT JOIN
        {bdawscobbr}.{CONS_TABLA_PE_PAYMENT_SPLIT} b ON a.Uid = b.pe_payment_schedule_Uid
   -- WHERE
       -- a.pe_financing_Uid = 26042
)
SELECT
    psr.pe_financing_Uid AS Cod_Financiamiento,
    psr.number AS NroCuota,
    psr.CodCronogramaSplit AS Cod_Cronograma, -- Usamos el UID del split para identificar la cuota-pago
    psr.Cod_Pago,
    psr.DescripcionPago,
    psr.Status Cod_Estado,  
    CASE WHEN psr.rn = 1 THEN psr.Description ELSE NULL END AS Descripcion,
    CASE WHEN psr.rn = 1 THEN psr.ToPrincipal ELSE NULL END AS MontoCapital,
    CASE WHEN psr.rn = 1 THEN psr.TaxAmount ELSE NULL END AS ImpuestoIGV,
    CASE WHEN psr.rn = 1 THEN psr.NextDueDate ELSE NULL END AS FechaVencimiento,
    CASE WHEN psr.rn = 1 THEN psr.PaymentDate ELSE NULL END AS FechaPago,
    CASE WHEN psr.rn = 1 THEN psr.PrincipalAmountDue ELSE NULL END AS SaldoDeudor,
    CASE WHEN psr.rn = 1 THEN psr.AmountDue ELSE NULL END AS SaldoCuota,
    CASE WHEN psr.rn = 1 THEN psr.ForceAllowToPayment ELSE NULL END AS AutPago,
    CASE WHEN psr.rn = 1 THEN psr.IsManualInterest ELSE NULL END AS InteresManual,
    CASE WHEN psr.rn = 1 THEN psr.IsToPayOnlyPrincipal ELSE NULL END AS ValorDesagio,
    CASE WHEN psr.rn = 1 THEN psr.Amount ELSE NULL END AS MontoCuota,
    CASE WHEN psr.rn = 1 THEN psr.PaidAmount ELSE NULL END AS MontoPago
FROM
    PaymentScheduleWithRowNum psr
ORDER BY
    psr.pe_financing_Uid, psr.number, psr.Cod_Pago; -- Ordena para ver los resultados agrupados 
""")
print('carga df_f_cronograma', df_f_cronograma.count())
carga_tabla_silver(df_f_cronograma,ruta_dest,CONST_OUT_TABLA_F_CRONOGRAMA)
#ft_financiamiento
# por validar fact_agreement_history porque los datos de esta tabla se estarian duplicando 

df_f_financiamiento = spark.sql(f"""
select b.Uid as Cod_Financiamiento
     , b.ParentUid Cod_Contrato
     , d.gen_package_Uid Cod_Paquete
     , d.Uid as Cod_PaqueteProducto
     , b.Type as Cod_ModoPago --de_Financiamiento_ModoPago confirmar
     , b.Status as Cod_Estado
     , b.TotalAmount as PrecioFinal
     , b.InitAmount as CuotaInicial
     , b.AmountTo as MontoFinanciado
     , b.InterestRate as TasaInteres
     , b.FCMTotal as MontoFCM
     , b.InitQty as NroCuotasInicial
     , b.NumberPayment as NroCuotas
     , b.Payment as ValorCuota
     , b.PaymentDay as FechaPago
     , b.AppCreationDate as FechaFinanciamiento
     , b.FirstInitDate as FechaPrimerPago
     , b.Comments as Observaciones
     ,c.Schedulespendingcount as Cuotaspendientes
     ,c.Schedulespendingamount as Deudapendiente
     ,c.Scheduleslatecount as Cuotasmorosas
     ,c.Pendingamount as Carteratotal
     ,c.Paidinterest as Interescompagado
     ,c.Paidamount as Totalpagado
     ,c.Lastpaymentdate as Fechaultimopago
     ,c.Interestoverdue as Interesmoroso
     ,c.Daysoverdue as Diasatraso
     ,c.Chargespaidamount as Cargospagados
     ,c.Chargesinterestoverdue as Cargosinteresmoroso
     ,c.Chargesdaysoverdue as Cargosdiasatrasados
     ,c.Chargeschedulespendingcount as Cargoscuotaspendientes
     ,c.Chargeschedulespendingamount as Cargospendientes
     ,c.Chargescheduleslatecount as Cargoscuotasmorosas
     ,c.Chargesamountoverdue as Cargosmorosos
     ,c.Chargelastpaymentdate as Fechaultimopagocargos
     ,c.Amountoverdue as Totalmoroso     
from {bdawscobbr}.{CONS_TABLA_PE_FINANCING} b --un contrato puede tener mas de un financiamiento
    left join {bdawscobbr}.{CONS_TABLA_FACT_AGREEMENT_HISTORY} c on c.AgreementKey = b.ParentUid --por contrato
    left join {bdawscobbr}.{CONS_TABLA_GEN_PACKAGE_AGREEMENT} d on d.pe_Agreement_Uid = b.ParentUid --por contrato
    """)
print('carga df_f_financiamiento', df_f_financiamiento.count())
carga_tabla_silver(df_f_financiamiento,ruta_dest,CONST_OUT_TABLA_F_FINANCIAMIENTO)
#Churn
df_churn = spark.table(f"""{bdawscobbr}.{CONS_TABLA_PE_AGREEMENT}""") 
df_churn_sin_duplicados = df_churn.dropDuplicates(["uid"]).select(current_timestamp().alias("FechaFoto"),col("uid").alias("Cod_Contrato"),col("number").alias("NroContrato"),col("gen_person_uid").alias("Cod_Cliente"),col("status").alias("Cod_Estado"), \
                                                                        col("type").alias("Cod_Tipo"),col("typeservice").alias("Cod_TipoServicio"), col("capacity").alias("Capacidad"), col("currentcapacity").alias("ActualCapacidad"), \
                                                                        col("qtyuses").alias("NroUsos"),col("currencyid").alias("Cod_TipoMoneda"),col("listprice").alias("PrecioLista"),col("discount").alias("Descuento"), \
                                                                        col("additionaldiscount").alias("DescuentoAdi"),col("discount3").alias("DescuentoProm1"),col("discount4").alias("DescuentoProm2"), \
                                                                        col("advancepayment").alias("Anticipo"),col("signaturedate").alias("FechaFirma"),col("appcreationby").alias("UsuarioRegistro"), \
                                                                        col("appcreationdate").alias("FechaVenta"), col("category").alias("Cod_TipoEspacio"),col("skipgraceperiod").alias("FlagPeriodoCarencia"), \
                                                                        col("composetype").alias("Cod_TipoClasificacion"),col("graceperioddays").alias("DiasCarencia"),col("annuldate").alias("FechaAnulacion"))
print('carga df_churn_sin_duplicados', df_churn_sin_duplicados.count())
carga_tabla_silver_churn(df_churn_sin_duplicados,ruta_dest,CONST_OUT_TABLA_CHURN)
job.commit()
print('fin de job')
job.commit()