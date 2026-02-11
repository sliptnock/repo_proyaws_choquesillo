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
JOB_NAME = "nt_pe_cob_create_tables"
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
bdawscobgl    = ssm.get_parameter(Name='p_aws_cob_gl_db', WithDecryption=True)['Parameter']['Value']



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
CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO = "DIM_CONTRATO_TIPO_SERVICIO"
CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION = "DIM_CONTRATO_TIPO_CLASIFICACION"
CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO = "DIM_CONTRATO_TIPO_ESPACIO"
CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA = "DIM_CONTRATO_TIPO_MONEDA"
CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO = "DIM_CRONOGRAMA_ESTADO"
CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO = "DIM_FINANCIAMIENTO_MODO_PAGO"
CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO = "DIM_LIQUIDACION_MOTIVO"
CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE = "DIM_LIQUIDACION_REPRESENTANTE"
CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION = "DIM_LIQUIDACION_TIPO_LIQUIDACION"
CONST_OUTPUT_TABLA_PAGOS_ESTADO = "DIM_PAGOS_ESTADO"
CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE = "DIM_PAGOS_TIPO_COMPROBANTE"
CONST_OUTPUT_TABLA_PAQUETE = "DIM_PAQUETE"
CONST_OUTPUT_TABLA_PAQUETE_TIPO = "DIM_PAQUETE_TIPO"
CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO = "DIM_PAQUETE_TIPOSERVICIO"
CONST_OUTPUT_TABLA_FINANCIAMIENTOPRODUCTO = "FT_FINANCIAMIENTO_PRODUCTO" 
CONST_OUTPUT_TABLA_PIEDRAS_ESTADO = "DIM_PLACA_ESTADO"
CONST_OUTPUT_TABLA_PIEDRAS_TIPO = "DIM_PLACA_TIPO"
CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION = "DIM_PLACA_TIPO_LIBERACION"
CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN = "DIM_PLACA_TIPO_ORDEN"
CONST_OUTPUT_TABLA_CHURN = "CHURN"

CONST_OUTPUT_TABLA_FT_ADENDAS = "FT_ADENDAS"
CONST_OUTPUT_TABLA_FT_PAGOS = "FT_PAGOS"
CONST_OUTPUT_TABLA_FT_CRONOGRAMA = "FT_CRONOGRAMA"
CONST_OUTPUT_TABLA_FT_FINANCIAMIENTO = "FT_FINANCIAMIENTO"
CONST_OUTPUT_TABLA_FT_ORDEN_PLACA = "FT_ORDEN_PLACA"
CONST_OUTPUT_TABLA_FT_LIQUIDACIONES = "FT_LIQUIDACIONES"


ruta_dest = f"s3://ue1stg{amb}as3con004/TABLAS_GOLD"
print('✅ Lectura de parametros realizado con éxito ')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_ESTADO} (
  Pk_estado INT,
  Cod_estado INT,
  Estado STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_ADENDA_ESTADO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_ESTADO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO} (
  Pk_operacionesalterno INT,
  Cod_operacionesalterno INT,
  Operacionesalterno STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_OPERACIONALTERNO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_TIPO} (
  Pk_tipo INT,
  Cod_tipo INT,
  Tipo STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_ADENDA_TIPO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_ADENDA_TIPO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL} (
  Pk_estadocivil INT,
  Cod_estadocivil INT,
  Estadocivil STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_ESTADOCIVIL}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_SEXO} (
  Pk_sexo INT,
  Cod_sexo INT,
  Sexo STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_SEXO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_SEXO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO} (
  Pk_tipodocumento INT,
  Cod_tipodocumento INT,
  Tipodocumento STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPODOCUMENTO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION} (
  Pk_tipoocupacion INT,
  Cod_tipoocupacion INT,
  Tipoocupacion STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPOOCUPACION}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION} (
  Pk_tiporeligion INT,
  Cod_tiporeligion INT,
  Tiporeligion STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE_TIPORELIGION}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE} (
Pk_cliente int,
cod_cliente int,
Pk_tipodocumento int,
nrodocumento string,
nombrecompleto string,
nrotelefonocel string,
nrotelefonocasa string,
nombre string,
apellidopat string,
apellidomat string,
apellidocasado string,
correopersonal string,
fechanacimiento timestamp,
fechadefuncion timestamp,
Pk_estadocivil int,
Pk_tiporeligion int,
Pk_tipoocupacion int,
Pk_sexo int,
direccion string,
urbanizacion string,
ciudad string,
distrito string,
provincia string,
referenciadireccion string,
lugartrabajo string,
direcciontrabajo string,
correotrabajo string,
nrotelefonotrabajo string
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CLIENTE}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CLIENTE}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_ESTADO} (
  Pk_estado INT,
  Cod_estado INT,
  Estado STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_ESTADO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_ESTADO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPO} (
  Pk_tipo INT,
  Cod_tipo INT,
  Tipo STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO} (
  Pk_tiposervicio INT,
  Cod_tiposervicio INT,
  Tiposervicio STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOSERVICIO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION} (
  Pk_tipoclasificacion INT,
  Cod_tipoclasificacion INT,
  Tipoclasificacion STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOCLASIFICACION}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO} (
  Pk_tipoespacio INT,
  Cod_tipoespacio INT,
  Tipoespacio STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOESPACIO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA} (
  Pk_tipomoneda INT,
  Cod_tipomoneda INT,
  Tipomoneda STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO_TIPOMONEDA}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO} (
Pk_contrato int,
cod_contrato int,
nrocontrato string,
Pk_cliente int,
Pk_estado int,
Pk_tipo int,
Pk_tiposervicio int,
capacidad int,
actualcapacidad int,
nrousos int,
Pk_tipomoneda int,
preciolista decimal(13,4),
descuento decimal(13,4),
descuentoadi decimal(13,4),
descuentoprom1 decimal(13,4),
descuentoprom2 decimal(13,4),
anticipo decimal(13,2),
fechafirma timestamp,
usuarioregistro string,
fechaventa timestamp,
Pk_tipoespacio int,
flagperiodocarencia tinyint,
Pk_tipoclasificacion int,
diascarencia int,
fechaanulacion timestamp
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CONTRATO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CONTRATO}')


spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO} (
  Pk_estado INT,
  Cod_estado INT,
  Estado STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CRONOGRAMA_ESTADO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO} (
  Pk_modopago INT,
  Cod_modopago INT,
  Modopago STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTO_MODOPAGO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO} (
  Pk_motivo INT,
  Cod_motivo INT,
  Motivo STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_MOTIVO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE} (
  Pk_representante INT,
  Cod_representante INT,
  Representante STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_REPRESENTANTE}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION} (
  Pk_tipoliquidacion INT,
  Cod_tipoliquidacion INT,
  Tipoliquidacion STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_LIQUIDACION_TIPOLIQUIDACION}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_ESTADO} (
  Pk_estado INT,
  Cod_estado INT,
  Estado STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_PAGOS_ESTADO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_ESTADO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE} (
  Pk_tipocomprobante INT,
  Cod_tipocomprobante INT,
  Tipocomprobante STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_PAGOS_TIPOCOMPROBANTE}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO} (
  Pk_tiposervicio INT,
  Cod_tiposervicio INT,
  Tiposervicio STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPOSERVICIO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPO} (
  Pk_tipo INT,
  Cod_tipo INT,
  Tipo STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_PAQUETE_TIPO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE_TIPO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE} (
Pk_paquete int,
Cod_paquete int,
Pk_tipo int,
Pk_tiposervicio int,
Paquete string,
Precioventa decimal(13, 4),
Preciolista decimal(13, 4),
FlagActivo tinyint
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_PAQUETE}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_PAQUETE}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTOPRODUCTO} (
Pk_paqueteproducto int,
Cod_paqueteproducto int,
Cod_producto int,
Nombreproducto string,
Precio decimal(13, 4),
Cantidad int,
Monto decimal(13, 4),
Tipo string
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_FINANCIAMIENTOPRODUCTO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_FINANCIAMIENTOPRODUCTO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_ESTADO} (
  Pk_estado INT,
  Cod_estado INT,
  Estado STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_PIEDRAS_ESTADO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_ESTADO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPO} (
  Pk_tipo INT,
  Cod_tipo INT,
  Tipo STRING
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_PIEDRAS_TIPO}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPO}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION} (
Pk_tipoliberacion int,
Cod_tipoliberacion int,
Tipoliberacion string
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOLIBERACION}')

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN} (
Pk_tipoorden int,
Cod_tipoorden int,
TipoOrden string
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_PIEDRAS_TIPOORDEN}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_CHURN} (
Pk_contrato int,
cod_contrato int,
nrocontrato string,
Pk_cliente int,
Pk_estado int,
Pk_tipo int,
Pk_tiposervicio int,
capacidad int,
actualcapacidad int,
nrousos int,
Pk_tipomoneda int,
preciolista decimal(13,4),
descuento decimal(13,4),
descuentoadi decimal(13,4),
descuentoprom1 decimal(13,4),
descuentoprom2 decimal(13,4),
anticipo decimal(13,2),
fechafirma timestamp,
usuarioregistro string,
fechaventa timestamp,
Pk_tipoespacio int,
flagperiodocarencia tinyint,
Pk_tipoclasificacion int,
diascarencia int,
fechaanulacion timestamp
)
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_CHURN}'
""")
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_CHURN}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_ADENDAS} (
         Pk_Adenda INT
        ,Pk_Contrato INT
        ,Pk_Tipo INT --de_Adenda_Tipo
        ,Pk_Estado INT   --de_Adenda_Estado
        ,Pk_PaqueteNew INT
        ,Pk_Paquete INT
        ,DiasCarencia INT
        ,PrecioAdenda DECIMAL(13,4)
        ,Pk_OperacionAlterno INT   --de_Adenda_OperacionAlterno
        ,FechaAdenda TIMESTAMP
        ,Comentario STRING 
        )
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_FT_ADENDAS}' """)
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_ADENDAS}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_PAGOS} (
         Pk_Contrato int
        ,Pk_Pago int
        ,Monto DECIMAL(13,2)
        ,MontoEfectivo DECIMAL(13,2)
        ,MontoTarjeta DECIMAL(13,2)
        ,MontoDeposito DECIMAL(13,2)
        ,NombreCajero string
        ,FechaPago timestamp
        ,Comentario string
        ,ReferenciaPago string
        ,MontoReserva DECIMAL(13,2)
        ,Pk_Estado int  --faltan identificar valores 
        ,OnlineCode string
        ,TipoPago string
        ,Pk_TipoComprobante int --de_Pagos_TipoComprobante
        )
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_FT_PAGOS}' """)
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_PAGOS}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_LIQUIDACIONES} (
         Pk_Liquidacion int
        ,Pk_Tipo int --de_Liquidacion_TipoLiquidacion
        ,Pk_Contrato int
        ,Pk_Motivo int  --de_Liquidacion_Motivo                                              
        ,Pk_Representante int    --de_Liquidacion_Representante
        ,ListaPrecio DECIMAL(13,2)
        ,MontoPagado DECIMAL(13,2)
        ,InteresMoratorio DECIMAL(13,2)
        ,MontoPenalidad DECIMAL(13,2)
        ,GastoAdministrativo DECIMAL(13,2)
        ,SaldoaFavor DECIMAL(13,2)
        ,Comentario string
        ,FechaLiquidacion timestamp
        ,NombreUsuario string
        )
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_FT_LIQUIDACIONES}' """)
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_LIQUIDACIONES}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_ORDEN_PLACA} (
         Pk_Contrato int
        ,Pk_Placa int
        ,Pk_Tipo int --de_Piedras_Tipo
        ,InteresMorosoIni decimal(13,2)
        ,MontoMorosoIni decimal(13,2)
        ,Pk_Estado int --de_Piedras_Estado
        ,MinCuotasxPagar int
        ,CuotasAtrasadas int
        ,InteresaRecuperar decimal(13,2)
        ,MontoaRecuperar decimal(13,2)
        ,Pk_TipoLiberacion int --de_Piedras_TipoLiberacion
        ,OperarioEncargado int
        ,InteresPagado decimal(13,2)
        ,MontoPagado decimal(13,2)
        ,FechaNotificacion timestamp
        ,FechaUltimoPago timestamp
        ,InteresMoratorioTotal decimal(13,2)
        ,CantNotificaciones int
        ,FechaProceso timestamp
        ,Comentarios string
        ,Pk_Liquidacion int
        ,FechaRegistro timestamp
        ,FechaActualizacion timestamp
        ,UsuarioActualizacion string
        ,MontoMoroso decimal(13,2)
        )
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_FT_ORDEN_PLACA}' """)
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_ORDEN_PLACA}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_CRONOGRAMA} (
    Pk_Financiamiento int,
    NroCuota int,
    Pk_Cronograma int, -- Usamos el UID del split para identificar la cuota-pago
    Pk_Pago int,
    DescripcionPago string,
    Pk_Estado int,  
    Descripcion string,
    MontoCapital decimal(13,2),
    ImpuestoIGV decimal(13,2),
    FechaVencimiento timestamp,
    FechaPago timestamp,
    SaldoDeudor decimal(13,2),
    SaldoCuota decimal(13,2),
    AutPago tinyint,
    InteresManual tinyint,
    ValorDesagio tinyint,
    MontoCuota decimal(13,2),
    MontoPago decimal(13,2)
        )
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_FT_CRONOGRAMA}' """)
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_CRONOGRAMA}')
spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_FINANCIAMIENTO} (
      Pk_Financiamiento int
     ,Pk_Contrato int
     ,Pk_Paquete int
     ,Pk_PaqueteProducto int
     ,Pk_ModoPago int --de_Financiamiento_ModoPago confirmar
     ,PrecioFinal decimal(13,2)
     ,CuotaInicial decimal(13,2)
     ,MontoFinanciado decimal(13,2)
     ,TasaInteres decimal(5,2)
     ,MontoFCM decimal(13,2)
     ,NroCuotasInicial int
     ,NroCuotas int
     ,ValorCuota decimal(13,2)
     ,FechaPago int
     ,FechaFinanciamiento timestamp
     ,FechaPrimerPago timestamp
     ,Observaciones string
     ,Cuotaspendientes int
     ,Deudapendiente decimal(13,2)
     ,Cuotasmorosas int
     ,Carteratotal decimal(13,2)
     ,Interescompagado decimal(13,2)
     ,Totalpagado decimal(13,2)
     ,Fechaultimopago timestamp
     ,Interesmoroso decimal(13,2)
     ,Diasatraso int
     ,Cargospagados decimal(13,2)
     ,Cargosinteresmoroso decimal(13,2)
     ,Cargosdiasatrasados int
     ,Cargoscuotaspendientes int
     ,Cargospendientes decimal(13,2)
     ,Cargoscuotasmorosas int
     ,Cargosmorosos decimal(13,2)
     ,Fechaultimopagocargos timestamp
     ,Totalmoroso decimal(13,2)
     )
STORED AS PARQUET
LOCATION '{ruta_dest}/{CONST_OUTPUT_TABLA_FT_FINANCIAMIENTO}' """)
print(f'✅ tabla gold creada {bdawscobgl}.{CONST_OUTPUT_TABLA_FT_FINANCIAMIENTO}')
job.commit()
print('fin de job')
job.commit()