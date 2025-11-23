import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Args
args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket"])
bucket = args["bucket"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# 1) Cargar Invoice
invoice = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [f"s3://{bucket}/raw/Invoice/"]
    }
).toDF()

# 2) Cargar InvoiceLine
invoice_line = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [f"s3://{bucket}/raw/InvoiceLine/"]
    }
).toDF()

# 3) Join
df = invoice.join(invoice_line, "InvoiceId")

# 4) Convertir fecha
df = df.withColumn("fecha", F.to_date("InvoiceDate"))

# 5) Agregación por fecha
agg = df.groupBy("fecha").agg(
    F.count("TrackId").alias("total_canciones_vendidas"),
    F.sum("Quantity").alias("cantidad_total"),
    F.countDistinct("InvoiceId").alias("numero_facturas"),
    F.countDistinct("CustomerId").alias("clientes_unicos"),
    F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("monto_total")
)

# 6) Enriquecimiento
agg = (
    agg.withColumn("dia_semana", F.date_format("fecha", "EEEE"))
       .withColumn("mes", F.month("fecha"))
       .withColumn("año", F.year("fecha"))
       .withColumn("trimestre", F.quarter("fecha"))
       .withColumn("avg_canciones_por_factura",
                   F.col("total_canciones_vendidas") / F.col("numero_facturas"))
       .withColumn("ticket_promedio",
                   F.col("monto_total") / F.col("numero_facturas"))
)

# 7) Guardar en processed/
output_path = f"s3://{bucket}/processed/ventas_por_dia/snapshot={timestamp}/"

(
    agg.write
       .mode("overwrite")
       .parquet(output_path)
)

print("✓ ETL 1 completado exitosamente")
print(f"Datos guardados en: {output_path}")

job.commit()
