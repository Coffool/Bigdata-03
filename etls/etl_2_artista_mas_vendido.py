import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from etls.constants import (
    RAW_INVOICE, RAW_INVOICE_LINE, RAW_TRACK, RAW_ALBUM, RAW_ARTIST, PROC_ARTISTA_MES
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket"])
bucket = args["bucket"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Cargar tablas raw necesarias
invoice = glueContext.create_dynamic_frame_from_options(
    connection_type="s3", format="parquet",
    connection_options={"paths": [f"s3://{bucket}/{RAW_INVOICE}"]}
).toDF()
invoice_line = glueContext.create_dynamic_frame_from_options(
    connection_type="s3", format="parquet",
    connection_options={"paths": [f"s3://{bucket}/{RAW_INVOICE_LINE}"]}
).toDF()
track = glueContext.create_dynamic_frame_from_options(
    connection_type="s3", format="parquet",
    connection_options={"paths": [f"s3://{bucket}/{RAW_TRACK}"]}
).toDF()
album = glueContext.create_dynamic_frame_from_options(
    connection_type="s3", format="parquet",
    connection_options={"paths": [f"s3://{bucket}/{RAW_ALBUM}"]}
).toDF()
artist = glueContext.create_dynamic_frame_from_options(
    connection_type="s3", format="parquet",
    connection_options={"paths": [f"s3://{bucket}/{RAW_ARTIST}"]}
).toDF()

# Joins para asociar artista a cada línea de factura
sales = invoice_line.join(invoice, "InvoiceId") \
    .join(track, "TrackId", "left") \
    .join(album, "AlbumId", "left") \
    .join(artist, "ArtistId", "left")

sales = sales.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate")).withColumn("mes", F.trunc("InvoiceDate", "month"))

# Métricas por artista y mes
artist_month = sales.groupBy("mes", "ArtistId", "Name").agg(
    F.count("TrackId").alias("total_canciones_vendidas"),
    F.sum("Quantity").alias("cantidad_total"),
    F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("monto_total"),
    F.countDistinct("InvoiceId").alias("numero_facturas"),
    F.countDistinct("CustomerId").alias("clientes_unicos"),
    F.countDistinct("AlbumId").alias("albums_vendidos")
).withColumn("nombre_artista", F.col("Name"))

artist_month = artist_month.withColumn("año", F.year("mes")).withColumn("numero_mes", F.month("mes")) \
    .withColumn("nombre_mes", F.date_format("mes", "MMMM"))

# Top artista por mes (window rank)
window = Window.partitionBy("mes").orderBy(F.col("total_canciones_vendidas").desc())
top_artist = artist_month.withColumn("rk", F.row_number().over(window)).filter(F.col("rk") == 1).drop("rk")

# Totales del mes para porcentajes
month_totals = artist_month.groupBy("mes").agg(
    F.sum("total_canciones_vendidas").alias("total_mes_canciones"),
    F.sum("monto_total").alias("total_mes_monto")
)
top_artist = top_artist.join(month_totals, "mes") \
    .withColumn("porcentaje_canciones", (F.col("total_canciones_vendidas")/F.col("total_mes_canciones")*100).cast("double")) \
    .withColumn("porcentaje_monto", (F.col("monto_total")/F.col("total_mes_monto")*100).cast("double")) \
    .withColumn("precio_promedio_cancion", (F.col("monto_total")/F.col("total_canciones_vendidas")).cast("double"))

output_path = f"s3://{bucket}/{PROC_ARTISTA_MES}snapshot={timestamp}/"
top_artist.write.mode("overwrite").parquet(output_path)

print("✓ ETL 2 completado exitosamente")
print(f"Datos guardados en: {output_path}")

job.commit()
