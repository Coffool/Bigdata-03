import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from etls.constants import RAW_INVOICE, RAW_INVOICE_LINE, PROC_MES_VENTAS

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket"])
bucket = args["bucket"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

invoice = glueContext.create_dynamic_frame_from_options(
    connection_type="s3", format="parquet",
    connection_options={"paths": [f"s3://{bucket}/{RAW_INVOICE}"]}
).toDF()
invoice_line = glueContext.create_dynamic_frame_from_options(
    connection_type="s3", format="parquet",
    connection_options={"paths": [f"s3://{bucket}/{RAW_INVOICE_LINE}"]}
).toDF()

df = invoice.join(invoice_line, "InvoiceId").withColumn("InvoiceDate", F.to_timestamp("InvoiceDate"))
df = df.withColumn("year_month", F.date_format("InvoiceDate", "yyyy-MM"))

agg = df.groupBy("year_month").agg(
    F.count("TrackId").alias("total_canciones_vendidas"),
    F.sum("Quantity").alias("cantidad_total"),
    F.countDistinct("InvoiceId").alias("numero_facturas"),
    F.countDistinct("CustomerId").alias("clientes_unicos"),
    F.sum(F.col("UnitPrice") * F.col("Quantity")).alias("monto_total")
).withColumn("avg_canciones_por_factura", F.col("total_canciones_vendidas")/F.col("numero_facturas")) \
 .withColumn("ticket_promedio", F.col("monto_total")/F.col("numero_facturas"))

output_path = f"s3://{bucket}/{PROC_MES_VENTAS}snapshot={timestamp}/"
agg.write.mode("overwrite").parquet(output_path)

print("✓ ETL 4 completado exitosamente")
print(f"Datos guardados en: {output_path}")

job.commit()
