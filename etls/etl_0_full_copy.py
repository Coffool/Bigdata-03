import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from datetime import datetime

# Args
args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket"])
bucket = args["bucket"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")


# 1) Obtener todas las tablas del catálogo JDBC
tables = [
    "Album", "Artist", "Customer", "Employee", "Genre",
    "Invoice", "InvoiceLine", "MediaType", "Playlist",
    "PlaylistTrack", "Track"
]

# 2) Procesar cada tabla del modelo transaccional
for table in tables:
    print(f"Procesando tabla {table}...")
    dyf = glueContext.create_dynamic_frame_from_options(
        connection_type="mysql",
        connection_options={
            "connectionName": "Jdbc connection",
            "dbtable": table
        },
        format="jdbc"
    )

    # Guardar en raw/<tabla>/snapshot=<timestamp>/
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": f"s3://{bucket}/raw/{table}/snapshot={timestamp}/"
        }
    )

# 3) Histórico cliente-empleado
query = """
SELECT 
    c.CustomerId,
    c.FirstName AS CustomerFirstName,
    c.LastName AS CustomerLastName,
    c.Email AS CustomerEmail,
    c.Company,
    c.Country AS CustomerCountry,
    c.SupportRepId,
    e.FirstName AS EmployeeFirstName,
    e.LastName AS EmployeeLastName,
    e.Title AS EmployeeTitle,
    e.ReportsTo,
    m.FirstName AS ManagerFirstName,
    m.LastName AS ManagerLastName,
    m.Title AS ManagerTitle
FROM Customer c
LEFT JOIN Employee e ON c.SupportRepId = e.EmployeeId
LEFT JOIN Employee m ON e.ReportsTo = m.EmployeeId
"""

history = glueContext.create_dynamic_frame_from_options(
    connection_type="mysql",
    connection_options={
        "connectionName": "Jdbc connection",
        "query": query
    },
    format="jdbc"
)

glueContext.write_dynamic_frame.from_options(
    frame=history,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": f"s3://{bucket}/raw/customer_employee_history/snapshot={timestamp}/"
    }
)

job.commit()
