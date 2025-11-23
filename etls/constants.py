"""Constantes compartidas para rutas S3 del Data Lake Chinook.

Centraliza nombres de prefijos usados por los ETLs para evitar
inconsistencias y facilitar futuros cambios.
"""

import os

DEFAULT_BUCKET = os.getenv("S3_BUCKET", "chinook-datalake")

# Prefijos RAW (cada tabla en su carpeta parquet partition-ready)
RAW_INVOICE = "raw/Invoice/"
RAW_INVOICE_LINE = "raw/InvoiceLine/"
RAW_TRACK = "raw/Track/"
RAW_ALBUM = "raw/Album/"
RAW_ARTIST = "raw/Artist/"
RAW_CUSTOMER = "raw/Customer/"
RAW_EMPLOYEE = "raw/Employee/"
RAW_GENRE = "raw/Genre/"
RAW_MEDIA_TYPE = "raw/MediaType/"
RAW_PLAYLIST = "raw/Playlist/"
RAW_PLAYLIST_TRACK = "raw/PlaylistTrack/"
RAW_CUSTOMER_EMPLOYEE_HISTORY = "raw/customer_employee_history/"

# Prefijos PROCESSED (datasets analíticos finales)
PROC_VENTAS_DIA = "processed/ventas_por_dia/"
PROC_ARTISTA_MES = "processed/artista_mes/"
PROC_DIA_SEMANA = "processed/dia_semana/"
PROC_MES_VENTAS = "processed/mes_ventas/"

__all__ = [
    "DEFAULT_BUCKET",
    "RAW_INVOICE","RAW_INVOICE_LINE","RAW_TRACK","RAW_ALBUM","RAW_ARTIST",
    "RAW_CUSTOMER","RAW_EMPLOYEE","RAW_GENRE","RAW_MEDIA_TYPE","RAW_PLAYLIST",
    "RAW_PLAYLIST_TRACK","RAW_CUSTOMER_EMPLOYEE_HISTORY",
    "PROC_VENTAS_DIA","PROC_ARTISTA_MES","PROC_DIA_SEMANA","PROC_MES_VENTAS"
]
