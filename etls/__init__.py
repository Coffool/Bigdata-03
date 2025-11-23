"""
ETLs para el proyecto Chinook Data Lake

Este paquete contiene los ETLs para transformar datos transaccionales
en datos analíticos para el data lake.
"""

try:
    from .etl_0_full_copy import run_full_copy_etl  # Puede requerir entorno Glue
except ImportError:  # awsglue/pyspark no disponible en entorno local
    run_full_copy_etl = None
try:
    from .etl_1_ventas_por_dia import run_sales_per_day_etl
except ImportError:
    run_sales_per_day_etl = None
try:
    from .etl_2_artista_mas_vendido import run_top_artist_per_month_etl
except ImportError:
    run_top_artist_per_month_etl = None
try:
    from .etl_3_dia_semana import run_weekday_analysis_etl
except ImportError:
    run_weekday_analysis_etl = None
try:
    from .etl_4_mes_mayor_ventas import run_monthly_analysis_etl
except ImportError:
    run_monthly_analysis_etl = None

__all__ = [
    name for name, ref in [
        ('run_full_copy_etl', run_full_copy_etl),
        ('run_sales_per_day_etl', run_sales_per_day_etl),
        ('run_top_artist_per_month_etl', run_top_artist_per_month_etl),
        ('run_weekday_analysis_etl', run_weekday_analysis_etl),
        ('run_monthly_analysis_etl', run_monthly_analysis_etl)
    ] if ref is not None
]

__version__ = '1.0.0'
