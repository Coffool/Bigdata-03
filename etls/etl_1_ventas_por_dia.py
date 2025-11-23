"""
ETL 1 - Ventas por Día

Este ETL calcula el número de canciones (tracks) vendidas por día
desde la tabla Invoice e InvoiceLine.

Salida: processed/ventas_por_dia/
"""

import boto3
import pymysql
import pandas as pd
from datetime import datetime
import json
import os
from typing import Dict, Optional


def get_db_connection(host: str, user: str, password: str, database: str, port: int = 3306):
    """Establece conexión con la base de datos"""
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        cursorclass=pymysql.cursors.DictCursor
    )


def extract_sales_per_day(connection) -> pd.DataFrame:
    """
    Extrae y calcula el número de canciones vendidas por día
    
    Args:
        connection: Conexión a la base de datos
    
    Returns:
        DataFrame con ventas por día
    """
    query = """
    SELECT 
        DATE(i.InvoiceDate) as fecha,
        COUNT(il.TrackId) as total_canciones_vendidas,
        SUM(il.Quantity) as cantidad_total,
        COUNT(DISTINCT i.InvoiceId) as numero_facturas,
        COUNT(DISTINCT i.CustomerId) as clientes_unicos,
        SUM(il.UnitPrice * il.Quantity) as monto_total
    FROM Invoice i
    JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
    GROUP BY DATE(i.InvoiceDate)
    ORDER BY fecha DESC
    """
    df = pd.read_sql(query, connection)
    return df


def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma y enriquece los datos de ventas
    
    Args:
        df: DataFrame con datos crudos
    
    Returns:
        DataFrame transformado
    """
    # Convertir fecha a datetime si no lo está
    df['fecha'] = pd.to_datetime(df['fecha'])
    
    # Agregar columnas adicionales
    df['dia_semana'] = df['fecha'].dt.day_name()
    df['mes'] = df['fecha'].dt.month
    df['año'] = df['fecha'].dt.year
    df['trimestre'] = df['fecha'].dt.quarter
    
    # Calcular promedio de canciones por factura
    df['avg_canciones_por_factura'] = (
        df['total_canciones_vendidas'] / df['numero_facturas']
    ).round(2)
    
    # Calcular ticket promedio
    df['ticket_promedio'] = (
        df['monto_total'] / df['numero_facturas']
    ).round(2)
    
    return df


def upload_to_s3(df: pd.DataFrame, bucket_name: str, s3_key: str, 
                 file_format: str = 'parquet') -> Dict[str, str]:
    """
    Sube DataFrame a S3
    
    Args:
        df: DataFrame a subir
        bucket_name: Nombre del bucket S3
        s3_key: Ruta en S3
        file_format: Formato del archivo
    
    Returns:
        Información del archivo subido
    """
    s3 = boto3.client('s3')
    temp_file = f'/tmp/{os.path.basename(s3_key)}'
    
    if file_format == 'parquet':
        df.to_parquet(temp_file, index=False)
    elif file_format == 'csv':
        df.to_csv(temp_file, index=False)
    elif file_format == 'json':
        df.to_json(temp_file, orient='records', date_format='iso')
    
    s3.upload_file(temp_file, bucket_name, s3_key)
    os.remove(temp_file)
    
    return {
        'bucket': bucket_name,
        'key': s3_key,
        'records': len(df),
        'format': file_format
    }


def run_sales_per_day_etl(db_config: Dict[str, str], 
                          bucket_name: str,
                          output_path: str = 'processed/ventas_por_dia',
                          file_format: str = 'parquet') -> Dict[str, any]:
    """
    Ejecuta el ETL de ventas por día
    
    Args:
        db_config: Configuración de la base de datos
        bucket_name: Nombre del bucket S3
        output_path: Ruta de salida en S3
        file_format: Formato del archivo de salida
    
    Returns:
        Resultados de la ejecución
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results = {
        'etl': 'sales_per_day',
        'timestamp': timestamp,
        'status': 'running',
        'errors': []
    }
    
    try:
        print("=== ETL 1: Ventas por Día ===")
        
        # Conectar a la base de datos
        connection = get_db_connection(**db_config)
        print("✓ Conexión a base de datos establecida")
        
        # Extraer datos
        df_raw = extract_sales_per_day(connection)
        print(f"✓ Extraídos {len(df_raw)} días con ventas")
        
        # Transformar datos
        df_transformed = transform_sales_data(df_raw)
        print("✓ Datos transformados")
        
        # Subir a S3
        s3_key = f"{output_path}/ventas_por_dia_{timestamp}.{file_format}"
        upload_info = upload_to_s3(df_transformed, bucket_name, s3_key, file_format)
        print(f"✓ Datos subidos a S3: {s3_key}")
        
        # Cerrar conexión
        connection.close()
        
        # Actualizar resultados
        results['status'] = 'success'
        results['output'] = upload_info
        results['summary'] = {
            'total_dias': len(df_transformed),
            'total_canciones': int(df_transformed['total_canciones_vendidas'].sum()),
            'total_facturas': int(df_transformed['numero_facturas'].sum()),
            'monto_total': float(df_transformed['monto_total'].sum()),
            'fecha_inicio': df_transformed['fecha'].min().strftime('%Y-%m-%d'),
            'fecha_fin': df_transformed['fecha'].max().strftime('%Y-%m-%d')
        }
        
        print(f"\n=== Resumen ===")
        print(f"Días procesados: {results['summary']['total_dias']}")
        print(f"Total canciones vendidas: {results['summary']['total_canciones']}")
        print(f"Monto total: ${results['summary']['monto_total']:.2f}")
        
    except Exception as e:
        results['status'] = 'failed'
        results['errors'].append(str(e))
        print(f"✗ Error: {str(e)}")
    
    return results


if __name__ == '__main__':
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'chinook'),
        'port': int(os.getenv('DB_PORT', 3306))
    }
    
    bucket_name = os.getenv('S3_BUCKET', 'chinook-datalake')
    
    results = run_sales_per_day_etl(db_config, bucket_name)
    
    # Guardar resultados
    with open(f'/tmp/etl_sales_per_day_{results["timestamp"]}.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
