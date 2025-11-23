"""
ETL 2 - Artista Más Vendido por Mes

Este ETL identifica el artista con más canciones vendidas en cada mes.

Salida: processed/artista_mes/
"""

import boto3
import pymysql
import pandas as pd
from datetime import datetime
import json
import os
from typing import Dict


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


def extract_artist_sales_by_month(connection) -> pd.DataFrame:
    """
    Extrae ventas de artistas agrupadas por mes
    
    Args:
        connection: Conexión a la base de datos
    
    Returns:
        DataFrame con ventas por artista y mes
    """
    query = """
    SELECT 
        DATE_FORMAT(i.InvoiceDate, '%Y-%m-01') as mes,
        YEAR(i.InvoiceDate) as año,
        MONTH(i.InvoiceDate) as numero_mes,
        a.ArtistId,
        a.Name as nombre_artista,
        COUNT(il.TrackId) as total_canciones_vendidas,
        SUM(il.Quantity) as cantidad_total,
        SUM(il.UnitPrice * il.Quantity) as monto_total,
        COUNT(DISTINCT i.InvoiceId) as numero_facturas,
        COUNT(DISTINCT i.CustomerId) as clientes_unicos,
        COUNT(DISTINCT t.AlbumId) as albums_vendidos
    FROM Invoice i
    JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
    JOIN Track t ON il.TrackId = t.TrackId
    JOIN Album al ON t.AlbumId = al.AlbumId
    JOIN Artist a ON al.ArtistId = a.ArtistId
    GROUP BY 
        DATE_FORMAT(i.InvoiceDate, '%Y-%m-01'),
        YEAR(i.InvoiceDate),
        MONTH(i.InvoiceDate),
        a.ArtistId,
        a.Name
    ORDER BY mes DESC, total_canciones_vendidas DESC
    """
    df = pd.read_sql(query, connection)
    return df


def transform_top_artists_per_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identifica el artista más vendido por mes
    
    Args:
        df: DataFrame con ventas por artista y mes
    
    Returns:
        DataFrame con el top artista por mes
    """
    # Convertir mes a datetime
    df['mes'] = pd.to_datetime(df['mes'])
    
    # Agregar nombre del mes
    df['nombre_mes'] = df['mes'].dt.month_name()
    
    # Obtener el artista con más ventas por mes
    df_top = df.loc[df.groupby('mes')['total_canciones_vendidas'].idxmax()]
    
    # Calcular participación vs total del mes
    df_monthly_totals = df.groupby('mes').agg({
        'total_canciones_vendidas': 'sum',
        'monto_total': 'sum'
    }).rename(columns={
        'total_canciones_vendidas': 'total_mes_canciones',
        'monto_total': 'total_mes_monto'
    })
    
    df_top = df_top.merge(df_monthly_totals, on='mes', how='left')
    
    # Calcular porcentajes
    df_top['porcentaje_canciones'] = (
        (df_top['total_canciones_vendidas'] / df_top['total_mes_canciones']) * 100
    ).round(2)
    
    df_top['porcentaje_monto'] = (
        (df_top['monto_total'] / df_top['total_mes_monto']) * 100
    ).round(2)
    
    # Calcular precio promedio por canción
    df_top['precio_promedio_cancion'] = (
        df_top['monto_total'] / df_top['total_canciones_vendidas']
    ).round(2)
    
    # Ordenar por mes descendente
    df_top = df_top.sort_values('mes', ascending=False)
    
    return df_top


def get_artist_ranking_per_month(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """
    Obtiene el ranking de artistas por mes (top N)
    
    Args:
        df: DataFrame con ventas por artista y mes
        top_n: Número de artistas top a retornar por mes
    
    Returns:
        DataFrame con ranking de artistas
    """
    df['mes'] = pd.to_datetime(df['mes'])
    
    # Agregar ranking por mes
    df['ranking'] = df.groupby('mes')['total_canciones_vendidas'].rank(
        method='dense', ascending=False
    )
    
    # Filtrar solo top N
    df_ranked = df[df['ranking'] <= top_n].copy()
    
    # Ordenar
    df_ranked = df_ranked.sort_values(['mes', 'ranking'], ascending=[False, True])
    
    return df_ranked


def upload_to_s3(df: pd.DataFrame, bucket_name: str, s3_key: str, 
                 file_format: str = 'parquet') -> Dict[str, str]:
    """Sube DataFrame a S3"""
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


def run_top_artist_per_month_etl(db_config: Dict[str, str], 
                                  bucket_name: str,
                                  output_path: str = 'processed/artista_mes',
                                  file_format: str = 'parquet',
                                  include_ranking: bool = True) -> Dict[str, any]:
    """
    Ejecuta el ETL de artista más vendido por mes
    
    Args:
        db_config: Configuración de la base de datos
        bucket_name: Nombre del bucket S3
        output_path: Ruta de salida en S3
        file_format: Formato del archivo
        include_ranking: Si incluir archivo con ranking completo
    
    Returns:
        Resultados de la ejecución
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results = {
        'etl': 'top_artist_per_month',
        'timestamp': timestamp,
        'status': 'running',
        'errors': [],
        'outputs': []
    }
    
    try:
        print("=== ETL 2: Artista Más Vendido por Mes ===")
        
        # Conectar
        connection = get_db_connection(**db_config)
        print("✓ Conexión a base de datos establecida")
        
        # Extraer datos
        df_raw = extract_artist_sales_by_month(connection)
        print(f"✓ Extraídos {len(df_raw)} registros de ventas por artista/mes")
        
        # Transformar - Top artista por mes
        df_top = transform_top_artists_per_month(df_raw)
        print(f"✓ Identificados top artistas para {len(df_top)} meses")
        
        # Subir top artistas
        s3_key_top = f"{output_path}/top_artista_por_mes_{timestamp}.{file_format}"
        upload_info_top = upload_to_s3(df_top, bucket_name, s3_key_top, file_format)
        results['outputs'].append(upload_info_top)
        print(f"✓ Top artistas subidos a: {s3_key_top}")
        
        # Generar ranking completo si se solicita
        if include_ranking:
            df_ranking = get_artist_ranking_per_month(df_raw, top_n=5)
            s3_key_ranking = f"{output_path}/ranking_artistas_por_mes_{timestamp}.{file_format}"
            upload_info_ranking = upload_to_s3(df_ranking, bucket_name, s3_key_ranking, file_format)
            results['outputs'].append(upload_info_ranking)
            print(f"✓ Ranking completo subido a: {s3_key_ranking}")
        
        # Cerrar conexión
        connection.close()
        
        # Resumen
        results['status'] = 'success'
        results['summary'] = {
            'total_meses': len(df_top),
            'artistas_unicos': df_raw['ArtistId'].nunique(),
            'artista_mas_frecuente': df_top['nombre_artista'].mode()[0] if len(df_top) > 0 else None,
            'veces_top': int(df_top['nombre_artista'].value_counts().iloc[0]) if len(df_top) > 0 else 0,
            'promedio_canciones_top': float(df_top['total_canciones_vendidas'].mean()),
            'max_canciones_mes': int(df_top['total_canciones_vendidas'].max())
        }
        
        print(f"\n=== Resumen ===")
        print(f"Meses analizados: {results['summary']['total_meses']}")
        print(f"Artista más frecuente en top: {results['summary']['artista_mas_frecuente']}")
        print(f"Apariciones: {results['summary']['veces_top']} veces")
        
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
    
    results = run_top_artist_per_month_etl(db_config, bucket_name)
    
    # Guardar resultados
    with open(f'/tmp/etl_top_artist_{results["timestamp"]}.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
