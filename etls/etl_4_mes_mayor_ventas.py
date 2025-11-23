"""
ETL 4 - Mes con Mayor Volumen de Ventas

Este ETL identifica el mes (histórico y por año) con mayor número de ventas.

Salida: processed/mes_ventas/
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


def extract_sales_by_month(connection) -> pd.DataFrame:
    """
    Extrae ventas agrupadas por mes
    
    Args:
        connection: Conexión a la base de datos
    
    Returns:
        DataFrame con ventas por mes
    """
    query = """
    SELECT 
        DATE_FORMAT(i.InvoiceDate, '%Y-%m-01') as mes,
        YEAR(i.InvoiceDate) as año,
        MONTH(i.InvoiceDate) as numero_mes,
        MONTHNAME(i.InvoiceDate) as nombre_mes,
        QUARTER(i.InvoiceDate) as trimestre,
        COUNT(il.TrackId) as total_canciones_vendidas,
        SUM(il.Quantity) as cantidad_total,
        COUNT(DISTINCT i.InvoiceId) as numero_facturas,
        COUNT(DISTINCT i.CustomerId) as clientes_unicos,
        COUNT(DISTINCT DATE(i.InvoiceDate)) as dias_con_ventas,
        SUM(il.UnitPrice * il.Quantity) as monto_total,
        AVG(il.UnitPrice * il.Quantity) as monto_promedio_linea,
        MIN(DATE(i.InvoiceDate)) as primera_venta,
        MAX(DATE(i.InvoiceDate)) as ultima_venta
    FROM Invoice i
    JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
    GROUP BY 
        DATE_FORMAT(i.InvoiceDate, '%Y-%m-01'),
        YEAR(i.InvoiceDate),
        MONTH(i.InvoiceDate),
        MONTHNAME(i.InvoiceDate),
        QUARTER(i.InvoiceDate)
    ORDER BY mes DESC
    """
    df = pd.read_sql(query, connection)
    return df


def transform_monthly_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma y enriquece el análisis mensual
    
    Args:
        df: DataFrame con datos crudos
    
    Returns:
        DataFrame transformado
    """
    # Convertir a datetime
    df['mes'] = pd.to_datetime(df['mes'])
    
    # Calcular promedios diarios
    df['promedio_canciones_por_dia'] = (
        df['total_canciones_vendidas'] / df['dias_con_ventas']
    ).round(2)
    
    df['promedio_facturas_por_dia'] = (
        df['numero_facturas'] / df['dias_con_ventas']
    ).round(2)
    
    df['promedio_monto_por_dia'] = (
        df['monto_total'] / df['dias_con_ventas']
    ).round(2)
    
    # Calcular promedios por factura
    df['promedio_canciones_por_factura'] = (
        df['total_canciones_vendidas'] / df['numero_facturas']
    ).round(2)
    
    df['ticket_promedio'] = (
        df['monto_total'] / df['numero_facturas']
    ).round(2)
    
    # Precio promedio por canción
    df['precio_promedio_cancion'] = (
        df['monto_total'] / df['total_canciones_vendidas']
    ).round(2)
    
    # Calcular participación porcentual
    total_canciones = df['total_canciones_vendidas'].sum()
    total_monto = df['monto_total'].sum()
    
    df['porcentaje_canciones'] = (
        (df['total_canciones_vendidas'] / total_canciones) * 100
    ).round(2)
    
    df['porcentaje_monto'] = (
        (df['monto_total'] / total_monto) * 100
    ).round(2)
    
    # Rankings
    df['ranking_historico'] = df['total_canciones_vendidas'].rank(
        method='dense', ascending=False
    ).astype(int)
    
    df['ranking_por_año'] = df.groupby('año')['total_canciones_vendidas'].rank(
        method='dense', ascending=False
    ).astype(int)
    
    # Traducir nombres de meses al español
    month_translation = {
        'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo',
        'April': 'Abril', 'May': 'Mayo', 'June': 'Junio',
        'July': 'Julio', 'August': 'Agosto', 'September': 'Septiembre',
        'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'
    }
    
    df['nombre_mes_es'] = df['nombre_mes'].map(month_translation)
    
    # Crecimiento vs mes anterior
    df = df.sort_values('mes')
    df['crecimiento_canciones'] = df['total_canciones_vendidas'].pct_change() * 100
    df['crecimiento_monto'] = df['monto_total'].pct_change() * 100
    
    # Ordenar por mes descendente
    df = df.sort_values('mes', ascending=False)
    
    return df


def get_top_months(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Obtiene los top N meses con más ventas
    
    Args:
        df: DataFrame con datos mensuales
        top_n: Número de meses top
    
    Returns:
        DataFrame con top meses
    """
    return df.nsmallest(top_n, 'ranking_historico')


def get_summary_statistics(df: pd.DataFrame) -> Dict:
    """
    Genera estadísticas resumen
    
    Args:
        df: DataFrame con datos transformados
    
    Returns:
        Diccionario con estadísticas
    """
    # Mejor mes histórico
    best_month_all_time = df.loc[df['total_canciones_vendidas'].idxmax()]
    
    # Mejor mes por año
    best_by_year = df.loc[df.groupby('año')['total_canciones_vendidas'].idxmax()]
    
    # Mejor trimestre
    quarterly_sales = df.groupby(['año', 'trimestre']).agg({
        'total_canciones_vendidas': 'sum',
        'monto_total': 'sum',
        'numero_facturas': 'sum'
    }).reset_index()
    best_quarter = quarterly_sales.loc[quarterly_sales['total_canciones_vendidas'].idxmax()]
    
    # Tendencia general
    df_sorted = df.sort_values('mes')
    recent_months = df_sorted.tail(6)
    older_months = df_sorted.head(6) if len(df_sorted) > 6 else df_sorted
    
    trend = {
        'promedio_reciente': float(recent_months['total_canciones_vendidas'].mean()),
        'promedio_antiguo': float(older_months['total_canciones_vendidas'].mean()),
        'cambio_porcentual': float(
            ((recent_months['total_canciones_vendidas'].mean() - 
              older_months['total_canciones_vendidas'].mean()) / 
             older_months['total_canciones_vendidas'].mean() * 100)
            if len(older_months) > 0 else 0
        )
    }
    
    summary = {
        'mejor_mes_historico': {
            'mes': best_month_all_time['mes'].strftime('%Y-%m'),
            'mes_nombre': best_month_all_time['nombre_mes_es'],
            'año': int(best_month_all_time['año']),
            'canciones': int(best_month_all_time['total_canciones_vendidas']),
            'monto': float(best_month_all_time['monto_total']),
            'facturas': int(best_month_all_time['numero_facturas'])
        },
        'mejores_meses_por_año': [
            {
                'año': int(row['año']),
                'mes': row['mes'].strftime('%Y-%m'),
                'mes_nombre': row['nombre_mes_es'],
                'canciones': int(row['total_canciones_vendidas']),
                'monto': float(row['monto_total'])
            }
            for _, row in best_by_year.iterrows()
        ],
        'mejor_trimestre': {
            'año': int(best_quarter['año']),
            'trimestre': int(best_quarter['trimestre']),
            'canciones': int(best_quarter['total_canciones_vendidas']),
            'monto': float(best_quarter['monto_total'])
        },
        'tendencia': trend,
        'estadisticas_generales': {
            'total_meses': len(df),
            'años_analizados': df['año'].nunique(),
            'promedio_mensual_canciones': float(df['total_canciones_vendidas'].mean()),
            'promedio_mensual_monto': float(df['monto_total'].mean()),
            'desviacion_std_canciones': float(df['total_canciones_vendidas'].std()),
            'coeficiente_variacion': float(
                (df['total_canciones_vendidas'].std() / 
                 df['total_canciones_vendidas'].mean() * 100)
            )
        }
    }
    
    return summary


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


def run_monthly_analysis_etl(db_config: Dict[str, str], 
                            bucket_name: str,
                            output_path: str = 'processed/mes_ventas',
                            file_format: str = 'parquet') -> Dict[str, any]:
    """
    Ejecuta el ETL de análisis mensual
    
    Args:
        db_config: Configuración de la base de datos
        bucket_name: Nombre del bucket S3
        output_path: Ruta de salida en S3
        file_format: Formato del archivo
    
    Returns:
        Resultados de la ejecución
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results = {
        'etl': 'monthly_analysis',
        'timestamp': timestamp,
        'status': 'running',
        'errors': [],
        'outputs': []
    }
    
    try:
        print("=== ETL 4: Mes con Mayor Volumen de Ventas ===")
        
        # Conectar
        connection = get_db_connection(**db_config)
        print("✓ Conexión a base de datos establecida")
        
        # Extraer datos
        df_raw = extract_sales_by_month(connection)
        print(f"✓ Extraídos datos de {len(df_raw)} meses")
        
        # Transformar
        df_transformed = transform_monthly_analysis(df_raw)
        print("✓ Datos transformados y enriquecidos")
        
        # Generar estadísticas
        summary = get_summary_statistics(df_transformed)
        print("✓ Estadísticas generadas")
        
        # Subir datos completos
        s3_key_full = f"{output_path}/ventas_mensuales_{timestamp}.{file_format}"
        upload_info_full = upload_to_s3(df_transformed, bucket_name, s3_key_full, file_format)
        results['outputs'].append(upload_info_full)
        print(f"✓ Datos completos subidos a: {s3_key_full}")
        
        # Subir top 10 meses
        df_top = get_top_months(df_transformed, top_n=10)
        s3_key_top = f"{output_path}/top_10_meses_{timestamp}.{file_format}"
        upload_info_top = upload_to_s3(df_top, bucket_name, s3_key_top, file_format)
        results['outputs'].append(upload_info_top)
        print(f"✓ Top 10 meses subidos a: {s3_key_top}")
        
        # Subir resumen como JSON
        s3_key_summary = f"{output_path}/resumen_mensual_{timestamp}.json"
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key_summary,
            Body=json.dumps(summary, indent=2, default=str),
            ContentType='application/json'
        )
        print(f"✓ Resumen subido a: {s3_key_summary}")
        
        # Cerrar conexión
        connection.close()
        
        # Resultados
        results['status'] = 'success'
        results['summary'] = summary
        
        print(f"\n=== Resumen ===")
        print(f"Mejor mes histórico: {summary['mejor_mes_historico']['mes_nombre']} "
              f"{summary['mejor_mes_historico']['año']} "
              f"({summary['mejor_mes_historico']['canciones']} canciones)")
        print(f"Monto: ${summary['mejor_mes_historico']['monto']:.2f}")
        print(f"Tendencia: {summary['tendencia']['cambio_porcentual']:.1f}% "
              f"({'crecimiento' if summary['tendencia']['cambio_porcentual'] > 0 else 'decrecimiento'})")
        
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
    
    results = run_monthly_analysis_etl(db_config, bucket_name)
    
    # Guardar resultados
    with open(f'/tmp/etl_monthly_{results["timestamp"]}.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
