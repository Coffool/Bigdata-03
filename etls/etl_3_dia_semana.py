"""
ETL 3 - Día de la Semana con Más Ventas

Este ETL calcula qué día de la semana tiene más ventas de canciones.

Salida: processed/dia_semana/
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


def extract_sales_by_weekday(connection) -> pd.DataFrame:
    """
    Extrae ventas agrupadas por día de la semana
    
    Args:
        connection: Conexión a la base de datos
    
    Returns:
        DataFrame con ventas por día de la semana
    """
    query = """
    SELECT 
        DAYOFWEEK(i.InvoiceDate) as numero_dia_semana,
        DAYNAME(i.InvoiceDate) as dia_semana,
        COUNT(il.TrackId) as total_canciones_vendidas,
        SUM(il.Quantity) as cantidad_total,
        COUNT(DISTINCT i.InvoiceId) as numero_facturas,
        COUNT(DISTINCT i.CustomerId) as clientes_unicos,
        COUNT(DISTINCT DATE(i.InvoiceDate)) as dias_registrados,
        SUM(il.UnitPrice * il.Quantity) as monto_total,
        AVG(il.UnitPrice * il.Quantity) as monto_promedio_linea
    FROM Invoice i
    JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
    GROUP BY 
        DAYOFWEEK(i.InvoiceDate),
        DAYNAME(i.InvoiceDate)
    ORDER BY numero_dia_semana
    """
    df = pd.read_sql(query, connection)
    return df


def transform_weekday_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma y enriquece el análisis por día de la semana
    
    Args:
        df: DataFrame con datos crudos
    
    Returns:
        DataFrame transformado
    """
    # Calcular promedios por día real (no por ocurrencias)
    df['promedio_canciones_por_dia'] = (
        df['total_canciones_vendidas'] / df['dias_registrados']
    ).round(2)
    
    df['promedio_facturas_por_dia'] = (
        df['numero_facturas'] / df['dias_registrados']
    ).round(2)
    
    df['promedio_monto_por_dia'] = (
        df['monto_total'] / df['dias_registrados']
    ).round(2)
    
    # Calcular promedios por factura
    df['promedio_canciones_por_factura'] = (
        df['total_canciones_vendidas'] / df['numero_facturas']
    ).round(2)
    
    df['ticket_promedio'] = (
        df['monto_total'] / df['numero_facturas']
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
    
    # Agregar ranking
    df['ranking_por_canciones'] = df['total_canciones_vendidas'].rank(
        method='dense', ascending=False
    ).astype(int)
    
    df['ranking_por_monto'] = df['monto_total'].rank(
        method='dense', ascending=False
    ).astype(int)
    
    # Normalizar nombres de días en español
    day_translation = {
        'Monday': 'Lunes',
        'Tuesday': 'Martes',
        'Wednesday': 'Miércoles',
        'Thursday': 'Jueves',
        'Friday': 'Viernes',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    
    df['dia_semana_es'] = df['dia_semana'].map(day_translation)
    
    # Clasificar tipo de día
    df['tipo_dia'] = df['numero_dia_semana'].apply(
        lambda x: 'Fin de semana' if x in [1, 7] else 'Entre semana'
    )
    
    return df


def get_summary_statistics(df: pd.DataFrame) -> Dict:
    """
    Genera estadísticas resumen del análisis
    
    Args:
        df: DataFrame con datos transformados
    
    Returns:
        Diccionario con estadísticas
    """
    # Identificar el mejor día
    best_day_sales = df.loc[df['total_canciones_vendidas'].idxmax()]
    best_day_revenue = df.loc[df['monto_total'].idxmax()]
    
    # Comparar entre semana vs fin de semana
    weekday_stats = df[df['tipo_dia'] == 'Entre semana'].agg({
        'total_canciones_vendidas': 'sum',
        'monto_total': 'sum',
        'numero_facturas': 'sum'
    })
    
    weekend_stats = df[df['tipo_dia'] == 'Fin de semana'].agg({
        'total_canciones_vendidas': 'sum',
        'monto_total': 'sum',
        'numero_facturas': 'sum'
    })
    
    summary = {
        'mejor_dia_por_ventas': {
            'dia': best_day_sales['dia_semana'],
            'dia_es': best_day_sales['dia_semana_es'],
            'canciones': int(best_day_sales['total_canciones_vendidas']),
            'porcentaje': float(best_day_sales['porcentaje_canciones'])
        },
        'mejor_dia_por_ingresos': {
            'dia': best_day_revenue['dia_semana'],
            'dia_es': best_day_revenue['dia_semana_es'],
            'monto': float(best_day_revenue['monto_total']),
            'porcentaje': float(best_day_revenue['porcentaje_monto'])
        },
        'comparacion_tipo_dia': {
            'entre_semana': {
                'canciones': int(weekday_stats['total_canciones_vendidas']),
                'monto': float(weekday_stats['monto_total']),
                'facturas': int(weekday_stats['numero_facturas'])
            },
            'fin_de_semana': {
                'canciones': int(weekend_stats['total_canciones_vendidas']),
                'monto': float(weekend_stats['monto_total']),
                'facturas': int(weekend_stats['numero_facturas'])
            }
        },
        'totales': {
            'canciones': int(df['total_canciones_vendidas'].sum()),
            'monto': float(df['monto_total'].sum()),
            'facturas': int(df['numero_facturas'].sum())
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
        df.to_json(temp_file, orient='records')
    
    s3.upload_file(temp_file, bucket_name, s3_key)
    os.remove(temp_file)
    
    return {
        'bucket': bucket_name,
        'key': s3_key,
        'records': len(df),
        'format': file_format
    }


def run_weekday_analysis_etl(db_config: Dict[str, str], 
                             bucket_name: str,
                             output_path: str = 'processed/dia_semana',
                             file_format: str = 'parquet') -> Dict[str, any]:
    """
    Ejecuta el ETL de análisis por día de la semana
    
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
        'etl': 'weekday_analysis',
        'timestamp': timestamp,
        'status': 'running',
        'errors': []
    }
    
    try:
        print("=== ETL 3: Día de la Semana con Más Ventas ===")
        
        # Conectar
        connection = get_db_connection(**db_config)
        print("✓ Conexión a base de datos establecida")
        
        # Extraer datos
        df_raw = extract_sales_by_weekday(connection)
        print(f"✓ Extraídos datos de {len(df_raw)} días de la semana")
        
        # Transformar
        df_transformed = transform_weekday_analysis(df_raw)
        print("✓ Datos transformados y enriquecidos")
        
        # Generar estadísticas
        summary = get_summary_statistics(df_transformed)
        print("✓ Estadísticas generadas")
        
        # Subir datos detallados
        s3_key = f"{output_path}/analisis_por_dia_semana_{timestamp}.{file_format}"
        upload_info = upload_to_s3(df_transformed, bucket_name, s3_key, file_format)
        print(f"✓ Datos subidos a: {s3_key}")
        
        # Subir resumen como JSON
        s3_key_summary = f"{output_path}/resumen_dia_semana_{timestamp}.json"
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key_summary,
            Body=json.dumps(summary, indent=2),
            ContentType='application/json'
        )
        print(f"✓ Resumen subido a: {s3_key_summary}")
        
        # Cerrar conexión
        connection.close()
        
        # Resultados
        results['status'] = 'success'
        results['output'] = upload_info
        results['summary'] = summary
        
        print(f"\n=== Resumen ===")
        print(f"Mejor día por ventas: {summary['mejor_dia_por_ventas']['dia_es']} "
              f"({summary['mejor_dia_por_ventas']['canciones']} canciones)")
        print(f"Mejor día por ingresos: {summary['mejor_dia_por_ingresos']['dia_es']} "
              f"(${summary['mejor_dia_por_ingresos']['monto']:.2f})")
        
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
    
    results = run_weekday_analysis_etl(db_config, bucket_name)
    
    # Guardar resultados
    with open(f'/tmp/etl_weekday_{results["timestamp"]}.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
