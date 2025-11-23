"""
ETL 0 - Full Copy: RDS → S3 (RAW)

Este ETL realiza una copia completa de todas las tablas transaccionales
desde RDS a S3, generando snapshots históricos.

Incluye:
- Todas las tablas de la base de datos Chinook
- Histórico de clientes con su empleado de soporte asignado
- Timestamp para control de versiones
"""

import boto3
import pymysql
import pandas as pd
from datetime import datetime
import json
import os
from typing import Dict, List, Optional


def get_db_connection(host: str, user: str, password: str, database: str, port: int = 3306):
    """
    Establece conexión con la base de datos MySQL/RDS
    
    Args:
        host: Hostname del servidor de base de datos
        user: Usuario de la base de datos
        password: Contraseña
        database: Nombre de la base de datos
        port: Puerto (default 3306)
    
    Returns:
        Conexión pymysql
    """
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        cursorclass=pymysql.cursors.DictCursor
    )


def get_all_tables(connection) -> List[str]:
    """
    Obtiene lista de todas las tablas en la base de datos
    
    Args:
        connection: Conexión a la base de datos
    
    Returns:
        Lista de nombres de tablas
    """
    with connection.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = [list(row.values())[0] for row in cursor.fetchall()]
    return tables


def extract_table_data(connection, table_name: str) -> pd.DataFrame:
    """
    Extrae todos los datos de una tabla específica
    
    Args:
        connection: Conexión a la base de datos
        table_name: Nombre de la tabla a extraer
    
    Returns:
        DataFrame con los datos de la tabla
    """
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, connection)
    return df


def extract_customer_employee_history(connection) -> pd.DataFrame:
    """
    Extrae histórico de clientes con su empleado de soporte asignado
    
    Args:
        connection: Conexión a la base de datos
    
    Returns:
        DataFrame con relación cliente-empleado
    """
    query = """
    SELECT 
        c.CustomerId,
        c.FirstName as CustomerFirstName,
        c.LastName as CustomerLastName,
        c.Email as CustomerEmail,
        c.Company,
        c.Country as CustomerCountry,
        c.SupportRepId,
        e.FirstName as EmployeeFirstName,
        e.LastName as EmployeeLastName,
        e.Title as EmployeeTitle,
        e.ReportsTo,
        m.FirstName as ManagerFirstName,
        m.LastName as ManagerLastName,
        m.Title as ManagerTitle
    FROM Customer c
    LEFT JOIN Employee e ON c.SupportRepId = e.EmployeeId
    LEFT JOIN Employee m ON e.ReportsTo = m.EmployeeId
    """
    df = pd.read_sql(query, connection)
    return df


def upload_to_s3(df: pd.DataFrame, bucket_name: str, s3_key: str, 
                 file_format: str = 'parquet') -> Dict[str, str]:
    """
    Sube DataFrame a S3 en formato especificado
    
    Args:
        df: DataFrame a subir
        bucket_name: Nombre del bucket S3
        s3_key: Ruta/key en S3
        file_format: Formato del archivo ('parquet', 'csv', 'json')
    
    Returns:
        Diccionario con información del archivo subido
    """
    s3 = boto3.client('s3')
    
    # Crear archivo temporal
    temp_file = f'/tmp/{os.path.basename(s3_key)}'
    
    if file_format == 'parquet':
        df.to_parquet(temp_file, index=False)
    elif file_format == 'csv':
        df.to_csv(temp_file, index=False)
    elif file_format == 'json':
        df.to_json(temp_file, orient='records', lines=True)
    else:
        raise ValueError(f"Formato no soportado: {file_format}")
    
    # Subir a S3
    s3.upload_file(temp_file, bucket_name, s3_key)
    
    # Limpiar archivo temporal
    os.remove(temp_file)
    
    return {
        'bucket': bucket_name,
        'key': s3_key,
        'format': file_format,
        'size': len(df),
        'columns': len(df.columns)
    }


def run_full_copy_etl(db_config: Dict[str, str], 
                      bucket_name: str,
                      base_path: str = 'raw',
                      file_format: str = 'parquet') -> Dict[str, any]:
    """
    Ejecuta el ETL completo de copia de todas las tablas
    
    Args:
        db_config: Configuración de conexión a la base de datos
        bucket_name: Nombre del bucket S3 destino
        base_path: Ruta base en S3 (default: 'raw')
        file_format: Formato de archivo (default: 'parquet')
    
    Returns:
        Diccionario con resultados de la ejecución
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results = {
        'timestamp': timestamp,
        'tables_processed': [],
        'errors': [],
        'summary': {}
    }
    
    try:
        # Conectar a la base de datos
        connection = get_db_connection(**db_config)
        
        # Obtener todas las tablas
        tables = get_all_tables(connection)
        
        # Procesar cada tabla
        for table in tables:
            try:
                print(f"Procesando tabla: {table}")
                
                # Extraer datos
                df = extract_table_data(connection, table)
                
                # Definir ruta en S3
                s3_key = f"{base_path}/{table}/snapshot_{timestamp}.{file_format}"
                
                # Subir a S3
                upload_info = upload_to_s3(df, bucket_name, s3_key, file_format)
                
                results['tables_processed'].append({
                    'table': table,
                    'records': len(df),
                    's3_key': s3_key,
                    'status': 'success'
                })
                
                print(f"✓ Tabla {table}: {len(df)} registros procesados")
                
            except Exception as e:
                error_msg = f"Error procesando tabla {table}: {str(e)}"
                print(f"✗ {error_msg}")
                results['errors'].append(error_msg)
        
        # Procesar histórico de cliente-empleado
        try:
            print("Procesando histórico cliente-empleado...")
            
            df_history = extract_customer_employee_history(connection)
            s3_key = f"{base_path}/customer_employee_history/snapshot_{timestamp}.{file_format}"
            
            upload_to_s3(df_history, bucket_name, s3_key, file_format)
            
            results['tables_processed'].append({
                'table': 'customer_employee_history',
                'records': len(df_history),
                's3_key': s3_key,
                'status': 'success'
            })
            
            print(f"✓ Histórico cliente-empleado: {len(df_history)} registros procesados")
            
        except Exception as e:
            error_msg = f"Error procesando histórico: {str(e)}"
            print(f"✗ {error_msg}")
            results['errors'].append(error_msg)
        
        # Cerrar conexión
        connection.close()
        
        # Resumen
        results['summary'] = {
            'total_tables': len(tables) + 1,  # +1 por el histórico
            'successful': len(results['tables_processed']),
            'failed': len(results['errors']),
            'total_records': sum(t['records'] for t in results['tables_processed'])
        }
        
        print(f"\n=== Resumen ETL Full Copy ===")
        print(f"Tablas procesadas: {results['summary']['successful']}/{results['summary']['total_tables']}")
        print(f"Total registros: {results['summary']['total_records']}")
        print(f"Errores: {results['summary']['failed']}")
        
    except Exception as e:
        results['errors'].append(f"Error general: {str(e)}")
        print(f"✗ Error general: {str(e)}")
    
    return results


if __name__ == '__main__':
    # Configuración de ejemplo
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'chinook'),
        'port': int(os.getenv('DB_PORT', 3306))
    }
    
    bucket_name = os.getenv('S3_BUCKET', 'chinook-datalake')
    
    # Ejecutar ETL
    results = run_full_copy_etl(db_config, bucket_name)
    
    # Guardar resultados
    with open(f'/tmp/etl_full_copy_{results["timestamp"]}.json', 'w') as f:
        json.dump(results, f, indent=2)
