import boto3
from botocore.exceptions import ClientError


def create_data_lake(bucket_name='chinook-datalake'):
    """
    Crea el data lake con la estructura de carpetas necesaria
    
    Args:
        bucket_name (str): Nombre del bucket S3 a crear
    
    Returns:
        dict: Resultado de la operación con detalles de creación
    """
    # Estructura de data lake
    folders = [
        'raw/ventas/',
        'raw/tracks/',
        'raw/artistas/',
        'raw/clientes/',
        'processed/ventas_por_dia/',
        'processed/artista_mes/',
        'processed/dia_semana/',
        'processed/mes_ventas/',
        'analytics/informes/'
    ]
    
    # Crear cliente S3
    s3 = boto3.client('s3')
    
    result = {
        'bucket_created': False,
        'bucket_name': bucket_name,
        'folders_created': [],
        'errors': []
    }
    
    # Crear bucket
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f'Bucket {bucket_name} creado.')
        result['bucket_created'] = True
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f'Bucket {bucket_name} ya existe.')
            result['bucket_created'] = False
        else:
            result['errors'].append(str(e))
            raise e
    
    # Crear carpetas dentro del bucket
    for folder in folders:
        try:
            s3.put_object(Bucket=bucket_name, Key=folder)
            print(f'Carpeta {folder} creada en {bucket_name}.')
            result['folders_created'].append(folder)
        except ClientError as e:
            result['errors'].append(f'Error creando {folder}: {str(e)}')
    
    return result


if __name__ == '__main__':
    # Solo ejecutar si se llama directamente
    create_data_lake()
