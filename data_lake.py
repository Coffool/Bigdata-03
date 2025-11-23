import boto3
from botocore.exceptions import ClientError
from etls import constants


def create_data_lake(bucket_name='chinook-datalake', dry_run=False):
    """
    Crea el data lake con la estructura de carpetas necesaria
    
    Args:
        bucket_name (str): Nombre del bucket S3 a crear
        dry_run (bool): Si True, simula la operación sin conectar a AWS
    
    Returns:
        dict: Resultado de la operación con detalles de creación
    """
    # Estructura de data lake
    folders = [
        constants.RAW_INVOICE,
        constants.RAW_INVOICE_LINE,
        constants.RAW_TRACK,
        constants.RAW_ALBUM,
        constants.RAW_ARTIST,
        constants.RAW_CUSTOMER,
        constants.RAW_EMPLOYEE,
        constants.RAW_GENRE,
        constants.RAW_MEDIA_TYPE,
        constants.RAW_PLAYLIST,
        constants.RAW_PLAYLIST_TRACK,
        constants.RAW_CUSTOMER_EMPLOYEE_HISTORY,
        constants.PROC_VENTAS_DIA,
        constants.PROC_ARTISTA_MES,
        constants.PROC_DIA_SEMANA,
        constants.PROC_MES_VENTAS,
        'analytics/informes/'
    ]
    
    result = {
        'bucket_created': False,
        'bucket_name': bucket_name,
        'folders_created': [],
        'errors': [],
        'dry_run': dry_run
    }
    
    if dry_run:
        print(f'[DRY RUN] Simulando creación de bucket {bucket_name}')
        result['bucket_created'] = True
        result['folders_created'] = folders.copy()
        for folder in folders:
            print(f'[DRY RUN] Carpeta {folder} sería creada en {bucket_name}.')
        return result
    
    # Crear cliente S3
    s3 = boto3.client('s3', region_name='us-east-1')
    
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
    import os
    import boto3
    
    # Determinar si usar dry_run basado en disponibilidad de credenciales
    try:
        boto3.client('sts', region_name='us-east-1').get_caller_identity()
        dry_run = False
        print("Credenciales AWS encontradas - ejecutando modo real")
    except Exception:
        dry_run = True
        print("Sin credenciales AWS - ejecutando modo dry_run")
    
    create_data_lake(dry_run=dry_run)
