import boto3
from botocore.exceptions import ClientError
from etls import constants


def setup_glue_crawler(bucket_name=constants.DEFAULT_BUCKET,
                      glue_database_name='chinook_db',
                      crawler_name='chinook-crawler',
                      iam_role_name='LabRole',
                      dry_run=False):
    """
    Configura AWS Glue con base de datos y crawler
    
    Args:
        bucket_name (str): Nombre del bucket S3
        glue_database_name (str): Nombre de la base de datos Glue
        crawler_name (str): Nombre del crawler
        iam_role_name (str): Nombre del rol IAM
        dry_run (bool): Si True, simula la operación sin conectar a AWS
    
    Returns:
        dict: Resultado de la operación con detalles de creación
    """
    result = {
        'database_created': False,
        'crawler_created': False,
        'crawler_started': False,
        'database_name': glue_database_name,
        'crawler_name': crawler_name,
        'errors': [],
        'dry_run': dry_run
    }
    
    if dry_run:
        print(f'[DRY RUN] Simulando setup Glue para bucket {bucket_name}')
        print(f'[DRY RUN] Base de datos {glue_database_name} sería creada.')
        print(f'[DRY RUN] Crawler {crawler_name} sería creado y iniciado.')
        result['database_created'] = True
        result['crawler_created'] = True
        result['crawler_started'] = True
        return result
    
    # Crear cliente Glue
    glue = boto3.client('glue', region_name='us-east-1')
    
    # Crear base de datos Glue
    try:
        glue.create_database(DatabaseInput={'Name': glue_database_name})
        print(f'Base de datos Glue {glue_database_name} creada.')
        result['database_created'] = True
    except ClientError as e:
        if 'AlreadyExistsException' in str(e):
            print(f'Base de datos {glue_database_name} ya existe.')
            result['database_created'] = False
        else:
            result['errors'].append(f'Error creando base de datos: {str(e)}')
            raise e
    
    # Crear Crawler
    try:
        glue.create_crawler(
            Name=crawler_name,
            Role=iam_role_name,
            DatabaseName=glue_database_name,
            Targets={'S3Targets': [
                {'Path': f's3://{bucket_name}/{constants.RAW_INVOICE}'},
                {'Path': f's3://{bucket_name}/{constants.RAW_INVOICE_LINE}'},
                {'Path': f's3://{bucket_name}/{constants.RAW_TRACK}'},
                {'Path': f's3://{bucket_name}/{constants.RAW_ALBUM}'},
                {'Path': f's3://{bucket_name}/{constants.RAW_ARTIST}'},
                {'Path': f's3://{bucket_name}/{constants.PROC_VENTAS_DIA}'},
                {'Path': f's3://{bucket_name}/{constants.PROC_ARTISTA_MES}'},
                {'Path': f's3://{bucket_name}/{constants.PROC_DIA_SEMANA}'},
                {'Path': f's3://{bucket_name}/{constants.PROC_MES_VENTAS}'}
            ]},
            TablePrefix='dl_',
            SchemaChangePolicy={'UpdateBehavior': 'UPDATE_IN_DATABASE', 'DeleteBehavior': 'DEPRECATE_IN_DATABASE'}
        )
        print(f'Crawler {crawler_name} creado.')
        result['crawler_created'] = True
    except ClientError as e:
        if 'AlreadyExistsException' in str(e):
            print(f'Crawler {crawler_name} ya existe.')
            result['crawler_created'] = False
        else:
            result['errors'].append(f'Error creando crawler: {str(e)}')
            raise e
    
    # Iniciar Crawler
    try:
        glue.start_crawler(Name=crawler_name)
        print(f'Crawler {crawler_name} iniciado, indexando datos en {bucket_name}.')
        result['crawler_started'] = True
    except ClientError as e:
        result['errors'].append(f'Error iniciando crawler: {str(e)}')
        raise e
    
    return result
