import boto3
from botocore.exceptions import ClientError


def setup_glue_crawler(bucket_name='chinook-datalake', 
                      glue_database_name='chinook_db',
                      crawler_name='chinook-crawler',
                      iam_role_name='LabRole'):
    """
    Configura AWS Glue con base de datos y crawler
    
    Args:
        bucket_name (str): Nombre del bucket S3
        glue_database_name (str): Nombre de la base de datos Glue
        crawler_name (str): Nombre del crawler
        iam_role_name (str): Nombre del rol IAM
    
    Returns:
        dict: Resultado de la operación con detalles de creación
    """
    # Crear cliente Glue
    glue = boto3.client('glue')
    
    result = {
        'database_created': False,
        'crawler_created': False,
        'crawler_started': False,
        'database_name': glue_database_name,
        'crawler_name': crawler_name,
        'errors': []
    }
    
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
            Targets={'S3Targets': [{'Path': f's3://{bucket_name}/'}]},
            TablePrefix='etl_',
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


if __name__ == '__main__':
    # Solo ejecutar si se llama directamente
    setup_glue_crawler()
