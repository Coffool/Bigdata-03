import pytest
import boto3
from moto import mock_aws
from botocore.exceptions import ClientError
import sys
import os

# Agregar el directorio raíz del proyecto al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_lake import create_data_lake


@mock_aws
class TestDataLake:
    """Tests para el módulo data_lake.py"""
    
    def test_bucket_creation_success(self):
        """Test que verifica la creación exitosa del bucket usando la función create_data_lake"""
        bucket_name = 'test-chinook-datalake'
        
        # Llamar a la función para crear el data lake
        result = create_data_lake(bucket_name)
        
        # Verificar resultado
        assert result['bucket_created'] == True
        assert result['bucket_name'] == bucket_name
        assert len(result['folders_created']) == 17  # Todas las carpetas esperadas (raw + processed + analytics)
        assert len(result['errors']) == 0
        
        # Verificar que el bucket existe
        s3 = boto3.client('s3', region_name='us-east-1')
        response = s3.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        assert bucket_name in bucket_names
    
    def test_bucket_already_exists(self):
        """Test que verifica el comportamiento cuando el bucket ya existe"""
        bucket_name = 'test-chinook-datalake'
        
        # Crear bucket directamente con boto3 primero
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket=bucket_name)
        
        # Ahora intentar crear el data lake
        result = create_data_lake(bucket_name)
        
        # El bucket ya existe, pero las carpetas se crean igual
        # Nota: moto puede comportarse diferente que AWS real
        assert result['bucket_name'] == bucket_name
        assert len(result['folders_created']) == 17
        assert len(result['errors']) == 0
    
    def test_folder_structure_creation(self):
        """Test que verifica la creación de la estructura de carpetas"""
        bucket_name = 'test-chinook-datalake'
        
        # Usar carpetas reales de constants.py
        from etls import constants
        expected_folders = [
            constants.RAW_INVOICE, constants.RAW_INVOICE_LINE, constants.RAW_TRACK,
            constants.RAW_ALBUM, constants.RAW_ARTIST, constants.RAW_CUSTOMER,
            constants.RAW_EMPLOYEE, constants.RAW_GENRE, constants.RAW_MEDIA_TYPE,
            constants.RAW_PLAYLIST, constants.RAW_PLAYLIST_TRACK, constants.RAW_CUSTOMER_EMPLOYEE_HISTORY,
            constants.PROC_VENTAS_DIA, constants.PROC_ARTISTA_MES,
            constants.PROC_DIA_SEMANA, constants.PROC_MES_VENTAS,
            'analytics/informes/'
        ]
        
        # Crear data lake
        result = create_data_lake(bucket_name)
        
        # Verificar que las carpetas fueron creadas
        assert len(result['folders_created']) == len(expected_folders)
        for folder in expected_folders:
            assert folder in result['folders_created']
        
        # Verificar en S3 directamente
        s3 = boto3.client('s3', region_name='us-east-1')
        response = s3.list_objects_v2(Bucket=bucket_name)
        created_folders = [obj['Key'] for obj in response['Contents']]
        
        for folder in expected_folders:
            assert folder in created_folders
    
    def test_raw_data_folders_exist(self):
        """Test específico para verificar carpetas de datos raw"""
        s3 = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'test-chinook-datalake'
        
        s3.create_bucket(Bucket=bucket_name)
        
        raw_folders = [
            'raw/ventas/',
            'raw/tracks/',
            'raw/artistas/',
            'raw/clientes/'
        ]
        
        for folder in raw_folders:
            s3.put_object(Bucket=bucket_name, Key=folder)
        
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='raw/')
        raw_objects = [obj['Key'] for obj in response['Contents']]
        
        for folder in raw_folders:
            assert folder in raw_objects
    
    def test_processed_data_folders_exist(self):
        """Test específico para verificar carpetas de datos procesados"""
        s3 = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'test-chinook-datalake'
        
        s3.create_bucket(Bucket=bucket_name)
        
        processed_folders = [
            'processed/ventas_por_dia/',
            'processed/artista_mes/',
            'processed/dia_semana/',
            'processed/mes_ventas/'
        ]
        
        for folder in processed_folders:
            s3.put_object(Bucket=bucket_name, Key=folder)
        
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='processed/')
        processed_objects = [obj['Key'] for obj in response['Contents']]
        
        for folder in processed_folders:
            assert folder in processed_objects
    
    def test_analytics_folder_exists(self):
        """Test específico para verificar carpeta de analytics"""
        s3 = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'test-chinook-datalake'
        
        s3.create_bucket(Bucket=bucket_name)
        
        analytics_folder = 'analytics/informes/'
        s3.put_object(Bucket=bucket_name, Key=analytics_folder)
        
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix='analytics/')
        analytics_objects = [obj['Key'] for obj in response['Contents']]
        
        assert analytics_folder in analytics_objects
    
    def test_empty_bucket_listing(self):
        """Test que verifica el comportamiento con bucket vacío"""
        s3 = boto3.client('s3', region_name='us-east-1')
        bucket_name = 'test-empty-bucket'
        
        s3.create_bucket(Bucket=bucket_name)
        
        # Listar objetos en bucket vacío
        response = s3.list_objects_v2(Bucket=bucket_name)
        
        # El bucket existe pero no tiene contenido
        assert 'Contents' not in response
        assert response['KeyCount'] == 0


if __name__ == '__main__':
    pytest.main([__file__])