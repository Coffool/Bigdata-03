import pytest
import boto3
from moto import mock_aws
from botocore.exceptions import ClientError
import sys
import os

# Agregar el directorio raíz del proyecto al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from crawler import setup_glue_crawler


class TestCrawler:
    """Tests para el módulo crawler.py"""
    
    @mock_aws
    def test_setup_glue_crawler_success(self):
        """Test que verifica la creación exitosa completa usando setup_glue_crawler"""
        database_name = 'test_chinook_db'
        crawler_name = 'test-crawler'
        bucket_name = 'test-bucket'
        role_name = 'TestRole' 
        
        # Ejecutar setup
        result = setup_glue_crawler(bucket_name, database_name, crawler_name, role_name)
        
        # Verificar resultado
        assert result['database_created'] == True
        assert result['database_name'] == database_name
        assert result['crawler_created'] == True
        assert result['crawler_name'] == crawler_name
        assert result['crawler_started'] == True
        assert len(result['errors']) == 0
        
        # Verificar que la base de datos existe
        glue = boto3.client('glue', region_name='us-east-1')
        response = glue.get_databases()
        db_names = [db['Name'] for db in response['DatabaseList']]
        assert database_name in db_names
        
        # Verificar que el crawler existe
        response = glue.get_crawlers()
        crawler_names = [crawler['Name'] for crawler in response['Crawlers']]
        assert crawler_name in crawler_names
    
    @mock_aws
    def test_database_already_exists(self):
        """Test que verifica el comportamiento cuando la base de datos ya existe"""
        database_name = 'test_chinook_db'
        crawler_name = 'test-crawler'
        bucket_name = 'test-bucket'
        role_name = 'TestRole'
        
        # Crear por primera vez
        result1 = setup_glue_crawler(bucket_name, database_name, crawler_name, role_name)
        assert result1['database_created'] == True
        
        # Intentar crear nuevamente (con diferente nombre de crawler para evitar conflicto)
        crawler_name2 = 'test-crawler-2'
        result2 = setup_glue_crawler(bucket_name, database_name, crawler_name2, role_name)
        
        # La base de datos ya existe, pero el crawler se crea
        assert result2['database_created'] == False
        assert result2['crawler_created'] == True
    
    @mock_aws
    def test_function_returns_correct_structure(self):
        """Test que verifica que la función retorna la estructura correcta"""
        database_name = 'test_chinook_db_2'
        crawler_name = 'test-crawler-2'
        bucket_name = 'test-bucket-2'
        role_name = 'TestRole2'
        
        result = setup_glue_crawler(bucket_name, database_name, crawler_name, role_name)
        
        # Verificar estructura del resultado
        assert 'database_created' in result
        assert 'crawler_created' in result
        assert 'crawler_started' in result
        assert 'database_name' in result
        assert 'crawler_name' in result
        assert 'errors' in result
        
        # Verificar tipos
        assert isinstance(result['database_created'], bool)
        assert isinstance(result['crawler_created'], bool)
        assert isinstance(result['crawler_started'], bool)
        assert isinstance(result['database_name'], str)
        assert isinstance(result['crawler_name'], str)
        assert isinstance(result['errors'], list)


if __name__ == '__main__':
    pytest.main([__file__])