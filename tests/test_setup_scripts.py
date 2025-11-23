"""
Tests para scripts de setup (data_lake.py y crawler.py)

Estos tests validan la funcionalidad de los scripts en modo dry_run
para evitar dependencias de credenciales AWS en CI/CD.
"""

import pytest
import sys
import os

# Agregar el directorio raíz del proyecto al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import data_lake
import crawler
from etls import constants


class TestDataLakeSetup:
    """Tests para data_lake.py"""
    
    def test_create_data_lake_dry_run(self):
        """Test que verifica la creación de data lake en modo dry_run"""
        result = data_lake.create_data_lake('test-bucket', dry_run=True)
        
        assert result['dry_run'] is True
        assert result['bucket_created'] is True
        assert result['bucket_name'] == 'test-bucket'
        assert len(result['folders_created']) > 0
        assert len(result['errors']) == 0
        
        # Verificar que incluye carpetas raw y processed
        folders = result['folders_created']
        assert constants.RAW_INVOICE in folders
        assert constants.RAW_INVOICE_LINE in folders
        assert constants.PROC_VENTAS_DIA in folders
        assert constants.PROC_ARTISTA_MES in folders
    
    def test_create_data_lake_with_constants(self):
        """Test que verifica el uso correcto de constantes"""
        result = data_lake.create_data_lake(constants.DEFAULT_BUCKET, dry_run=True)
        
        assert result['bucket_name'] == constants.DEFAULT_BUCKET
        assert constants.RAW_TRACK in result['folders_created']
        assert constants.RAW_ALBUM in result['folders_created']
        assert constants.RAW_ARTIST in result['folders_created']


class TestCrawlerSetup:
    """Tests para crawler.py"""
    
    def test_setup_glue_crawler_dry_run(self):
        """Test que verifica la configuración del crawler en modo dry_run"""
        result = crawler.setup_glue_crawler(
            bucket_name='test-bucket',
            glue_database_name='test_db',
            crawler_name='test-crawler',
            dry_run=True
        )
        
        assert result['dry_run'] is True
        assert result['database_created'] is True
        assert result['crawler_created'] is True
        assert result['crawler_started'] is True
        assert result['database_name'] == 'test_db'
        assert result['crawler_name'] == 'test-crawler'
        assert len(result['errors']) == 0
    
    def test_setup_glue_crawler_with_defaults(self):
        """Test que verifica el uso de valores por defecto"""
        result = crawler.setup_glue_crawler(dry_run=True)
        
        assert result['database_name'] == 'chinook_db'
        assert result['crawler_name'] == 'chinook-crawler'
        assert result['dry_run'] is True


class TestConstantsIntegration:
    """Tests para verificar integración de constants.py con scripts"""
    
    def test_constants_import(self):
        """Test que verifica que constants.py se puede importar correctamente"""
        assert hasattr(constants, 'DEFAULT_BUCKET')
        assert hasattr(constants, 'RAW_INVOICE')
        assert hasattr(constants, 'PROC_VENTAS_DIA')
        
        # Verificar que los valores no están vacíos
        assert constants.DEFAULT_BUCKET
        assert constants.RAW_INVOICE.startswith('raw/')
        assert constants.PROC_VENTAS_DIA.startswith('processed/')
    
    def test_constants_used_in_data_lake(self):
        """Test que verifica que data_lake.py usa las constantes correctamente"""
        # Ejecutar en dry_run y verificar que las carpetas coinciden con constants
        result = data_lake.create_data_lake(dry_run=True)
        folders = result['folders_created']
        
        expected_constants = [
            constants.RAW_INVOICE, constants.RAW_INVOICE_LINE, constants.RAW_TRACK,
            constants.RAW_ALBUM, constants.RAW_ARTIST, constants.RAW_CUSTOMER,
            constants.PROC_VENTAS_DIA, constants.PROC_ARTISTA_MES,
            constants.PROC_DIA_SEMANA, constants.PROC_MES_VENTAS
        ]
        
        for const_folder in expected_constants:
            assert const_folder in folders, f"Constante {const_folder} no encontrada en carpetas creadas"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])