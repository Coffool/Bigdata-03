"""
Tests para setup_emr_s3.py

Tests unitarios para el gestor de scripts EMR en S3
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from botocore.exceptions import ClientError
import sys
from pathlib import Path
from datetime import datetime

# Agregar directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from setup_emr_s3 import EMRScriptManager


@pytest.fixture
def mock_s3_client():
    """Mock de cliente S3"""
    return MagicMock()


@pytest.fixture
def mock_s3_resource():
    """Mock de resource S3"""
    return MagicMock()


@pytest.fixture
def manager(mock_s3_client, mock_s3_resource):
    """Fixture del manager con mocks"""
    with patch('boto3.client', return_value=mock_s3_client), \
         patch('boto3.resource', return_value=mock_s3_resource):
        manager = EMRScriptManager(region='us-east-1')
        manager.s3_client = mock_s3_client
        manager.s3_resource = mock_s3_resource
        return manager


class TestEMRScriptManagerInit:
    """Tests para inicialización"""
    
    @patch('boto3.client')
    @patch('boto3.resource')
    def test_initialization_default_region(self, mock_resource, mock_client):
        """Test: Inicialización con región por defecto"""
        manager = EMRScriptManager()
        
        assert manager.region == 'us-east-1'
        mock_client.assert_called_once_with('s3', region_name='us-east-1')
        mock_resource.assert_called_once_with('s3', region_name='us-east-1')
    
    @patch('boto3.client')
    @patch('boto3.resource')
    def test_initialization_custom_region(self, mock_resource, mock_client):
        """Test: Inicialización con región personalizada"""
        manager = EMRScriptManager(region='us-west-2')
        
        assert manager.region == 'us-west-2'
        mock_client.assert_called_once_with('s3', region_name='us-west-2')


class TestCreateBucket:
    """Tests para creación de bucket"""
    
    def test_create_bucket_us_east_1(self, manager):
        """Test: Crear bucket en us-east-1"""
        manager.region = 'us-east-1'
        manager.s3_client.create_bucket = MagicMock()
        manager.s3_client.put_bucket_versioning = MagicMock()
        manager.s3_client.put_bucket_tagging = MagicMock()
        manager.s3_client.put_bucket_encryption = MagicMock()
        
        result = manager.create_bucket('test-bucket')
        
        # Verificar que se llamó sin LocationConstraint
        manager.s3_client.create_bucket.assert_called_once_with(
            Bucket='test-bucket'
        )
        
        assert result['bucket_name'] == 'test-bucket'
        assert result['status'] == 'created'
        assert result['versioning'] == True
        assert result['encryption'] == 'AES256'
    
    def test_create_bucket_other_region(self, manager):
        """Test: Crear bucket en otra región"""
        manager.region = 'us-west-2'
        manager.s3_client.create_bucket = MagicMock()
        manager.s3_client.put_bucket_versioning = MagicMock()
        manager.s3_client.put_bucket_tagging = MagicMock()
        manager.s3_client.put_bucket_encryption = MagicMock()
        
        result = manager.create_bucket('test-bucket')
        
        # Verificar que se llamó con LocationConstraint
        manager.s3_client.create_bucket.assert_called_once_with(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        
        assert result['bucket_name'] == 'test-bucket'
        assert result['region'] == 'us-west-2'
    
    def test_create_bucket_already_owned(self, manager):
        """Test: Bucket ya existe y es tuyo"""
        error_response = {
            'Error': {
                'Code': 'BucketAlreadyOwnedByYou',
                'Message': 'Your previous request to create the named bucket succeeded'
            }
        }
        
        manager.s3_client.create_bucket.side_effect = ClientError(
            error_response, 'CreateBucket'
        )
        
        result = manager.create_bucket('test-bucket')
        
        assert result['bucket_name'] == 'test-bucket'
        assert result['status'] == 'already_exists'
    
    def test_create_bucket_already_exists_other_owner(self, manager):
        """Test: Bucket ya existe pero es de otro"""
        error_response = {
            'Error': {
                'Code': 'BucketAlreadyExists',
                'Message': 'The requested bucket name is not available'
            }
        }
        
        manager.s3_client.create_bucket.side_effect = ClientError(
            error_response, 'CreateBucket'
        )
        
        with pytest.raises(ClientError):
            manager.create_bucket('test-bucket')
    
    def test_create_bucket_without_versioning(self, manager):
        """Test: Crear bucket sin versionado"""
        manager.s3_client.create_bucket = MagicMock()
        manager.s3_client.put_bucket_tagging = MagicMock()
        manager.s3_client.put_bucket_encryption = MagicMock()
        
        result = manager.create_bucket('test-bucket', enable_versioning=False)
        
        # Verificar que NO se llamó put_bucket_versioning
        manager.s3_client.put_bucket_versioning.assert_not_called()
        assert result['versioning'] == False


class TestEnableVersioning:
    """Tests para habilitar versionado"""
    
    def test_enable_versioning(self, manager):
        """Test: Habilitar versionado"""
        manager.s3_client.put_bucket_versioning = MagicMock()
        
        manager.enable_versioning('test-bucket')
        
        manager.s3_client.put_bucket_versioning.assert_called_once_with(
            Bucket='test-bucket',
            VersioningConfiguration={'Status': 'Enabled'}
        )


class TestUploadScript:
    """Tests para subir scripts"""
    
    def test_upload_script_success(self, manager, tmp_path):
        """Test: Subir script exitosamente"""
        # Crear archivo temporal
        test_file = tmp_path / "test_script.py"
        test_file.write_text("print('hello')")
        
        # Configurar mocks
        manager.s3_client.upload_file = MagicMock()
        manager.s3_client.head_object = MagicMock(return_value={
            'ContentLength': 100,
            'VersionId': 'v123',
            'ETag': '"abc123"',
            'LastModified': datetime.now()
        })
        
        result = manager.upload_script(
            bucket_name='test-bucket',
            local_file_path=str(test_file)
        )
        
        # Verificar llamadas
        manager.s3_client.upload_file.assert_called_once()
        assert result['bucket'] == 'test-bucket'
        assert result['s3_key'] == 'scripts/test_script.py'
        assert result['s3_url'] == 's3://test-bucket/scripts/test_script.py'
        assert result['size'] == 100
    
    def test_upload_script_custom_key(self, manager, tmp_path):
        """Test: Subir script con clave personalizada"""
        test_file = tmp_path / "test.py"
        test_file.write_text("print('test')")
        
        manager.s3_client.upload_file = MagicMock()
        manager.s3_client.head_object = MagicMock(return_value={
            'ContentLength': 50,
            'ETag': '"xyz"',
            'LastModified': datetime.now()
        })
        
        result = manager.upload_script(
            bucket_name='test-bucket',
            local_file_path=str(test_file),
            s3_key='custom/path/script.py'
        )
        
        assert result['s3_key'] == 'custom/path/script.py'
    
    def test_upload_script_file_not_found(self, manager):
        """Test: Archivo no encontrado"""
        with pytest.raises(FileNotFoundError):
            manager.upload_script(
                bucket_name='test-bucket',
                local_file_path='nonexistent.py'
            )
    
    def test_upload_script_with_metadata(self, manager, tmp_path):
        """Test: Subir script con metadata"""
        test_file = tmp_path / "test.py"
        test_file.write_text("print('test')")
        
        manager.s3_client.upload_file = MagicMock()
        manager.s3_client.head_object = MagicMock(return_value={
            'ContentLength': 50,
            'ETag': '"xyz"',
            'LastModified': datetime.now()
        })
        
        metadata = {'Purpose': 'Test', 'Version': '1.0'}
        
        manager.upload_script(
            bucket_name='test-bucket',
            local_file_path=str(test_file),
            metadata=metadata
        )
        
        # Verificar que se pasó metadata
        call_args = manager.s3_client.upload_file.call_args
        assert call_args[1]['ExtraArgs']['Metadata'] == metadata


class TestCreateFolderStructure:
    """Tests para crear estructura de carpetas"""
    
    def test_create_folder_structure(self, manager):
        """Test: Crear estructura de carpetas"""
        manager.s3_client.put_object = MagicMock()
        
        manager.create_folder_structure('test-bucket')
        
        # Verificar que se crearon todas las carpetas
        expected_folders = [
            'scripts/',
            'scripts/clustering/',
            'scripts/etl/',
            'logs/',
            'output/',
            'output/clustering/',
            'notebooks/'
        ]
        
        assert manager.s3_client.put_object.call_count == len(expected_folders)
        
        # Verificar que se llamó con cada carpeta
        for folder in expected_folders:
            manager.s3_client.put_object.assert_any_call(
                Bucket='test-bucket',
                Key=folder,
                Body=b''
            )


class TestListScripts:
    """Tests para listar scripts"""
    
    def test_list_scripts_success(self, manager):
        """Test: Listar scripts exitosamente"""
        manager.s3_client.list_objects_v2 = MagicMock(return_value={
            'Contents': [
                {
                    'Key': 'scripts/test1.py',
                    'Size': 100,
                    'LastModified': datetime.now()
                },
                {
                    'Key': 'scripts/test2.py',
                    'Size': 200,
                    'LastModified': datetime.now()
                },
                {
                    'Key': 'scripts/README.md',  # No .py
                    'Size': 50,
                    'LastModified': datetime.now()
                }
            ]
        })
        
        scripts = manager.list_scripts('test-bucket')
        
        # Solo debe retornar archivos .py
        assert len(scripts) == 2
        assert all(s['key'].endswith('.py') for s in scripts)
        assert scripts[0]['s3_url'] == 's3://test-bucket/scripts/test1.py'
    
    def test_list_scripts_empty(self, manager):
        """Test: Sin scripts"""
        manager.s3_client.list_objects_v2 = MagicMock(return_value={})
        
        scripts = manager.list_scripts('test-bucket')
        
        assert scripts == []
    
    def test_list_scripts_with_prefix(self, manager):
        """Test: Listar con prefijo personalizado"""
        manager.s3_client.list_objects_v2 = MagicMock(return_value={
            'Contents': [
                {
                    'Key': 'custom/path/script.py',
                    'Size': 100,
                    'LastModified': datetime.now()
                }
            ]
        })
        
        scripts = manager.list_scripts('test-bucket', prefix='custom/path/')
        
        assert len(scripts) == 1
        manager.s3_client.list_objects_v2.assert_called_with(
            Bucket='test-bucket',
            Prefix='custom/path/'
        )


class TestDownloadScript:
    """Tests para descargar scripts"""
    
    def test_download_script_success(self, manager, tmp_path):
        """Test: Descargar script exitosamente"""
        manager.s3_client.download_file = MagicMock()
        
        local_path = str(tmp_path / "downloaded.py")
        
        result = manager.download_script(
            bucket_name='test-bucket',
            s3_key='scripts/test.py',
            local_path=local_path
        )
        
        manager.s3_client.download_file.assert_called_once_with(
            Bucket='test-bucket',
            Key='scripts/test.py',
            Filename=local_path
        )
        
        assert result == local_path
    
    def test_download_script_default_path(self, manager):
        """Test: Descargar con path por defecto"""
        manager.s3_client.download_file = MagicMock()
        
        result = manager.download_script(
            bucket_name='test-bucket',
            s3_key='scripts/test.py'
        )
        
        # Debería usar solo el nombre del archivo
        assert result == 'test.py'


class TestSetupEMREnvironment:
    """Tests para configuración completa"""
    
    def test_setup_emr_environment(self, manager, tmp_path):
        """Test: Configuración completa del entorno"""
        # Crear archivo de script
        script_file = tmp_path / "clustering.py"
        script_file.write_text("# clustering script")
        
        # Mockear métodos
        manager.create_bucket = MagicMock(return_value={
            'bucket_name': 'test-bucket',
            'status': 'created'
        })
        
        manager.create_folder_structure = MagicMock()
        
        manager.upload_script = MagicMock(return_value={
            's3_url': 's3://test-bucket/scripts/clustering/customer_clustering.py',
            'size': 1000,
            'version_id': 'v1'
        })
        
        # Ejecutar
        result = manager.setup_emr_environment(
            bucket_name='test-bucket',
            clustering_script_path=str(script_file)
        )
        
        # Verificar llamadas
        manager.create_bucket.assert_called_once_with('test-bucket')
        manager.create_folder_structure.assert_called_once_with('test-bucket')
        manager.upload_script.assert_called_once()
        
        # Verificar resultado
        assert result['bucket_name'] == 'test-bucket'
        assert 's3_url' in result['script_info']
        assert result['script_s3_url'] == 's3://test-bucket/scripts/clustering/customer_clustering.py'


class TestEdgeCases:
    """Tests para casos edge"""
    
    def test_client_error_handling(self, manager):
        """Test: Manejo de errores de cliente"""
        error_response = {
            'Error': {
                'Code': 'AccessDenied',
                'Message': 'Access Denied'
            }
        }
        
        manager.s3_client.create_bucket.side_effect = ClientError(
            error_response, 'CreateBucket'
        )
        
        with pytest.raises(ClientError):
            manager.create_bucket('test-bucket')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
