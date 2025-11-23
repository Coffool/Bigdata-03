"""
Script para configurar S3 y subir script de clustering EMR

Este script:
1. Crea un bucket S3 para almacenar scripts de EMR
2. Sube el script de clustering al bucket
3. Configura permisos y versionado
4. Retorna la URL del script para uso en EMR
"""

import boto3
import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Dict
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EMRScriptManager:
    """Gestor de scripts para EMR en S3"""
    
    def __init__(self, region: str = 'us-east-1'):
        """
        Inicializar gestor
        
        Args:
            region: Región de AWS
        """
        self.region = region
        self.s3_client = boto3.client('s3', region_name=region)
        self.s3_resource = boto3.resource('s3', region_name=region)
        
    def create_bucket(self, bucket_name: str, enable_versioning: bool = True) -> Dict:
        """
        Crear bucket S3 para scripts de EMR
        
        Args:
            bucket_name: Nombre del bucket
            enable_versioning: Habilitar versionado
            
        Returns:
            Dict con información del bucket creado
        """
        logger.info(f"Creando bucket: {bucket_name}")
        
        try:
            # Crear bucket
            if self.region == 'us-east-1':
                # us-east-1 no requiere LocationConstraint
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region}
                )
            
            logger.info(f"Bucket {bucket_name} creado exitosamente")
            
            # Habilitar versionado
            if enable_versioning:
                self.enable_versioning(bucket_name)
            
            # Configurar tags
            self.s3_client.put_bucket_tagging(
                Bucket=bucket_name,
                Tagging={
                    'TagSet': [
                        {'Key': 'Project', 'Value': 'Chinook-Analytics'},
                        {'Key': 'Purpose', 'Value': 'EMR-Scripts'},
                        {'Key': 'ManagedBy', 'Value': 'Python-Script'}
                    ]
                }
            )
            
            # Habilitar encriptación por defecto
            self.s3_client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration={
                    'Rules': [
                        {
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'AES256'
                            }
                        }
                    ]
                }
            )
            
            return {
                'bucket_name': bucket_name,
                'region': self.region,
                'versioning': enable_versioning,
                'encryption': 'AES256',
                'status': 'created'
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == 'BucketAlreadyOwnedByYou':
                logger.warning(f"Bucket {bucket_name} ya existe y es tuyo")
                return {
                    'bucket_name': bucket_name,
                    'region': self.region,
                    'status': 'already_exists'
                }
            elif error_code == 'BucketAlreadyExists':
                logger.error(f"Bucket {bucket_name} ya existe (propiedad de otra cuenta)")
                raise
            else:
                logger.error(f"Error creando bucket: {e}")
                raise
    
    def enable_versioning(self, bucket_name: str):
        """
        Habilitar versionado en bucket
        
        Args:
            bucket_name: Nombre del bucket
        """
        logger.info(f"Habilitando versionado en {bucket_name}")
        
        self.s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
    
    def upload_script(self,
                     bucket_name: str,
                     local_file_path: str,
                     s3_key: Optional[str] = None,
                     metadata: Optional[Dict] = None) -> Dict:
        """
        Subir script a S3
        
        Args:
            bucket_name: Nombre del bucket
            local_file_path: Ruta local del archivo
            s3_key: Clave S3 (path en el bucket). Si None, usa nombre del archivo
            metadata: Metadata adicional
            
        Returns:
            Dict con información del archivo subido
        """
        local_path = Path(local_file_path)
        
        if not local_path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {local_file_path}")
        
        # Determinar clave S3
        if s3_key is None:
            s3_key = f"scripts/{local_path.name}"
        
        logger.info(f"Subiendo {local_file_path} a s3://{bucket_name}/{s3_key}")
        
        # Preparar metadata
        extra_args = {
            'Metadata': metadata or {},
            'ContentType': 'text/x-python'
        }
        
        # Subir archivo
        try:
            self.s3_client.upload_file(
                Filename=str(local_path),
                Bucket=bucket_name,
                Key=s3_key,
                ExtraArgs=extra_args
            )
            
            logger.info("Archivo subido exitosamente")
            
            # Obtener información del objeto
            response = self.s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            
            return {
                'bucket': bucket_name,
                's3_key': s3_key,
                's3_url': f"s3://{bucket_name}/{s3_key}",
                'https_url': f"https://{bucket_name}.s3.{self.region}.amazonaws.com/{s3_key}",
                'size': response['ContentLength'],
                'version_id': response.get('VersionId'),
                'etag': response['ETag'],
                'last_modified': response['LastModified']
            }
            
        except ClientError as e:
            logger.error(f"Error subiendo archivo: {e}")
            raise
    
    def create_folder_structure(self, bucket_name: str):
        """
        Crear estructura de carpetas en el bucket
        
        Args:
            bucket_name: Nombre del bucket
        """
        logger.info(f"Creando estructura de carpetas en {bucket_name}")
        
        folders = [
            'scripts/',
            'scripts/clustering/',
            'scripts/etl/',
            'logs/',
            'output/',
            'output/clustering/',
            'notebooks/'
        ]
        
        for folder in folders:
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=folder,
                Body=b''
            )
            logger.info(f"Carpeta creada: {folder}")
    
    def list_scripts(self, bucket_name: str, prefix: str = 'scripts/') -> list:
        """
        Listar scripts en el bucket
        
        Args:
            bucket_name: Nombre del bucket
            prefix: Prefijo para filtrar
            
        Returns:
            Lista de scripts
        """
        logger.info(f"Listando scripts en s3://{bucket_name}/{prefix}")
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.info("No se encontraron scripts")
                return []
            
            scripts = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.py'):
                    scripts.append({
                        'key': obj['Key'],
                        's3_url': f"s3://{bucket_name}/{obj['Key']}",
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
            
            logger.info(f"Se encontraron {len(scripts)} scripts")
            return scripts
            
        except ClientError as e:
            logger.error(f"Error listando scripts: {e}")
            raise
    
    def download_script(self, bucket_name: str, s3_key: str, 
                       local_path: Optional[str] = None) -> str:
        """
        Descargar script desde S3
        
        Args:
            bucket_name: Nombre del bucket
            s3_key: Clave S3 del archivo
            local_path: Ruta local de destino (opcional)
            
        Returns:
            Ruta local del archivo descargado
        """
        if local_path is None:
            local_path = Path(s3_key).name
        
        logger.info(f"Descargando s3://{bucket_name}/{s3_key} a {local_path}")
        
        try:
            self.s3_client.download_file(
                Bucket=bucket_name,
                Key=s3_key,
                Filename=local_path
            )
            
            logger.info("Archivo descargado exitosamente")
            return local_path
            
        except ClientError as e:
            logger.error(f"Error descargando archivo: {e}")
            raise
    
    def setup_emr_environment(self,
                            bucket_name: str,
                            clustering_script_path: str) -> Dict:
        """
        Configurar entorno completo para EMR
        
        Args:
            bucket_name: Nombre del bucket
            clustering_script_path: Ruta local del script de clustering
            
        Returns:
            Dict con URLs y configuración
        """
        logger.info("="*60)
        logger.info("CONFIGURANDO ENTORNO EMR")
        logger.info("="*60)
        
        # 1. Crear bucket
        bucket_info = self.create_bucket(bucket_name)
        
        # 2. Crear estructura de carpetas
        self.create_folder_structure(bucket_name)
        
        # 3. Subir script de clustering
        script_info = self.upload_script(
            bucket_name=bucket_name,
            local_file_path=clustering_script_path,
            s3_key='scripts/clustering/customer_clustering.py',
            metadata={
                'Purpose': 'Customer-Clustering',
                'Type': 'PySpark-Script',
                'Version': '1.0'
            }
        )
        
        logger.info("="*60)
        logger.info("CONFIGURACIÓN COMPLETADA")
        logger.info("="*60)
        logger.info(f"\nBucket: {bucket_name}")
        logger.info(f"Script URL: {script_info['s3_url']}")
        logger.info(f"\nPara ejecutar en EMR:")
        logger.info(f"  spark-submit {script_info['s3_url']} --data-source s3 --s3-bucket YOUR_DATA_BUCKET")
        
        return {
            'bucket_info': bucket_info,
            'script_info': script_info,
            'bucket_name': bucket_name,
            'script_s3_url': script_info['s3_url']
        }


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description='Configurar S3 para scripts de EMR'
    )
    
    parser.add_argument(
        '--bucket-name',
        type=str,
        required=True,
        help='Nombre del bucket S3 a crear'
    )
    
    parser.add_argument(
        '--script-path',
        type=str,
        default='emr_clustering.py',
        help='Ruta del script de clustering (default: emr_clustering.py)'
    )
    
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='Región de AWS (default: us-east-1)'
    )
    
    parser.add_argument(
        '--list-scripts',
        action='store_true',
        help='Listar scripts existentes'
    )
    
    args = parser.parse_args()
    
    try:
        manager = EMRScriptManager(region=args.region)
        
        if args.list_scripts:
            scripts = manager.list_scripts(args.bucket_name)
            print("\nScripts encontrados:")
            for script in scripts:
                print(f"  - {script['key']}")
                print(f"    URL: {script['s3_url']}")
                print(f"    Size: {script['size']} bytes")
                print()
        else:
            result = manager.setup_emr_environment(
                bucket_name=args.bucket_name,
                clustering_script_path=args.script_path
            )
            
            print("\n" + "="*60)
            print("CONFIGURACIÓN EXITOSA")
            print("="*60)
            print(f"\nBucket creado: {result['bucket_name']}")
            print(f"Script URL: {result['script_s3_url']}")
            print("\nPróximos pasos:")
            print("1. Crear cluster EMR")
            print("2. Ejecutar el script:")
            print(f"   spark-submit {result['script_s3_url']} --data-source s3 --s3-bucket YOUR_DATA_BUCKET")
            
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
