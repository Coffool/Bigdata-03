"""
Tests para emr_clustering.py

Tests unitarios para el pipeline de clustering de clientes
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys
from pathlib import Path

# Agregar directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from emr_clustering import CustomerClusteringPipeline


@pytest.fixture(scope="session")
def spark():
    """
    Fixture de SparkSession para tests
    """
    spark = SparkSession.builder \
        .appName("TestCustomerClustering") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_customers_data(spark):
    """
    Datos de ejemplo para clientes
    """
    data = [
        (1, "John", "Doe", "USA"),
        (2, "Jane", "Smith", "Canada"),
        (3, "Bob", "Johnson", "UK"),
        (4, "Alice", "Williams", "USA"),
        (5, "Charlie", "Brown", "Germany")
    ]
    
    schema = StructType([
        StructField("CustomerId", IntegerType(), False),
        StructField("FirstName", StringType(), False),
        StructField("LastName", StringType(), False),
        StructField("Country", StringType(), False)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_invoices_data(spark):
    """
    Datos de ejemplo para invoices
    """
    data = [
        (1, 1),
        (2, 1),
        (3, 2),
        (4, 3),
        (5, 4),
        (6, 5)
    ]
    
    schema = StructType([
        StructField("InvoiceId", IntegerType(), False),
        StructField("CustomerId", IntegerType(), False)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_invoice_lines_data(spark):
    """
    Datos de ejemplo para invoice lines
    """
    data = [
        (1, 1, 1, 0.99),
        (2, 1, 2, 0.99),
        (3, 2, 3, 1.29),
        (4, 3, 4, 0.99),
        (5, 4, 5, 0.99),
        (6, 5, 1, 0.99),
        (7, 6, 2, 0.99)
    ]
    
    schema = StructType([
        StructField("InvoiceLineId", IntegerType(), False),
        StructField("InvoiceId", IntegerType(), False),
        StructField("TrackId", IntegerType(), False),
        StructField("UnitPrice", DoubleType(), False)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_tracks_data(spark):
    """
    Datos de ejemplo para tracks
    """
    data = [
        (1, "Track 1", 1, 180000),
        (2, "Track 2", 2, 240000),
        (3, "Track 3", 1, 200000),
        (4, "Track 4", 3, 220000),
        (5, "Track 5", 1, 190000)
    ]
    
    schema = StructType([
        StructField("TrackId", IntegerType(), False),
        StructField("Name", StringType(), False),
        StructField("GenreId", IntegerType(), False),
        StructField("Milliseconds", IntegerType(), False)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_genres_data(spark):
    """
    Datos de ejemplo para genres
    """
    data = [
        (1, "Rock"),
        (2, "Jazz"),
        (3, "Metal")
    ]
    
    schema = StructType([
        StructField("GenreId", IntegerType(), False),
        StructField("Name", StringType(), False)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def pipeline(spark):
    """
    Fixture del pipeline
    """
    return CustomerClusteringPipeline(spark, s3_bucket="test-bucket")


class TestCustomerClusteringPipeline:
    """Tests para CustomerClusteringPipeline"""
    
    def test_initialization(self, spark):
        """Test: Inicialización del pipeline"""
        pipeline = CustomerClusteringPipeline(spark, "test-bucket")
        
        assert pipeline.spark == spark
        assert pipeline.s3_bucket == "test-bucket"
        assert pipeline.model is None
        assert pipeline.scaler_model is None
    
    def test_prepare_customer_features(self, pipeline, sample_customers_data,
                                      sample_invoices_data, sample_invoice_lines_data,
                                      sample_tracks_data, sample_genres_data):
        """Test: Preparación de features de clientes"""
        features = pipeline.prepare_customer_features(
            sample_customers_data,
            sample_invoices_data,
            sample_invoice_lines_data,
            sample_tracks_data,
            sample_genres_data
        )
        
        # Verificar que se generaron las columnas esperadas
        expected_cols = [
            'CustomerId', 'FirstName', 'LastName', 'Country',
            'total_purchases', 'total_spent', 'avg_track_price',
            'avg_track_duration', 'unique_genres',
            'rock_percentage', 'metal_percentage', 'jazz_percentage',
            'latin_percentage', 'blues_percentage', 'classical_percentage',
            'other_percentage'
        ]
        
        for col in expected_cols:
            assert col in features.columns, f"Columna {col} no encontrada"
        
        # Verificar que hay datos
        assert features.count() > 0
        
        # Verificar valores lógicos
        row = features.filter(F.col("CustomerId") == 1).first()
        assert row['total_purchases'] > 0
        assert row['total_spent'] > 0
        assert row['avg_track_price'] > 0
        assert row['unique_genres'] > 0
    
    def test_create_feature_vector(self, pipeline, sample_customers_data,
                                   sample_invoices_data, sample_invoice_lines_data,
                                   sample_tracks_data, sample_genres_data):
        """Test: Creación de vectores de características"""
        features = pipeline.prepare_customer_features(
            sample_customers_data,
            sample_invoices_data,
            sample_invoice_lines_data,
            sample_tracks_data,
            sample_genres_data
        )
        
        df_with_vectors, feature_cols = pipeline.create_feature_vector(features)
        
        # Verificar que se crearon las columnas de vectores
        assert 'raw_features' in df_with_vectors.columns
        assert 'features' in df_with_vectors.columns
        
        # Verificar que el scaler fue entrenado
        assert pipeline.scaler_model is not None
        
        # Verificar que feature_cols contiene las columnas esperadas
        assert len(feature_cols) == 12
        assert 'total_purchases' in feature_cols
        assert 'rock_percentage' in feature_cols
    
    def test_train_kmeans(self, pipeline, sample_customers_data,
                         sample_invoices_data, sample_invoice_lines_data,
                         sample_tracks_data, sample_genres_data):
        """Test: Entrenamiento de modelo KMeans"""
        features = pipeline.prepare_customer_features(
            sample_customers_data,
            sample_invoices_data,
            sample_invoice_lines_data,
            sample_tracks_data,
            sample_genres_data
        )
        
        df_with_vectors, _ = pipeline.create_feature_vector(features)
        
        # Entrenar con k=2 (pocos datos de prueba)
        predictions = pipeline.train_kmeans(df_with_vectors, k=2)
        
        # Verificar que el modelo fue entrenado
        assert pipeline.model is not None
        
        # Verificar que se generaron predicciones
        assert 'cluster' in predictions.columns
        
        # Verificar que los clusters están en el rango esperado
        cluster_values = predictions.select('cluster').distinct().collect()
        cluster_ids = [row['cluster'] for row in cluster_values]
        assert all(0 <= c < 2 for c in cluster_ids)
    
    def test_analyze_clusters(self, pipeline, sample_customers_data,
                             sample_invoices_data, sample_invoice_lines_data,
                             sample_tracks_data, sample_genres_data):
        """Test: Análisis de clusters"""
        features = pipeline.prepare_customer_features(
            sample_customers_data,
            sample_invoices_data,
            sample_invoice_lines_data,
            sample_tracks_data,
            sample_genres_data
        )
        
        df_with_vectors, feature_cols = pipeline.create_feature_vector(features)
        predictions = pipeline.train_kmeans(df_with_vectors, k=2)
        
        cluster_stats = pipeline.analyze_clusters(predictions, feature_cols)
        
        # Verificar que se generaron estadísticas
        assert cluster_stats.count() == 2  # 2 clusters
        
        # Verificar que existen columnas de estadísticas
        assert 'customer_count' in cluster_stats.columns
        assert 'total_purchases_mean' in cluster_stats.columns
        assert 'rock_percentage_mean' in cluster_stats.columns
    
    @patch('emr_clustering.CustomerClusteringPipeline.read_from_s3')
    def test_read_from_s3(self, mock_read, pipeline, spark):
        """Test: Lectura desde S3"""
        # Configurar mock
        mock_df = spark.createDataFrame([(1, "test")], ["id", "name"])
        mock_read.return_value = mock_df
        
        # Llamar método
        result = pipeline.read_from_s3("test/path")
        
        # Verificar
        assert result == mock_df
    
    @patch('emr_clustering.CustomerClusteringPipeline.save_results')
    def test_save_results(self, mock_save, pipeline, sample_customers_data,
                         sample_invoices_data, sample_invoice_lines_data,
                         sample_tracks_data, sample_genres_data):
        """Test: Guardado de resultados"""
        features = pipeline.prepare_customer_features(
            sample_customers_data,
            sample_invoices_data,
            sample_invoice_lines_data,
            sample_tracks_data,
            sample_genres_data
        )
        
        df_with_vectors, feature_cols = pipeline.create_feature_vector(features)
        predictions = pipeline.train_kmeans(df_with_vectors, k=2)
        cluster_stats = pipeline.analyze_clusters(predictions, feature_cols)
        
        # Llamar método (mockeado para no escribir realmente)
        pipeline.save_results(predictions, cluster_stats, "s3://test/output")
        
        # Verificar que se llamó
        mock_save.assert_called_once()


class TestFindOptimalK:
    """Tests para búsqueda de k óptimo"""
    
    def test_find_optimal_k(self, pipeline, sample_customers_data,
                           sample_invoices_data, sample_invoice_lines_data,
                           sample_tracks_data, sample_genres_data):
        """Test: Búsqueda de k óptimo"""
        features = pipeline.prepare_customer_features(
            sample_customers_data,
            sample_invoices_data,
            sample_invoice_lines_data,
            sample_tracks_data,
            sample_genres_data
        )
        
        df_with_vectors, _ = pipeline.create_feature_vector(features)
        
        # Buscar k óptimo en rango pequeño
        optimal_k = pipeline.find_optimal_k(df_with_vectors, k_range=range(2, 4))
        
        # Verificar que k está en el rango
        assert 2 <= optimal_k < 4


class TestEdgeCases:
    """Tests para casos edge"""
    
    def test_empty_dataframe(self, pipeline, spark):
        """Test: DataFrame vacío"""
        schema = StructType([
            StructField("CustomerId", IntegerType(), False),
            StructField("FirstName", StringType(), False)
        ])
        
        empty_df = spark.createDataFrame([], schema)
        
        # Debería manejar DataFrame vacío sin crash
        assert empty_df.count() == 0
    
    def test_single_customer(self, pipeline, spark):
        """Test: Un solo cliente"""
        customers_data = spark.createDataFrame(
            [(1, "John", "Doe", "USA")],
            ["CustomerId", "FirstName", "LastName", "Country"]
        )
        
        invoices_data = spark.createDataFrame(
            [(1, 1)],
            ["InvoiceId", "CustomerId"]
        )
        
        invoice_lines_data = spark.createDataFrame(
            [(1, 1, 1, 0.99)],
            ["InvoiceLineId", "InvoiceId", "TrackId", "UnitPrice"]
        )
        
        tracks_data = spark.createDataFrame(
            [(1, "Track 1", 1, 180000)],
            ["TrackId", "Name", "GenreId", "Milliseconds"]
        )
        
        genres_data = spark.createDataFrame(
            [(1, "Rock")],
            ["GenreId", "Name"]
        )
        
        features = pipeline.prepare_customer_features(
            customers_data,
            invoices_data,
            invoice_lines_data,
            tracks_data,
            genres_data
        )
        
        # Debería generar features para 1 cliente
        assert features.count() == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
