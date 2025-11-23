"""
EMR PySpark - Customer Clustering por características de canciones

Este script realiza clustering de clientes basado en sus preferencias musicales:
- Características de las canciones que compran
- Patrones de compra por género musical
- Duración promedio de canciones
- Precio promedio de compra

Funciona tanto como:
1. Notebook de Jupyter/Zeppelin en EMR
2. Script standalone ejecutado por SSH/spark-submit
"""

import sys
import argparse
from typing import Optional, Tuple
import logging

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CustomerClusteringPipeline:
    """Pipeline de clustering de clientes basado en características musicales"""
    
    def __init__(self, spark: SparkSession, s3_bucket: Optional[str] = None):
        """
        Inicializar pipeline
        
        Args:
            spark: SparkSession activo
            s3_bucket: Bucket S3 para leer/escribir datos (opcional)
        """
        self.spark = spark
        self.s3_bucket = s3_bucket
        self.model = None
        self.scaler_model = None
        
    def read_from_s3(self, path: str) -> DataFrame:
        """
        Leer datos desde S3
        
        Args:
            path: Ruta relativa en el bucket (ej: 'processed/invoices/')
            
        Returns:
            DataFrame con los datos
        """
        full_path = f"s3://{self.s3_bucket}/{path}"
        logger.info(f"Leyendo datos desde: {full_path}")
        return self.spark.read.parquet(full_path)
    
    def read_from_mysql(self, jdbc_url: str, table: str, 
                       user: str, password: str) -> DataFrame:
        """
        Leer datos directamente desde MySQL/RDS
        
        Args:
            jdbc_url: URL JDBC de conexión
            table: Nombre de la tabla
            user: Usuario de base de datos
            password: Contraseña
            
        Returns:
            DataFrame con los datos
        """
        logger.info(f"Leyendo tabla {table} desde MySQL")
        return self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
    
    def prepare_customer_features(self, 
                                 customers_df: DataFrame,
                                 invoices_df: DataFrame,
                                 invoice_lines_df: DataFrame,
                                 tracks_df: DataFrame,
                                 genres_df: DataFrame) -> DataFrame:
        """
        Preparar características de clientes para clustering
        
        Features generadas:
        - total_purchases: Total de compras realizadas
        - total_spent: Total gastado
        - avg_track_price: Precio promedio de canciones compradas
        - avg_track_duration: Duración promedio (ms)
        - unique_genres: Cantidad de géneros únicos comprados
        - rock_percentage: % de compras de Rock
        - metal_percentage: % de compras de Metal
        - jazz_percentage: % de compras de Jazz
        - latin_percentage: % de compras de Latin
        - blues_percentage: % de compras de Blues
        - classical_percentage: % de compras de Classical
        - other_percentage: % de otros géneros
        
        Args:
            customers_df: DataFrame de clientes
            invoices_df: DataFrame de facturas
            invoice_lines_df: DataFrame de líneas de factura
            tracks_df: DataFrame de canciones
            genres_df: DataFrame de géneros
            
        Returns:
            DataFrame con características por cliente
        """
        logger.info("Preparando características de clientes...")
        
        # Unir datos de compras
        purchases = invoice_lines_df \
            .join(invoices_df, "InvoiceId") \
            .join(tracks_df, "TrackId") \
            .join(genres_df, "GenreId") \
            .select(
                "CustomerId",
                F.col("UnitPrice").alias("price"),
                F.col("Milliseconds").alias("duration"),
                genres_df.Name.alias("genre_name")
            )
        
        # Características básicas por cliente
        basic_features = purchases.groupBy("CustomerId").agg(
            F.count("*").alias("total_purchases"),
            F.sum("price").alias("total_spent"),
            F.avg("price").alias("avg_track_price"),
            F.avg("duration").alias("avg_track_duration"),
            F.countDistinct("genre_name").alias("unique_genres")
        )
        
        # Calcular porcentaje de cada género
        genre_totals = purchases.groupBy("CustomerId").agg(
            F.count("*").alias("total_tracks")
        )
        
        genre_counts = purchases.groupBy("CustomerId", "genre_name") \
            .agg(F.count("*").alias("genre_count"))
        
        # Pivot para obtener columnas por género
        genre_pivot = genre_counts.groupBy("CustomerId").pivot("genre_name").sum("genre_count").fillna(0)
        
        # Unir con totales para calcular porcentajes
        genre_percentages = genre_pivot.join(genre_totals, "CustomerId")
        
        # Calcular porcentajes para géneros principales
        for genre in ["Rock", "Metal", "Jazz", "Latin", "Blues", "Classical"]:
            col_name = f"{genre.lower()}_percentage"
            if genre in genre_pivot.columns:
                genre_percentages = genre_percentages.withColumn(
                    col_name,
                    (F.col(genre) / F.col("total_tracks") * 100)
                )
            else:
                genre_percentages = genre_percentages.withColumn(col_name, F.lit(0.0))
        
        # Calcular porcentaje de otros géneros
        genre_cols = [c for c in genre_pivot.columns if c != "CustomerId"]
        main_genres = ["Rock", "Metal", "Jazz", "Latin", "Blues", "Classical"]
        other_genre_cols = [c for c in genre_cols if c not in main_genres]
        
        if other_genre_cols:
            other_sum = sum(F.col(c) for c in other_genre_cols)
            genre_percentages = genre_percentages.withColumn(
                "other_percentage",
                (other_sum / F.col("total_tracks") * 100)
            )
        else:
            genre_percentages = genre_percentages.withColumn("other_percentage", F.lit(0.0))
        
        # Seleccionar solo las columnas necesarias
        genre_features = genre_percentages.select(
            "CustomerId",
            "rock_percentage",
            "metal_percentage",
            "jazz_percentage",
            "latin_percentage",
            "blues_percentage",
            "classical_percentage",
            "other_percentage"
        )
        
        # Unir características básicas con géneros
        customer_features = basic_features.join(genre_features, "CustomerId")
        
        # Unir con datos de clientes - usar expresión de join para evitar columnas duplicadas
        final_features = customers_df.join(
            customer_features, 
            customers_df.CustomerId == customer_features.CustomerId
        ).select(
            customers_df.CustomerId,
            customers_df.FirstName,
            customers_df.LastName,
            customers_df.Country,
            customer_features.total_purchases,
            customer_features.total_spent,
            customer_features.avg_track_price,
            customer_features.avg_track_duration,
            customer_features.unique_genres,
            customer_features.rock_percentage,
            customer_features.metal_percentage,
            customer_features.jazz_percentage,
            customer_features.latin_percentage,
            customer_features.blues_percentage,
            customer_features.classical_percentage,
            customer_features.other_percentage
        )
        
        logger.info(f"Features preparadas para {final_features.count()} clientes")
        return final_features
    
    def create_feature_vector(self, df: DataFrame) -> Tuple[DataFrame, list]:
        """
        Crear vector de características para ML
        
        Args:
            df: DataFrame con características
            
        Returns:
            Tupla (DataFrame con vectores, lista de nombres de features)
        """
        logger.info("Creando vectores de características...")
        
        feature_cols = [
            "total_purchases",
            "total_spent",
            "avg_track_price",
            "avg_track_duration",
            "unique_genres",
            "rock_percentage",
            "metal_percentage",
            "jazz_percentage",
            "latin_percentage",
            "blues_percentage",
            "classical_percentage",
            "other_percentage"
        ]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="raw_features"
        )
        
        df_with_features = assembler.transform(df)
        
        # Normalizar features
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        self.scaler_model = scaler.fit(df_with_features)
        df_scaled = self.scaler_model.transform(df_with_features)
        
        return df_scaled, feature_cols
    
    def find_optimal_k(self, df: DataFrame, k_range: range = range(2, 11)) -> int:
        """
        Encontrar número óptimo de clusters usando método del codo
        
        Args:
            df: DataFrame con features
            k_range: Rango de k a probar
            
        Returns:
            Número óptimo de clusters
        """
        logger.info(f"Buscando k óptimo en rango {k_range}...")
        
        evaluator = ClusteringEvaluator(
            predictionCol='cluster',
            featuresCol='features',
            metricName='silhouette',
            distanceMeasure='squaredEuclidean'
        )
        
        scores = []
        for k in k_range:
            kmeans = KMeans(k=k, seed=42, featuresCol='features', predictionCol='cluster')
            model = kmeans.fit(df)
            predictions = model.transform(df)
            score = evaluator.evaluate(predictions)
            scores.append((k, score))
            logger.info(f"k={k}, silhouette score={score:.4f}")
        
        # Seleccionar k con mejor score
        optimal_k = max(scores, key=lambda x: x[1])[0]
        logger.info(f"k óptimo seleccionado: {optimal_k}")
        
        return optimal_k
    
    def train_kmeans(self, df: DataFrame, k: int = 5) -> DataFrame:
        """
        Entrenar modelo KMeans
        
        Args:
            df: DataFrame con features
            k: Número de clusters
            
        Returns:
            DataFrame con predicciones de clusters
        """
        logger.info(f"Entrenando KMeans con k={k}...")
        
        kmeans = KMeans(
            k=k,
            seed=42,
            featuresCol='features',
            predictionCol='cluster',
            maxIter=100
        )
        
        self.model = kmeans.fit(df)
        predictions = self.model.transform(df)
        
        # Evaluar modelo
        evaluator = ClusteringEvaluator(
            predictionCol='cluster',
            featuresCol='features',
            metricName='silhouette'
        )
        silhouette = evaluator.evaluate(predictions)
        
        logger.info(f"Silhouette score: {silhouette:.4f}")
        logger.info(f"Within Set Sum of Squared Errors: {self.model.summary.trainingCost:.2f}")
        
        return predictions
    
    def analyze_clusters(self, predictions: DataFrame, feature_cols: list) -> DataFrame:
        """
        Analizar características de cada cluster
        
        Args:
            predictions: DataFrame con predicciones
            feature_cols: Lista de nombres de features
            
        Returns:
            DataFrame con estadísticas por cluster
        """
        logger.info("Analizando clusters...")
        
        # Calcular estadísticas por cluster
        agg_exprs = [F.count("*").alias("customer_count")]
        
        for col in feature_cols:
            agg_exprs.extend([
                F.avg(col).alias(f"{col}_mean"),
                F.stddev(col).alias(f"{col}_std")
            ])
        
        cluster_stats = predictions.groupBy("cluster").agg(*agg_exprs)
        
        # Mostrar ejemplos de clientes por cluster
        logger.info("\nEjemplos de clientes por cluster:")
        for cluster_id in range(self.model.getK()):
            logger.info(f"\nCluster {cluster_id}:")
            examples = predictions.filter(F.col("cluster") == cluster_id) \
                .select("CustomerId", "FirstName", "LastName", "Country", "total_purchases", "total_spent") \
                .limit(5)
            examples.show(truncate=False)
        
        return cluster_stats
    
    def save_results(self, predictions: DataFrame, cluster_stats: DataFrame, 
                    output_path: str):
        """
        Guardar resultados del clustering
        
        Args:
            predictions: DataFrame con predicciones
            cluster_stats: DataFrame con estadísticas de clusters
            output_path: Ruta donde guardar (S3 o local)
        """
        logger.info(f"Guardando resultados en {output_path}...")
        
        # Guardar predicciones
        predictions.select(
            "CustomerId", "FirstName", "LastName", "Country",
            "total_purchases", "total_spent", "cluster"
        ).write.mode("overwrite").parquet(f"{output_path}/customer_clusters")
        
        # Guardar estadísticas
        cluster_stats.write.mode("overwrite").parquet(f"{output_path}/cluster_statistics")
        
        # Guardar modelo
        self.model.write().overwrite().save(f"{output_path}/kmeans_model")
        self.scaler_model.write().overwrite().save(f"{output_path}/scaler_model")
        
        logger.info("Resultados guardados exitosamente")
    
    def run_pipeline(self,
                    data_source: str = "s3",
                    jdbc_url: Optional[str] = None,
                    db_user: Optional[str] = None,
                    db_password: Optional[str] = None,
                    k: Optional[int] = None,
                    find_optimal: bool = True,
                    output_path: Optional[str] = None):
        """
        Ejecutar pipeline completo de clustering
        
        Args:
            data_source: 's3' o 'mysql'
            jdbc_url: URL JDBC (si data_source='mysql')
            db_user: Usuario DB (si data_source='mysql')
            db_password: Password DB (si data_source='mysql')
            k: Número de clusters (None para búsqueda automática)
            find_optimal: Si buscar k óptimo automáticamente
            output_path: Ruta de salida (None para no guardar)
            
        Returns:
            Tupla (predictions, cluster_stats)
        """
        logger.info("="*60)
        logger.info("INICIANDO PIPELINE DE CLUSTERING DE CLIENTES")
        logger.info("="*60)
        
        # 1. Cargar datos
        if data_source == "s3":
            customers_df = self.read_from_s3("processed/customers/")
            invoices_df = self.read_from_s3("processed/invoices/")
            invoice_lines_df = self.read_from_s3("processed/invoice_lines/")
            tracks_df = self.read_from_s3("processed/tracks/")
            genres_df = self.read_from_s3("processed/genres/")
        elif data_source == "mysql":
            customers_df = self.read_from_mysql(jdbc_url, "Customer", db_user, db_password)
            invoices_df = self.read_from_mysql(jdbc_url, "Invoice", db_user, db_password)
            invoice_lines_df = self.read_from_mysql(jdbc_url, "InvoiceLine", db_user, db_password)
            tracks_df = self.read_from_mysql(jdbc_url, "Track", db_user, db_password)
            genres_df = self.read_from_mysql(jdbc_url, "Genre", db_user, db_password)
        else:
            raise ValueError(f"Data source no soportado: {data_source}")
        
        # 2. Preparar features
        customer_features = self.prepare_customer_features(
            customers_df, invoices_df, invoice_lines_df, tracks_df, genres_df
        )
        
        # 3. Crear vectores
        df_with_vectors, feature_cols = self.create_feature_vector(customer_features)
        
        # 4. Encontrar k óptimo o usar el especificado
        if k is None and find_optimal:
            k = self.find_optimal_k(df_with_vectors)
        elif k is None:
            k = 5  # Default
        
        # 5. Entrenar modelo
        predictions = self.train_kmeans(df_with_vectors, k)
        
        # 6. Analizar resultados
        cluster_stats = self.analyze_clusters(predictions, feature_cols)
        
        logger.info("\nEstadísticas de clusters:")
        cluster_stats.show(truncate=False)
        
        # 7. Guardar resultados
        if output_path:
            self.save_results(predictions, cluster_stats, output_path)
        
        logger.info("="*60)
        logger.info("PIPELINE COMPLETADO")
        logger.info("="*60)
        
        return predictions, cluster_stats


def main():
    """Función principal para ejecución como script"""
    parser = argparse.ArgumentParser(
        description='Customer Clustering basado en preferencias musicales'
    )
    
    parser.add_argument(
        '--data-source',
        choices=['s3', 'mysql'],
        default='s3',
        help='Fuente de datos: s3 o mysql'
    )
    
    parser.add_argument(
        '--s3-bucket',
        type=str,
        help='Bucket S3 para datos (requerido si data-source=s3)'
    )
    
    parser.add_argument(
        '--jdbc-url',
        type=str,
        help='JDBC URL para MySQL (requerido si data-source=mysql)'
    )
    
    parser.add_argument(
        '--db-user',
        type=str,
        help='Usuario de base de datos'
    )
    
    parser.add_argument(
        '--db-password',
        type=str,
        help='Contraseña de base de datos'
    )
    
    parser.add_argument(
        '--k',
        type=int,
        help='Número de clusters (omitir para búsqueda automática)'
    )
    
    parser.add_argument(
        '--no-optimize',
        action='store_true',
        help='No buscar k óptimo automáticamente'
    )
    
    parser.add_argument(
        '--output-path',
        type=str,
        help='Ruta de salida para resultados (S3 o local)'
    )
    
    args = parser.parse_args()
    
    # Validaciones
    if args.data_source == 's3' and not args.s3_bucket:
        parser.error("--s3-bucket es requerido cuando data-source=s3")
    
    if args.data_source == 'mysql' and not (args.jdbc_url and args.db_user and args.db_password):
        parser.error("--jdbc-url, --db-user y --db-password son requeridos cuando data-source=mysql")
    
    # Crear SparkSession
    spark = SparkSession.builder \
        .appName("CustomerClusteringKMeans") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Crear pipeline
        pipeline = CustomerClusteringPipeline(spark, args.s3_bucket)
        
        # Ejecutar
        predictions, cluster_stats = pipeline.run_pipeline(
            data_source=args.data_source,
            jdbc_url=args.jdbc_url,
            db_user=args.db_user,
            db_password=args.db_password,
            k=args.k,
            find_optimal=not args.no_optimize,
            output_path=args.output_path
        )
        
        logger.info("Ejecución completada exitosamente")
        
    except Exception as e:
        logger.error(f"Error en la ejecución: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
