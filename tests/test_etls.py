"""
Tests para los ETLs del proyecto

Este archivo contiene tests unitarios para todos los ETLs desarrollados.
Usa mocking para simular las conexiones a bases de datos y S3.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Agregar el directorio raíz del proyecto al path para importar paquete etls
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# Nueva librería de transformaciones desacopladas de Glue / RDS
from etls.etl_transform_lib import (
    transform_sales_per_day,
    compute_monthly_artist_metrics,
    select_top_artist_per_month,
    transform_sales_weekday,
    transform_monthly_sales
)




class TestETL1VentasPorDia:
    """Tests para ETL 1 - Ventas por Día (Data Lake / Glue Catalog)"""

    def test_transform_sales_per_day(self):
        # DataFrames simulando tablas raw en S3
        df_invoice = pd.DataFrame({
            'InvoiceId': [1, 2, 3],
            'InvoiceDate': ['2024-01-01', '2024-01-01', '2024-01-02'],
            'CustomerId': [10, 11, 10]
        })
        df_invoice_line = pd.DataFrame({
            'InvoiceId': [1, 1, 2, 3],
            'TrackId': [100, 101, 102, 103],
            'UnitPrice': [0.99, 0.99, 1.99, 2.99],
            'Quantity': [1, 2, 1, 3]
        })

        df_out = transform_sales_per_day(df_invoice, df_invoice_line)
        assert not df_out.empty
        assert 'fecha' in df_out.columns
        assert 'total_canciones_vendidas' in df_out.columns
        # Día 2024-01-01 tiene 3 líneas
        dia1 = df_out[df_out['fecha'] == pd.to_datetime('2024-01-01')].iloc[0]
        assert dia1['total_canciones_vendidas'] == 3
        assert dia1['numero_facturas'] == 2  # invoice 1 y 2


class TestETL2ArtistaMasVendido:
    """Tests para ETL 2 - Artista Más Vendido (Data Lake)"""

    def test_compute_monthly_artist_metrics_and_top(self):
        df_sales = pd.DataFrame({
            'InvoiceDate': ['2024-01-01', '2024-01-15', '2024-02-01', '2024-02-10'],
            'TrackId': [1, 2, 3, 4],
            'Quantity': [1, 2, 3, 4],
            'UnitPrice': [0.99, 1.99, 2.99, 0.99],
            'ArtistId': [10, 11, 10, 11],
            'ArtistName': ['Artist A', 'Artist B', 'Artist A', 'Artist B']
        })

        monthly = compute_monthly_artist_metrics(df_sales)
        assert not monthly.empty
        assert 'mes' in monthly.columns
        assert monthly['mes'].nunique() == 2

        top = select_top_artist_per_month(monthly)
        assert top['mes'].nunique() == 2
        assert 'porcentaje_canciones' in top.columns


class TestETL3DiaSemana:
    """Tests para ETL 3 - Día de la Semana (Data Lake)"""

    def test_transform_sales_weekday(self):
        df_invoice = pd.DataFrame({
            'InvoiceId': [1, 2, 3, 4],
            'InvoiceDate': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04'],
            'CustomerId': [10, 11, 12, 13]
        })
        df_invoice_line = pd.DataFrame({
            'InvoiceId': [1, 1, 2, 3, 4],
            'TrackId': [100, 101, 102, 103, 104],
            'UnitPrice': [0.99, 0.99, 1.99, 2.99, 3.99],
            'Quantity': [1, 2, 1, 3, 1]
        })
        df_out = transform_sales_weekday(df_invoice, df_invoice_line)
        assert not df_out.empty
        assert 'dia_semana' in df_out.columns
        assert 'total_canciones_vendidas' in df_out.columns


class TestETL4MesMayorVentas:
    """Tests para ETL 4 - Mes con Mayor Volumen (Data Lake)"""

    def test_transform_monthly_sales(self):
        df_invoice = pd.DataFrame({
            'InvoiceId': [1, 2, 3],
            'InvoiceDate': ['2024-01-01', '2024-01-15', '2024-02-01'],
            'CustomerId': [10, 11, 12]
        })
        df_invoice_line = pd.DataFrame({
            'InvoiceId': [1, 1, 2, 3],
            'TrackId': [100, 101, 102, 103],
            'UnitPrice': [0.99, 0.99, 1.99, 2.99],
            'Quantity': [1, 2, 1, 3]
        })
        df_out = transform_monthly_sales(df_invoice, df_invoice_line)
        assert not df_out.empty
        assert 'year_month' in df_out.columns
        assert 'total_canciones_vendidas' in df_out.columns

    def test_monthly_sales_aggregation_values(self):
        df_invoice = pd.DataFrame({
            'InvoiceId': [1],
            'InvoiceDate': ['2024-01-01'],
            'CustomerId': [10]
        })
        df_invoice_line = pd.DataFrame({
            'InvoiceId': [1],
            'TrackId': [100],
            'UnitPrice': [1.99],
            'Quantity': [3]
        })
        df_out = transform_monthly_sales(df_invoice, df_invoice_line)
        row = df_out.iloc[0]
        assert row['total_canciones_vendidas'] == 1
        assert row['cantidad_total'] == 3
        assert round(row['monto_total'], 2) == 5.97



if __name__ == '__main__':
    pytest.main([__file__, '-v'])
