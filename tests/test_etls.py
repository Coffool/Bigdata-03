"""
Tests para los ETLs del proyecto

Este archivo contiene tests unitarios para todos los ETLs desarrollados.
Usa mocking para simular las conexiones a bases de datos y S3.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os

# Agregar el directorio etls al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'etls'))

from etls.etl_0_full_copy import (
    get_all_tables, extract_table_data, extract_customer_employee_history,
    run_full_copy_etl
)
from etls.etl_1_ventas_por_dia import (
    extract_sales_per_day, transform_sales_data, run_sales_per_day_etl
)
from etls.etl_2_artista_mas_vendido import (
    extract_artist_sales_by_month, transform_top_artists_per_month,
    get_artist_ranking_per_month, run_top_artist_per_month_etl
)
from etls.etl_3_dia_semana import (
    extract_sales_by_weekday, transform_weekday_analysis,
    get_summary_statistics, run_weekday_analysis_etl
)
from etls.etl_4_mes_mayor_ventas import (
    extract_sales_by_month, transform_monthly_analysis,
    get_top_months, run_monthly_analysis_etl
)


class TestETL0FullCopy:
    """Tests para ETL 0 - Full Copy"""
    
    def test_get_all_tables(self):
        """Test que verifica la obtención de tablas"""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        
        # Configurar el context manager del cursor
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        
        # Simular resultado de SHOW TABLES
        mock_cursor.fetchall.return_value = [
            {'Tables_in_db': 'Artist'},
            {'Tables_in_db': 'Album'},
            {'Tables_in_db': 'Track'}
        ]
        
        tables = get_all_tables(mock_connection)
        
        assert len(tables) == 3
        assert 'Artist' in tables
        assert 'Album' in tables
        assert 'Track' in tables
    
    @patch('etls.etl_0_full_copy.pd.read_sql')
    def test_extract_table_data(self, mock_read_sql):
        """Test que verifica la extracción de datos de una tabla"""
        mock_connection = Mock()
        
        # Simular DataFrame de retorno
        mock_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })
        mock_read_sql.return_value = mock_df
        
        df = extract_table_data(mock_connection, 'Artist')
        
        assert len(df) == 3
        assert 'id' in df.columns
        assert 'name' in df.columns
        mock_read_sql.assert_called_once()
    
    @patch('etls.etl_0_full_copy.pd.read_sql')
    def test_extract_customer_employee_history(self, mock_read_sql):
        """Test que verifica la extracción del histórico cliente-empleado"""
        mock_connection = Mock()
        
        mock_df = pd.DataFrame({
            'CustomerId': [1, 2],
            'CustomerFirstName': ['John', 'Jane'],
            'EmployeeFirstName': ['Alice', 'Bob'],
            'ManagerFirstName': ['Charlie', 'David']
        })
        mock_read_sql.return_value = mock_df
        
        df = extract_customer_employee_history(mock_connection)
        
        assert len(df) == 2
        assert 'CustomerId' in df.columns
        assert 'ManagerFirstName' in df.columns


class TestETL1VentasPorDia:
    """Tests para ETL 1 - Ventas por Día"""
    
    @patch('etls.etl_1_ventas_por_dia.pd.read_sql')
    def test_extract_sales_per_day(self, mock_read_sql):
        """Test que verifica la extracción de ventas por día"""
        mock_connection = Mock()
        
        mock_df = pd.DataFrame({
            'fecha': ['2024-01-01', '2024-01-02'],
            'total_canciones_vendidas': [10, 15],
            'numero_facturas': [5, 7],
            'monto_total': [100.0, 150.0]
        })
        mock_read_sql.return_value = mock_df
        
        df = extract_sales_per_day(mock_connection)
        
        assert len(df) == 2
        assert 'fecha' in df.columns
        assert 'total_canciones_vendidas' in df.columns
    
    def test_transform_sales_data(self):
        """Test que verifica la transformación de datos de ventas"""
        df_input = pd.DataFrame({
            'fecha': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'total_canciones_vendidas': [10, 15],
            'numero_facturas': [5, 7],
            'cantidad_total': [10, 15],
            'clientes_unicos': [3, 4],
            'monto_total': [100.0, 150.0]
        })
        
        df_output = transform_sales_data(df_input)
        
        assert 'dia_semana' in df_output.columns
        assert 'mes' in df_output.columns
        assert 'año' in df_output.columns
        assert 'avg_canciones_por_factura' in df_output.columns
        assert 'ticket_promedio' in df_output.columns
        
        # Verificar cálculos
        assert df_output.iloc[0]['avg_canciones_por_factura'] == 2.0
        assert df_output.iloc[0]['ticket_promedio'] == 20.0


class TestETL2ArtistaMasVendido:
    """Tests para ETL 2 - Artista Más Vendido"""
    
    @patch('etls.etl_2_artista_mas_vendido.pd.read_sql')
    def test_extract_artist_sales_by_month(self, mock_read_sql):
        """Test que verifica la extracción de ventas por artista y mes"""
        mock_connection = Mock()
        
        mock_df = pd.DataFrame({
            'mes': ['2024-01-01', '2024-01-01'],
            'año': [2024, 2024],
            'numero_mes': [1, 1],
            'ArtistId': [1, 2],
            'nombre_artista': ['Artist A', 'Artist B'],
            'total_canciones_vendidas': [100, 80],
            'monto_total': [1000.0, 800.0]
        })
        mock_read_sql.return_value = mock_df
        
        df = extract_artist_sales_by_month(mock_connection)
        
        assert len(df) == 2
        assert 'nombre_artista' in df.columns
    
    def test_transform_top_artists_per_month(self):
        """Test que verifica la transformación para obtener top artista por mes"""
        df_input = pd.DataFrame({
            'mes': ['2024-01-01', '2024-01-01', '2024-02-01'],
            'año': [2024, 2024, 2024],
            'numero_mes': [1, 1, 2],
            'ArtistId': [1, 2, 1],
            'nombre_artista': ['Artist A', 'Artist B', 'Artist A'],
            'total_canciones_vendidas': [100, 80, 120],
            'cantidad_total': [100, 80, 120],
            'monto_total': [1000.0, 800.0, 1200.0],
            'numero_facturas': [50, 40, 60],
            'clientes_unicos': [30, 25, 35],
            'albums_vendidos': [10, 8, 12]
        })
        
        df_output = transform_top_artists_per_month(df_input)
        
        # Debe retornar solo 2 filas (un artista por mes)
        assert len(df_output) == 2
        assert 'porcentaje_canciones' in df_output.columns
        assert 'precio_promedio_cancion' in df_output.columns
        
        # El primer mes debe tener Artist A (100 > 80)
        enero = df_output[df_output['numero_mes'] == 1].iloc[0]
        assert enero['nombre_artista'] == 'Artist A'
    
    def test_get_artist_ranking_per_month(self):
        """Test que verifica el ranking de artistas por mes"""
        df_input = pd.DataFrame({
            'mes': ['2024-01-01'] * 5,
            'año': [2024] * 5,
            'numero_mes': [1] * 5,
            'ArtistId': [1, 2, 3, 4, 5],
            'nombre_artista': ['A', 'B', 'C', 'D', 'E'],
            'total_canciones_vendidas': [100, 90, 80, 70, 60],
            'cantidad_total': [100, 90, 80, 70, 60],
            'monto_total': [1000, 900, 800, 700, 600],
            'numero_facturas': [50, 45, 40, 35, 30],
            'clientes_unicos': [30, 28, 26, 24, 22],
            'albums_vendidos': [10, 9, 8, 7, 6]
        })
        
        df_output = get_artist_ranking_per_month(df_input, top_n=3)
        
        # Debe retornar solo top 3
        assert len(df_output) == 3
        assert 'ranking' in df_output.columns
        assert df_output.iloc[0]['ranking'] == 1
        assert df_output.iloc[0]['nombre_artista'] == 'A'


class TestETL3DiaSemana:
    """Tests para ETL 3 - Día de la Semana"""
    
    @patch('etls.etl_3_dia_semana.pd.read_sql')
    def test_extract_sales_by_weekday(self, mock_read_sql):
        """Test que verifica la extracción de ventas por día de semana"""
        mock_connection = Mock()
        
        mock_df = pd.DataFrame({
            'numero_dia_semana': [1, 2, 3, 4, 5, 6, 7],
            'dia_semana': ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 
                          'Thursday', 'Friday', 'Saturday'],
            'total_canciones_vendidas': [100, 120, 130, 125, 140, 150, 110],
            'numero_facturas': [50, 60, 65, 62, 70, 75, 55],
            'monto_total': [1000, 1200, 1300, 1250, 1400, 1500, 1100],
            'dias_registrados': [50, 50, 50, 50, 50, 50, 50],
            'cantidad_total': [100, 120, 130, 125, 140, 150, 110],
            'clientes_unicos': [40, 45, 50, 48, 55, 60, 45],
            'monto_promedio_linea': [20, 20, 20, 20, 20, 20, 20]
        })
        mock_read_sql.return_value = mock_df
        
        df = extract_sales_by_weekday(mock_connection)
        
        assert len(df) == 7
        assert 'dia_semana' in df.columns
    
    def test_transform_weekday_analysis(self):
        """Test que verifica la transformación del análisis por día"""
        df_input = pd.DataFrame({
            'numero_dia_semana': [1, 2, 3],
            'dia_semana': ['Sunday', 'Monday', 'Tuesday'],
            'total_canciones_vendidas': [100, 120, 130],
            'numero_facturas': [50, 60, 65],
            'monto_total': [1000.0, 1200.0, 1300.0],
            'dias_registrados': [50, 50, 50],
            'cantidad_total': [100, 120, 130],
            'clientes_unicos': [40, 45, 50],
            'monto_promedio_linea': [20.0, 20.0, 20.0]
        })
        
        df_output = transform_weekday_analysis(df_input)
        
        assert 'promedio_canciones_por_dia' in df_output.columns
        assert 'dia_semana_es' in df_output.columns
        assert 'tipo_dia' in df_output.columns
        assert 'ranking_por_canciones' in df_output.columns
        
        # Verificar cálculos
        assert df_output.iloc[0]['promedio_canciones_por_dia'] == 2.0
        
        # Verificar traducción
        assert df_output[df_output['dia_semana'] == 'Sunday']['dia_semana_es'].iloc[0] == 'Domingo'
        
        # Verificar tipo de día
        assert df_output.iloc[0]['tipo_dia'] == 'Fin de semana'
    
    def test_get_summary_statistics(self):
        """Test que verifica las estadísticas resumen"""
        df_input = pd.DataFrame({
            'numero_dia_semana': [1, 2, 3, 4, 5, 6, 7],
            'dia_semana': ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 
                          'Thursday', 'Friday', 'Saturday'],
            'dia_semana_es': ['Domingo', 'Lunes', 'Martes', 'Miércoles',
                             'Jueves', 'Viernes', 'Sábado'],
            'total_canciones_vendidas': [100, 120, 130, 125, 140, 150, 110],
            'numero_facturas': [50, 60, 65, 62, 70, 75, 55],
            'monto_total': [1000.0, 1200.0, 1300.0, 1250.0, 1400.0, 1500.0, 1100.0],
            'porcentaje_canciones': [11.3, 13.6, 14.7, 14.2, 15.8, 17.0, 12.4],
            'porcentaje_monto': [11.4, 13.7, 14.9, 14.3, 16.0, 17.2, 12.6],
            'tipo_dia': ['Fin de semana', 'Entre semana', 'Entre semana', 
                        'Entre semana', 'Entre semana', 'Entre semana', 'Fin de semana'],
            'promedio_canciones_por_dia': [2.0, 2.4, 2.6, 2.5, 2.8, 3.0, 2.2],
            'ranking_por_canciones': [7, 5, 3, 4, 2, 1, 6],
            'ranking_por_monto': [7, 5, 3, 4, 2, 1, 6]
        })
        
        summary = get_summary_statistics(df_input)
        
        assert 'mejor_dia_por_ventas' in summary
        assert 'mejor_dia_por_ingresos' in summary
        assert 'comparacion_tipo_dia' in summary
        
        # Verificar que Friday es el mejor día
        assert summary['mejor_dia_por_ventas']['dia'] == 'Friday'
        assert summary['mejor_dia_por_ventas']['canciones'] == 150


class TestETL4MesMayorVentas:
    """Tests para ETL 4 - Mes con Mayor Volumen"""
    
    @patch('etls.etl_4_mes_mayor_ventas.pd.read_sql')
    def test_extract_sales_by_month(self, mock_read_sql):
        """Test que verifica la extracción de ventas por mes"""
        mock_connection = Mock()
        
        mock_df = pd.DataFrame({
            'mes': ['2024-01-01', '2024-02-01', '2024-03-01'],
            'año': [2024, 2024, 2024],
            'numero_mes': [1, 2, 3],
            'nombre_mes': ['January', 'February', 'March'],
            'trimestre': [1, 1, 1],
            'total_canciones_vendidas': [1000, 1200, 1100],
            'numero_facturas': [500, 600, 550],
            'monto_total': [10000.0, 12000.0, 11000.0],
            'dias_con_ventas': [28, 25, 30],
            'cantidad_total': [1000, 1200, 1100],
            'clientes_unicos': [300, 350, 320],
            'monto_promedio_linea': [20.0, 20.0, 20.0],
            'primera_venta': ['2024-01-01', '2024-02-01', '2024-03-01'],
            'ultima_venta': ['2024-01-31', '2024-02-29', '2024-03-31']
        })
        mock_read_sql.return_value = mock_df
        
        df = extract_sales_by_month(mock_connection)
        
        assert len(df) == 3
        assert 'nombre_mes' in df.columns
    
    def test_transform_monthly_analysis(self):
        """Test que verifica la transformación del análisis mensual"""
        df_input = pd.DataFrame({
            'mes': ['2024-01-01', '2024-02-01'],
            'año': [2024, 2024],
            'numero_mes': [1, 2],
            'nombre_mes': ['January', 'February'],
            'trimestre': [1, 1],
            'total_canciones_vendidas': [1000, 1200],
            'numero_facturas': [500, 600],
            'monto_total': [10000.0, 12000.0],
            'dias_con_ventas': [28, 25],
            'cantidad_total': [1000, 1200],
            'clientes_unicos': [300, 350],
            'monto_promedio_linea': [20.0, 20.0],
            'primera_venta': ['2024-01-01', '2024-02-01'],
            'ultima_venta': ['2024-01-31', '2024-02-29']
        })
        
        df_output = transform_monthly_analysis(df_input)
        
        assert 'promedio_canciones_por_dia' in df_output.columns
        assert 'nombre_mes_es' in df_output.columns
        assert 'ranking_historico' in df_output.columns
        assert 'crecimiento_canciones' in df_output.columns
        
        # Verificar traducción de mes
        assert 'Enero' in df_output['nombre_mes_es'].values
        assert 'Febrero' in df_output['nombre_mes_es'].values
    
    def test_get_top_months(self):
        """Test que verifica la obtención de top meses"""
        df_input = pd.DataFrame({
            'mes': pd.to_datetime(['2024-01-01', '2024-02-01', '2024-03-01']),
            'ranking_historico': [2, 1, 3],
            'total_canciones_vendidas': [1000, 1200, 900],
            'monto_total': [10000, 12000, 9000]
        })
        
        df_output = get_top_months(df_input, top_n=2)
        
        # Debe retornar solo 2 meses
        assert len(df_output) == 2
        
        # El primero debe ser el de ranking 1
        assert df_output.iloc[0]['ranking_historico'] == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
