"""
Testes Unitários: Agregações de Dados
Projeto: SPTrans Real-Time Data Pipeline
Autor: Equipe LABDATA/FIA

Testa as funções de agregação do pipeline:
- Agregações temporais (hourly, daily)
- Agregações por rota
- Cálculo de KPIs
- Métricas operacionais
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import col, count, avg, sum as spark_sum, min as spark_min, max as spark_max


class TestHourlyAggregations:
    """Testes para agregações horárias"""
    
    def test_aggregate_vehicles_per_hour(self):
        """Testa agregação de veículos por hora"""
        # Criar dados de teste
        timestamps = pd.date_range('2025-10-29 08:00:00', periods=120, freq='1min')
        data = {
            'timestamp': timestamps,
            'vehicle_id': [f'V{i%10}' for i in range(len(timestamps))]
        }
        df = pd.DataFrame(data)
        df['hour'] = df['timestamp'].dt.floor('H')
        
        # Agregar por hora
        hourly = df.groupby('hour').agg({
            'vehicle_id': 'nunique'
        }).rename(columns={'vehicle_id': 'unique_vehicles'})
        
        assert len(hourly) == 2  # 2 horas de dados
        assert all(hourly['unique_vehicles'] > 0)
    
    def test_calculate_average_speed_per_hour(self):
        """Testa cálculo de velocidade média por hora"""
        timestamps = pd.date_range('2025-10-29 08:00:00', periods=100, freq='1min')
        data = {
            'timestamp': timestamps,
            'vehicle_id': [f'V{i%10}' for i in range(len(timestamps))],
            'speed': np.random.uniform(20, 60, len(timestamps))
        }
        df = pd.DataFrame(data)
        df['hour'] = df['timestamp'].dt.floor('H')
        
        # Calcular velocidade média por hora
        hourly_speed = df.groupby('hour')['speed'].mean()
        
        assert len(hourly_speed) == 2
        assert all(hourly_speed >= 20) and all(hourly_speed <= 60)
    
    def test_count_trips_per_hour(self):
        """Testa contagem de viagens por hora"""
        # Simular viagens
        timestamps = pd.date_range('2025-10-29 06:00:00', periods=100, freq='15min')
        data = {
            'timestamp': timestamps,
            'trip_id': [f'T{i}' for i in range(len(timestamps))],
            'route_id': np.random.choice(['R1', 'R2', 'R3'], len(timestamps))
        }
        df = pd.DataFrame(data)
        df['hour'] = df['timestamp'].dt.floor('H')
        
        # Contar viagens por hora
        trips_per_hour = df.groupby('hour')['trip_id'].count()
        
        assert len(trips_per_hour) > 0
        assert trips_per_hour.sum() == len(df)


class TestRouteAggregations:
    """Testes para agregações por rota"""
    
    def test_aggregate_metrics_by_route(self):
        """Testa agregação de métricas por rota"""
        data = {
            'route_id': ['R1', 'R1', 'R1', 'R2', 'R2', 'R3'],
            'num_vehicles': [5, 6, 4, 3, 4, 8],
            'avg_speed': [40, 42, 38, 35, 36, 45]
        }
        df = pd.DataFrame(data)
        
        # Agregar por rota
        route_metrics = df.groupby('route_id').agg({
            'num_vehicles': ['mean', 'max'],
            'avg_speed': ['mean', 'min', 'max']
        })
        
        assert len(route_metrics) == 3
        assert route_metrics[('num_vehicles', 'mean')]['R1'] == 5.0
    
    def test_calculate_headway_by_route(self):
        """Testa cálculo de headway (intervalo entre ônibus) por rota"""
        # Simular passagens de ônibus
        data = {
            'route_id': ['R1', 'R1', 'R1', 'R1', 'R2', 'R2'],
            'stop_id': ['S1', 'S1', 'S1', 'S1', 'S1', 'S1'],
            'timestamp': pd.to_datetime([
                '2025-10-29 08:00:00',
                '2025-10-29 08:10:00',
                '2025-10-29 08:20:00',
                '2025-10-29 08:30:00',
                '2025-10-29 08:00:00',
                '2025-10-29 08:25:00'
            ])
        }
        df = pd.DataFrame(data)
        
        # Calcular headway
        headways = []
        for route in df['route_id'].unique():
            route_df = df[df['route_id'] == route].sort_values('timestamp')
            if len(route_df) > 1:
                route_df['headway'] = route_df['timestamp'].diff().dt.total_seconds() / 60
                headways.extend(route_df['headway'].dropna().tolist())
        
        assert len(headways) > 0
        assert all(h > 0 for h in headways)
    
    def test_route_performance_score(self):
        """Testa cálculo de score de performance por rota"""
        data = {
            'route_id': ['R1', 'R2', 'R3'],
            'on_time_rate': [0.95, 0.85, 0.90],
            'avg_occupancy': [0.75, 0.85, 0.70],
            'avg_speed': [35, 30, 40]
        }
        df = pd.DataFrame(data)
        
        # Calcular score composto (0-100)
        df['performance_score'] = (
            df['on_time_rate'] * 40 +
            df['avg_occupancy'] * 30 +
            (df['avg_speed'] / 50) * 30
        ) * 100
        
        assert all(df['performance_score'] >= 0)
        assert all(df['performance_score'] <= 100)
        assert df['performance_score']['R1'] > df['performance_score']['R2']


class TestKPICalculations:
    """Testes para cálculo de KPIs"""
    
    def test_calculate_fleet_utilization(self):
        """Testa cálculo de utilização da frota"""
        total_vehicles = 100
        active_vehicles = 85
        
        utilization_rate = (active_vehicles / total_vehicles) * 100
        
        assert utilization_rate == 85.0
        assert 0 <= utilization_rate <= 100
    
    def test_calculate_average_occupancy(self):
        """Testa cálculo de ocupação média"""
        data = {
            'vehicle_id': ['V1', 'V2', 'V3', 'V4', 'V5'],
            'passengers': [45, 50, 30, 40, 55],
            'capacity': [60, 60, 60, 60, 60]
        }
        df = pd.DataFrame(data)
        
        df['occupancy_rate'] = (df['passengers'] / df['capacity']) * 100
        avg_occupancy = df['occupancy_rate'].mean()
        
        assert 0 <= avg_occupancy <= 100
        assert avg_occupancy == pytest.approx(73.33, 0.1)
    
    def test_calculate_on_time_performance(self):
        """Testa cálculo de pontualidade"""
        data = {
            'trip_id': [f'T{i}' for i in range(10)],
            'scheduled_time': pd.date_range('2025-10-29 08:00:00', periods=10, freq='10min'),
            'actual_time': pd.date_range('2025-10-29 08:00:00', periods=10, freq='10min') + pd.to_timedelta([0, 2, -1, 5, 1, 0, 3, -2, 6, 1], unit='min')
        }
        df = pd.DataFrame(data)
        
        # Calcular diferença em minutos
        df['delay_minutes'] = (df['actual_time'] - df['scheduled_time']).dt.total_seconds() / 60
        
        # Considerar on-time se delay <= 3 minutos
        df['is_on_time'] = df['delay_minutes'].abs() <= 3
        on_time_rate = (df['is_on_time'].sum() / len(df)) * 100
        
        assert 0 <= on_time_rate <= 100
        assert on_time_rate == 80.0
    
    def test_calculate_service_reliability(self):
        """Testa cálculo de confiabilidade do serviço"""
        data = {
            'route_id': ['R1'] * 10,
            'trip_completed': [True, True, False, True, True, True, True, True, False, True]
        }
        df = pd.DataFrame(data)
        
        reliability = (df['trip_completed'].sum() / len(df)) * 100
        
        assert reliability == 80.0
        assert 0 <= reliability <= 100


class TestTimeSeriesAggregations:
    """Testes para agregações de séries temporais"""
    
    def test_rolling_average_speed(self):
        """Testa cálculo de média móvel de velocidade"""
        timestamps = pd.date_range('2025-10-29 08:00:00', periods=20, freq='1min')
        data = {
            'timestamp': timestamps,
            'speed': np.random.uniform(30, 50, 20)
        }
        df = pd.DataFrame(data)
        
        # Calcular média móvel de 5 períodos
        df['rolling_avg_speed'] = df['speed'].rolling(window=5).mean()
        
        assert len(df) == 20
        assert df['rolling_avg_speed'].isna().sum() == 4  # Primeiros 4 são NaN
        assert all(df['rolling_avg_speed'].dropna() >= 30)
        assert all(df['rolling_avg_speed'].dropna() <= 50)
    
    def test_cumulative_distance(self):
        """Testa cálculo de distância acumulada"""
        data = {
            'timestamp': pd.date_range('2025-10-29 08:00:00', periods=10, freq='1min'),
            'distance_km': [0.5, 0.6, 0.4, 0.7, 0.5, 0.6, 0.5, 0.6, 0.7, 0.5]
        }
        df = pd.DataFrame(data)
        
        df['cumulative_distance'] = df['distance_km'].cumsum()
        
        assert df['cumulative_distance'].iloc[0] == 0.5
        assert df['cumulative_distance'].iloc[-1] == df['distance_km'].sum()
        assert all(df['cumulative_distance'].diff().dropna() >= 0)  # Sempre crescente
    
    def test_peak_hours_detection(self):
        """Testa detecção de horários de pico"""
        timestamps = pd.date_range('2025-10-29 00:00:00', periods=24, freq='H')
        data = {
            'hour': range(24),
            'num_trips': [10, 8, 5, 3, 5, 15, 35, 50, 45, 30, 25, 30, 35, 30, 28, 32, 40, 55, 50, 35, 25, 20, 15, 12]
        }
        df = pd.DataFrame(data)
        
        # Detectar picos (acima de 40 viagens)
        df['is_peak'] = df['num_trips'] > 40
        peak_hours = df[df['is_peak']]['hour'].tolist()
        
        assert len(peak_hours) > 0
        assert 7 in peak_hours or 8 in peak_hours  # Manhã
        assert 17 in peak_hours or 18 in peak_hours  # Tarde


class TestSparkAggregations:
    """Testes para agregações com PySpark"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Cria sessão Spark para testes"""
        spark = SparkSession.builder \
            .appName("test_aggregations") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_spark_groupby_count(self, spark):
        """Testa agregação com count no Spark"""
        schema = StructType([
            StructField("route_id", StringType(), True),
            StructField("vehicle_id", StringType(), True),
        ])
        
        data = [
            ("R1", "V1"),
            ("R1", "V2"),
            ("R1", "V3"),
            ("R2", "V4"),
            ("R2", "V5"),
        ]
        
        df = spark.createDataFrame(data, schema)
        result = df.groupBy("route_id").agg(count("vehicle_id").alias("vehicle_count"))
        
        result_dict = {row.route_id: row.vehicle_count for row in result.collect()}
        
        assert result_dict["R1"] == 3
        assert result_dict["R2"] == 2
    
    def test_spark_multiple_aggregations(self, spark):
        """Testa múltiplas agregações simultâneas"""
        schema = StructType([
            StructField("route_id", StringType(), True),
            StructField("speed", DoubleType(), True),
        ])
        
        data = [
            ("R1", 40.0),
            ("R1", 45.0),
            ("R1", 35.0),
            ("R2", 30.0),
            ("R2", 32.0),
        ]
        
        df = spark.createDataFrame(data, schema)
        result = df.groupBy("route_id").agg(
            count("*").alias("count"),
            avg("speed").alias("avg_speed"),
            spark_min("speed").alias("min_speed"),
            spark_max("speed").alias("max_speed")
        )
        
        r1_data = result.filter(col("route_id") == "R1").collect()[0]
        
        assert r1_data["count"] == 3
        assert r1_data["avg_speed"] == 40.0
        assert r1_data["min_speed"] == 35.0
        assert r1_data["max_speed"] == 45.0
    
    def test_spark_window_function(self, spark):
        """Testa uso de window functions"""
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        schema = StructType([
            StructField("route_id", StringType(), True),
            StructField("vehicle_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ])
        
        data = [
            ("R1", "V1", datetime(2025, 10, 29, 8, 0, 0)),
            ("R1", "V2", datetime(2025, 10, 29, 8, 10, 0)),
            ("R1", "V3", datetime(2025, 10, 29, 8, 20, 0)),
        ]
        
        df = spark.createDataFrame(data, schema)
        
        window_spec = Window.partitionBy("route_id").orderBy("timestamp")
        result = df.withColumn("row_num", row_number().over(window_spec))
        
        assert result.count() == 3
        assert all(row.row_num in [1, 2, 3] for row in result.collect())


class TestAdvancedAggregations:
    """Testes para agregações avançadas"""
    
    def test_percentile_calculation(self):
        """Testa cálculo de percentis"""
        data = {
            'speed': np.random.uniform(20, 80, 100)
        }
        df = pd.DataFrame(data)
        
        percentiles = df['speed'].quantile([0.25, 0.50, 0.75, 0.95])
        
        assert len(percentiles) == 4
        assert percentiles[0.25] < percentiles[0.50] < percentiles[0.75] < percentiles[0.95]
    
    def test_weighted_average(self):
        """Testa cálculo de média ponderada"""
        data = {
            'route_id': ['R1', 'R2', 'R3'],
            'avg_speed': [40, 35, 45],
            'num_vehicles': [10, 5, 15]
        }
        df = pd.DataFrame(data)
        
        # Média ponderada pelo número de veículos
        weighted_avg = (df['avg_speed'] * df['num_vehicles']).sum() / df['num_vehicles'].sum()
        
        assert weighted_avg > 0
        assert 35 < weighted_avg < 45  # Entre os valores min e max
    
    def test_cohort_analysis(self):
        """Testa análise de coorte (por período)"""
        dates = pd.date_range('2025-10-01', periods=30, freq='D')
        data = {
            'date': dates,
            'new_users': np.random.randint(50, 150, 30)
        }
        df = pd.DataFrame(data)
        df['week'] = df['date'].dt.isocalendar().week
        
        # Agregar por semana
        weekly = df.groupby('week')['new_users'].sum()
        
        assert len(weekly) > 0
        assert all(weekly > 0)
    
    def test_moving_aggregations(self):
        """Testa agregações móveis (sum, count)"""
        data = {
            'timestamp': pd.date_range('2025-10-29 08:00:00', periods=20, freq='1min'),
            'passengers': np.random.randint(20, 60, 20)
        }
        df = pd.DataFrame(data)
        
        # Soma móvel de 5 períodos
        df['rolling_sum'] = df['passengers'].rolling(window=5).sum()
        
        # Contagem móvel
        df['rolling_count'] = df['passengers'].rolling(window=5).count()
        
        assert len(df) == 20
        assert df['rolling_sum'].notna().sum() == 16  # 20 - 4 inicial
        assert all(df['rolling_count'].dropna() == 5)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
