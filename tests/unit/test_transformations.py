"""
Testes Unitários: Transformações de Dados
Projeto: SPTrans Real-Time Data Pipeline
Autor: Equipe LABDATA/FIA

Testa as funções de transformação de dados do pipeline:
- Limpeza de dados
- Validação de coordenadas
- Normalização de campos
- Enriquecimento de dados
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Assumindo que os módulos de transformação existem
# from src.processing.transformations.data_quality import DataQualityChecker
# from src.processing.transformations.enrichment import enrich_with_weather, enrich_with_address
# from src.processing.transformations.deduplication import remove_duplicates


class TestDataCleaning:
    """Testes para funções de limpeza de dados"""
    
    def test_remove_null_coordinates(self):
        """Testa remoção de registros com coordenadas nulas"""
        # Dados de teste
        data = [
            {"vehicle_id": "V1", "lat": -23.5505, "lon": -46.6333},
            {"vehicle_id": "V2", "lat": None, "lon": -46.6333},
            {"vehicle_id": "V3", "lat": -23.5505, "lon": None},
            {"vehicle_id": "V4", "lat": -23.5505, "lon": -46.6333},
        ]
        df = pd.DataFrame(data)
        
        # Remover nulos
        cleaned = df.dropna(subset=['lat', 'lon'])
        
        assert len(cleaned) == 2
        assert "V1" in cleaned['vehicle_id'].values
        assert "V4" in cleaned['vehicle_id'].values
        assert "V2" not in cleaned['vehicle_id'].values
    
    def test_validate_coordinate_ranges(self):
        """Testa validação de ranges de coordenadas"""
        # Coordenadas válidas para São Paulo
        SP_LAT_MIN, SP_LAT_MAX = -24.0, -23.0
        SP_LON_MIN, SP_LON_MAX = -47.0, -46.0
        
        data = [
            {"vehicle_id": "V1", "lat": -23.5505, "lon": -46.6333},  # Válido
            {"vehicle_id": "V2", "lat": -25.5505, "lon": -46.6333},  # Lat inválida
            {"vehicle_id": "V3", "lat": -23.5505, "lon": -50.6333},  # Lon inválida
            {"vehicle_id": "V4", "lat": 0, "lon": 0},                # Inválido
        ]
        df = pd.DataFrame(data)
        
        # Filtrar coordenadas válidas
        valid = df[
            (df['lat'] >= SP_LAT_MIN) & (df['lat'] <= SP_LAT_MAX) &
            (df['lon'] >= SP_LON_MIN) & (df['lon'] <= SP_LON_MAX)
        ]
        
        assert len(valid) == 1
        assert valid.iloc[0]['vehicle_id'] == "V1"
    
    def test_remove_duplicate_timestamps(self):
        """Testa remoção de timestamps duplicados"""
        timestamp = datetime.now()
        
        data = [
            {"vehicle_id": "V1", "timestamp": timestamp, "lat": -23.5505},
            {"vehicle_id": "V1", "timestamp": timestamp, "lat": -23.5506},  # Duplicado
            {"vehicle_id": "V1", "timestamp": timestamp + timedelta(minutes=1), "lat": -23.5507},
        ]
        df = pd.DataFrame(data)
        
        # Remover duplicatas mantendo o primeiro
        deduped = df.drop_duplicates(subset=['vehicle_id', 'timestamp'], keep='first')
        
        assert len(deduped) == 2
        assert deduped.iloc[0]['lat'] == -23.5505  # Primeiro registro mantido


class TestDataValidation:
    """Testes para validação de dados"""
    
    def test_validate_vehicle_id_format(self):
        """Testa validação do formato de ID de veículo"""
        import re
        
        valid_ids = ["BUS-001", "BUS-1234", "V12345"]
        invalid_ids = ["", None, "BUS", "12345ABC!@#"]
        
        # Pattern simples: letras, números e hífen
        pattern = re.compile(r'^[A-Z0-9\-]+$')
        
        for vid in valid_ids:
            assert pattern.match(vid) is not None, f"{vid} deveria ser válido"
        
        for vid in invalid_ids:
            if vid:
                assert pattern.match(vid) is None or len(vid) == 0, f"{vid} deveria ser inválido"
    
    def test_validate_timestamp_format(self):
        """Testa validação de formato de timestamp"""
        valid_timestamps = [
            "2025-10-29 14:30:00",
            "2025-01-01 00:00:00",
        ]
        
        for ts in valid_timestamps:
            try:
                parsed = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                assert isinstance(parsed, datetime)
            except ValueError:
                pytest.fail(f"Timestamp válido falhou: {ts}")
    
    def test_validate_speed_limits(self):
        """Testa validação de limites de velocidade"""
        MAX_SPEED_KMH = 120  # Velocidade máxima razoável para ônibus
        
        data = [
            {"vehicle_id": "V1", "speed": 40},   # Normal
            {"vehicle_id": "V2", "speed": 0},    # Parado
            {"vehicle_id": "V3", "speed": 150},  # Muito rápido (erro)
            {"vehicle_id": "V4", "speed": -10},  # Negativo (erro)
        ]
        df = pd.DataFrame(data)
        
        valid = df[(df['speed'] >= 0) & (df['speed'] <= MAX_SPEED_KMH)]
        
        assert len(valid) == 2
        assert all(valid['speed'] >= 0)
        assert all(valid['speed'] <= MAX_SPEED_KMH)


class TestDataNormalization:
    """Testes para normalização de dados"""
    
    def test_normalize_route_codes(self):
        """Testa normalização de códigos de linhas"""
        data = [
            {"route_code": "8000-10"},
            {"route_code": " 8000-10 "},  # Com espaços
            {"route_code": "8000-10"},
            {"route_code": "8000-10\n"},  # Com newline
        ]
        df = pd.DataFrame(data)
        
        # Normalizar
        df['route_code'] = df['route_code'].str.strip().str.upper()
        
        assert df['route_code'].nunique() == 1
        assert all(df['route_code'] == "8000-10")
    
    def test_normalize_boolean_fields(self):
        """Testa normalização de campos booleanos"""
        data = [
            {"accessible": "true"},
            {"accessible": "True"},
            {"accessible": "1"},
            {"accessible": "yes"},
            {"accessible": "false"},
            {"accessible": "False"},
            {"accessible": "0"},
            {"accessible": "no"},
        ]
        df = pd.DataFrame(data)
        
        # Normalizar para booleano
        true_values = ['true', '1', 'yes']
        df['accessible'] = df['accessible'].str.lower().isin(true_values)
        
        assert df['accessible'].dtype == bool
        assert df['accessible'].sum() == 4  # 4 verdadeiros
    
    def test_standardize_timestamps(self):
        """Testa padronização de timestamps"""
        data = [
            {"timestamp": "2025-10-29 14:30:00"},
            {"timestamp": "2025-10-29T14:30:00"},
            {"timestamp": "29/10/2025 14:30:00"},
        ]
        df = pd.DataFrame(data)
        
        # Converter para datetime
        def parse_timestamp(ts):
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%d/%m/%Y %H:%M:%S"
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(ts, fmt)
                except ValueError:
                    continue
            return None
        
        df['timestamp'] = df['timestamp'].apply(parse_timestamp)
        
        assert all(df['timestamp'].notna())
        assert all(isinstance(ts, datetime) for ts in df['timestamp'])


class TestDataEnrichment:
    """Testes para enriquecimento de dados"""
    
    def test_calculate_distance_between_points(self):
        """Testa cálculo de distância entre dois pontos"""
        from math import radians, sin, cos, sqrt, atan2
        
        def haversine_distance(lat1, lon1, lat2, lon2):
            """Calcula distância em km usando fórmula de Haversine"""
            R = 6371  # Raio da Terra em km
            
            lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            
            return R * c
        
        # Teste: Praça da Sé para Av. Paulista (aprox 2.5 km)
        dist = haversine_distance(-23.5505, -46.6333, -23.5613, -46.6563)
        
        assert 2.0 < dist < 3.5  # Aproximadamente 2.5 km
    
    def test_calculate_speed(self):
        """Testa cálculo de velocidade entre pontos"""
        # Dois pontos com distância conhecida e tempo conhecido
        lat1, lon1 = -23.5505, -46.6333
        lat2, lon2 = -23.5613, -46.6563
        
        time1 = datetime(2025, 10, 29, 14, 0, 0)
        time2 = datetime(2025, 10, 29, 14, 5, 0)  # 5 minutos depois
        
        from math import radians, sin, cos, sqrt, atan2
        
        def calculate_speed(lat1, lon1, time1, lat2, lon2, time2):
            """Calcula velocidade em km/h"""
            R = 6371
            lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            distance_km = R * c
            
            time_diff_hours = (time2 - time1).total_seconds() / 3600
            return distance_km / time_diff_hours if time_diff_hours > 0 else 0
        
        speed = calculate_speed(lat1, lon1, time1, lat2, lon2, time2)
        
        assert speed > 0
        assert speed < 100  # Velocidade razoável para ônibus urbano
    
    def test_add_time_features(self):
        """Testa adição de features temporais"""
        timestamps = pd.date_range('2025-10-29 00:00:00', periods=24, freq='H')
        df = pd.DataFrame({'timestamp': timestamps})
        
        # Adicionar features
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        df['is_rush_hour'] = df['hour'].isin([7, 8, 9, 17, 18, 19])
        
        assert len(df) == 24
        assert df['hour'].min() == 0
        assert df['hour'].max() == 23
        assert 'is_rush_hour' in df.columns
        assert 'is_weekend' in df.columns


class TestSparkTransformations:
    """Testes para transformações com PySpark"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Cria sessão Spark para testes"""
        spark = SparkSession.builder \
            .appName("test_transformations") \
            .master("local[2]") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_spark_filter_invalid_coordinates(self, spark):
        """Testa filtro de coordenadas inválidas com Spark"""
        schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True)
        ])
        
        data = [
            ("V1", -23.5505, -46.6333),
            ("V2", None, -46.6333),
            ("V3", -23.5505, None),
            ("V4", -23.5505, -46.6333),
        ]
        
        df = spark.createDataFrame(data, schema)
        filtered = df.filter("lat IS NOT NULL AND lon IS NOT NULL")
        
        assert filtered.count() == 2
    
    def test_spark_add_calculated_columns(self, spark):
        """Testa adição de colunas calculadas com Spark"""
        from pyspark.sql.functions import col, round as spark_round
        
        schema = StructType([
            StructField("lat1", DoubleType(), True),
            StructField("lon1", DoubleType(), True),
            StructField("lat2", DoubleType(), True),
            StructField("lon2", DoubleType(), True),
        ])
        
        data = [(-23.5505, -46.6333, -23.5613, -46.6563)]
        df = spark.createDataFrame(data, schema)
        
        # Adicionar coluna calculada simples
        result = df.withColumn("lat_diff", spark_round(col("lat2") - col("lat1"), 4))
        
        assert result.count() == 1
        assert "lat_diff" in result.columns
    
    def test_spark_groupby_aggregation(self, spark):
        """Testa agregação com groupBy"""
        from pyspark.sql.functions import count, avg
        
        schema = StructType([
            StructField("route_id", StringType(), True),
            StructField("speed", DoubleType(), True),
        ])
        
        data = [
            ("8000-10", 40.0),
            ("8000-10", 45.0),
            ("8000-20", 35.0),
            ("8000-20", 38.0),
        ]
        
        df = spark.createDataFrame(data, schema)
        result = df.groupBy("route_id").agg(
            count("*").alias("num_records"),
            avg("speed").alias("avg_speed")
        )
        
        assert result.count() == 2
        result_list = result.collect()
        assert all(row.num_records == 2 for row in result_list)


class TestDataQualityChecks:
    """Testes para verificações de qualidade de dados"""
    
    def test_check_completeness(self):
        """Testa verificação de completude dos dados"""
        data = [
            {"vehicle_id": "V1", "lat": -23.5505, "lon": -46.6333, "route": "8000-10"},
            {"vehicle_id": "V2", "lat": None, "lon": -46.6333, "route": "8000-20"},
            {"vehicle_id": "V3", "lat": -23.5505, "lon": -46.6333, "route": None},
        ]
        df = pd.DataFrame(data)
        
        # Calcular completude por coluna
        completeness = {}
        for col in df.columns:
            completeness[col] = (df[col].notna().sum() / len(df)) * 100
        
        assert completeness['vehicle_id'] == 100.0
        assert completeness['lat'] == pytest.approx(66.67, 0.1)
        assert completeness['route'] == pytest.approx(66.67, 0.1)
    
    def test_check_uniqueness(self):
        """Testa verificação de unicidade"""
        data = [
            {"vehicle_id": "V1", "timestamp": "2025-10-29 14:00:00"},
            {"vehicle_id": "V1", "timestamp": "2025-10-29 14:00:00"},  # Duplicado
            {"vehicle_id": "V2", "timestamp": "2025-10-29 14:00:00"},
        ]
        df = pd.DataFrame(data)
        
        duplicates = df.duplicated(subset=['vehicle_id', 'timestamp']).sum()
        uniqueness_rate = ((len(df) - duplicates) / len(df)) * 100
        
        assert duplicates == 1
        assert uniqueness_rate == pytest.approx(66.67, 0.1)
    
    def test_check_consistency(self):
        """Testa verificação de consistência (ex: speed vs distance)"""
        # Speed deveria ser consistente com distância e tempo
        data = [
            {"distance_km": 10, "time_hours": 0.25, "speed_kmh": 40},  # Consistente
            {"distance_km": 10, "time_hours": 0.25, "speed_kmh": 100}, # Inconsistente
        ]
        df = pd.DataFrame(data)
        
        # Calcular speed esperado
        df['expected_speed'] = df['distance_km'] / df['time_hours']
        df['is_consistent'] = abs(df['speed_kmh'] - df['expected_speed']) < 1.0
        
        assert df.iloc[0]['is_consistent'] == True
        assert df.iloc[1]['is_consistent'] == False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
