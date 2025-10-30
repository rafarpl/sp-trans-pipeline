"""
Testes de Integração: Bronze to Silver
Projeto: SPTrans Real-Time Data Pipeline
Autor: Equipe LABDATA/FIA

Testa a transformação de dados da camada Bronze para Silver:
- Limpeza e validação de dados
- Deduplicação
- Enriquecimento
- Qualidade de dados
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, BooleanType
)
from pyspark.sql.functions import col, count, isnan, when
import tempfile
import shutil
from pathlib import Path


@pytest.fixture(scope="module")
def spark_session():
    """Cria uma sessão Spark para testes de integração"""
    spark = SparkSession.builder \
        .appName("test_bronze_to_silver") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="module")
def temp_data_dir():
    """Cria diretório temporário para dados de teste"""
    temp_dir = tempfile.mkdtemp()
    bronze_dir = Path(temp_dir) / "bronze"
    silver_dir = Path(temp_dir) / "silver"
    
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    yield {
        "root": temp_dir,
        "bronze": str(bronze_dir),
        "silver": str(silver_dir)
    }
    
    # Cleanup
    shutil.rmtree(temp_dir)


class TestBronzeToSilverPositions:
    """Testes para transformação de dados de posições de ônibus"""
    
    def test_load_bronze_positions(self, spark_session, temp_data_dir):
        """Testa carregamento de dados da camada Bronze"""
        # Criar dados de teste na camada Bronze
        schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("route_code", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("accessible", BooleanType(), True),
        ])
        
        test_data = [
            ("V001", "8000-10", datetime(2025, 10, 29, 8, 0, 0), -23.5505, -46.6333, True),
            ("V002", "8000-20", datetime(2025, 10, 29, 8, 2, 0), -23.5613, -46.6563, False),
            ("V003", "8100-15", datetime(2025, 10, 29, 8, 4, 0), -23.5447, -46.6392, True),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Salvar na camada Bronze
        bronze_path = f"{temp_data_dir['bronze']}/positions"
        df.write.mode("overwrite").parquet(bronze_path)
        
        # Carregar e verificar
        loaded_df = spark_session.read.parquet(bronze_path)
        
        assert loaded_df.count() == 3
        assert len(loaded_df.columns) == 6
        assert "vehicle_id" in loaded_df.columns
    
    def test_clean_invalid_coordinates(self, spark_session, temp_data_dir):
        """Testa limpeza de coordenadas inválidas"""
        schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ])
        
        # Incluir coordenadas inválidas
        test_data = [
            ("V001", -23.5505, -46.6333),  # Válido
            ("V002", None, -46.6333),       # Lat nula
            ("V003", -23.5505, None),       # Lon nula
            ("V004", 0.0, 0.0),             # Coordenada zero
            ("V005", -25.5505, -46.6333),   # Fora de SP
            ("V006", -23.5505, -46.6333),   # Válido
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Aplicar filtros de limpeza
        SP_LAT_MIN, SP_LAT_MAX = -24.0, -23.0
        SP_LON_MIN, SP_LON_MAX = -47.0, -46.0
        
        cleaned_df = df.filter(
            (col("latitude").isNotNull()) &
            (col("longitude").isNotNull()) &
            (col("latitude") != 0) &
            (col("longitude") != 0) &
            (col("latitude") >= SP_LAT_MIN) &
            (col("latitude") <= SP_LAT_MAX) &
            (col("longitude") >= SP_LON_MIN) &
            (col("longitude") <= SP_LON_MAX)
        )
        
        assert cleaned_df.count() == 2
        assert all(row.latitude is not None for row in cleaned_df.collect())
    
    def test_deduplicate_records(self, spark_session):
        """Testa deduplicação de registros"""
        schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("latitude", DoubleType(), True),
        ])
        
        timestamp = datetime(2025, 10, 29, 8, 0, 0)
        
        # Criar dados com duplicatas
        test_data = [
            ("V001", timestamp, -23.5505),
            ("V001", timestamp, -23.5505),  # Duplicata exata
            ("V001", timestamp, -23.5506),  # Duplicata com coord diferente
            ("V002", timestamp, -23.5613),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Deduplicar por vehicle_id e timestamp (mantendo primeiro)
        deduped_df = df.dropDuplicates(["vehicle_id", "timestamp"])
        
        assert deduped_df.count() == 2
        assert deduped_df.filter(col("vehicle_id") == "V001").count() == 1
    
    def test_add_quality_flags(self, spark_session):
        """Testa adição de flags de qualidade"""
        schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed", DoubleType(), True),
        ])
        
        test_data = [
            ("V001", -23.5505, -46.6333, 40.0),   # OK
            ("V002", -23.5613, -46.6563, 150.0),  # Speed suspeita
            ("V003", -23.5447, -46.6392, -10.0),  # Speed negativa
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Adicionar flags de qualidade
        MAX_SPEED = 120
        
        df_with_flags = df.withColumn(
            "quality_speed",
            when((col("speed") >= 0) & (col("speed") <= MAX_SPEED), "VALID")
            .otherwise("INVALID")
        )
        
        valid_count = df_with_flags.filter(col("quality_speed") == "VALID").count()
        assert valid_count == 1
    
    def test_bronze_to_silver_transformation(self, spark_session, temp_data_dir):
        """Testa transformação completa Bronze → Silver"""
        # 1. Criar dados Bronze
        bronze_schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("route_code", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed", DoubleType(), True),
        ])
        
        bronze_data = [
            ("V001", "8000-10", datetime(2025, 10, 29, 8, 0, 0), -23.5505, -46.6333, 40.0),
            ("V001", "8000-10", datetime(2025, 10, 29, 8, 0, 0), -23.5505, -46.6333, 40.0),  # Dup
            ("V002", "8000-20", datetime(2025, 10, 29, 8, 2, 0), None, -46.6563, 35.0),      # Null
            ("V003", "8100-15", datetime(2025, 10, 29, 8, 4, 0), -23.5447, -46.6392, 42.0),  # OK
            ("V004", "8200-30", datetime(2025, 10, 29, 8, 6, 0), -23.5550, -46.6400, 150.0), # Speed inv
        ]
        
        bronze_df = spark_session.createDataFrame(bronze_data, bronze_schema)
        
        # 2. Aplicar transformações Silver
        silver_df = bronze_df \
            .filter(col("latitude").isNotNull()) \
            .filter(col("longitude").isNotNull()) \
            .filter((col("latitude") >= -24.0) & (col("latitude") <= -23.0)) \
            .filter((col("longitude") >= -47.0) & (col("longitude") <= -46.0)) \
            .dropDuplicates(["vehicle_id", "timestamp"]) \
            .filter((col("speed") >= 0) & (col("speed") <= 120))
        
        # Adicionar metadados
        silver_df = silver_df.withColumn("processed_at", col("timestamp"))
        
        # 3. Salvar na camada Silver
        silver_path = f"{temp_data_dir['silver']}/positions_cleaned"
        silver_df.write.mode("overwrite").parquet(silver_path)
        
        # 4. Validar resultado
        result_df = spark_session.read.parquet(silver_path)
        
        assert result_df.count() == 2  # Apenas V001 e V003 são válidos
        assert all(row.latitude is not None for row in result_df.collect())
        assert all(0 <= row.speed <= 120 for row in result_df.collect())


class TestBronzeToSilverGTFS:
    """Testes para transformação de dados GTFS"""
    
    def test_clean_gtfs_routes(self, spark_session):
        """Testa limpeza de dados de rotas GTFS"""
        schema = StructType([
            StructField("route_id", StringType(), True),
            StructField("route_short_name", StringType(), True),
            StructField("route_long_name", StringType(), True),
            StructField("route_type", IntegerType(), True),
        ])
        
        test_data = [
            ("R001", "8000-10", "Terminal A - Terminal B", 3),
            ("R002", " 8000-20 ", "Terminal C - Terminal D ", 3),  # Com espaços
            ("R003", "", "Terminal E - Terminal F", 3),            # Nome vazio
            ("R004", "8100-15", "Terminal G - Terminal H", 3),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Limpeza
        from pyspark.sql.functions import trim, upper
        
        cleaned_df = df \
            .withColumn("route_short_name", trim(col("route_short_name"))) \
            .withColumn("route_short_name", upper(col("route_short_name"))) \
            .withColumn("route_long_name", trim(col("route_long_name"))) \
            .filter(col("route_short_name") != "")
        
        assert cleaned_df.count() == 3
        assert cleaned_df.filter(col("route_short_name") == "8000-20").count() == 1
    
    def test_validate_gtfs_stops(self, spark_session):
        """Testa validação de paradas GTFS"""
        schema = StructType([
            StructField("stop_id", StringType(), True),
            StructField("stop_name", StringType(), True),
            StructField("stop_lat", DoubleType(), True),
            StructField("stop_lon", DoubleType(), True),
        ])
        
        test_data = [
            ("S001", "Parada 1", -23.5505, -46.6333),
            ("S002", "Parada 2", None, -46.6563),      # Lat inválida
            ("S003", "Parada 3", -23.5447, -46.6392),
            ("S004", "", -23.5550, -46.6400),          # Nome vazio
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Validação
        valid_df = df \
            .filter(col("stop_lat").isNotNull()) \
            .filter(col("stop_lon").isNotNull()) \
            .filter(col("stop_name") != "")
        
        assert valid_df.count() == 2


class TestDataQualityMetrics:
    """Testes para métricas de qualidade de dados"""
    
    def test_calculate_completeness(self, spark_session):
        """Testa cálculo de completude dos dados"""
        schema = StructType([
            StructField("col1", StringType(), True),
            StructField("col2", IntegerType(), True),
            StructField("col3", DoubleType(), True),
        ])
        
        test_data = [
            ("A", 1, 1.0),
            ("B", None, 2.0),
            ("C", 3, None),
            (None, 4, 4.0),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular completude
        total_rows = df.count()
        
        completeness = {}
        for col_name in df.columns:
            non_null_count = df.filter(col(col_name).isNotNull()).count()
            completeness[col_name] = (non_null_count / total_rows) * 100
        
        assert completeness["col1"] == 75.0
        assert completeness["col2"] == 75.0
        assert completeness["col3"] == 75.0
    
    def test_calculate_validity(self, spark_session):
        """Testa cálculo de validade dos dados"""
        schema = StructType([
            StructField("speed", DoubleType(), True),
        ])
        
        test_data = [(40.0,), (120.0,), (150.0,), (-10.0,), (60.0,)]
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular validade (0-120 km/h)
        valid_count = df.filter((col("speed") >= 0) & (col("speed") <= 120)).count()
        validity_rate = (valid_count / df.count()) * 100
        
        assert validity_rate == 60.0  # 3 de 5 são válidos
    
    def test_calculate_uniqueness(self, spark_session):
        """Testa cálculo de unicidade"""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", IntegerType(), True),
        ])
        
        test_data = [
            ("A", 1),
            ("A", 1),  # Duplicata
            ("B", 2),
            ("C", 3),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular unicidade
        total_count = df.count()
        unique_count = df.dropDuplicates(["id"]).count()
        uniqueness_rate = (unique_count / total_count) * 100
        
        assert uniqueness_rate == 75.0  # 3 únicos de 4 totais


class TestSilverLayerValidation:
    """Testes para validação da camada Silver"""
    
    def test_silver_schema_validation(self, spark_session, temp_data_dir):
        """Testa validação do schema da camada Silver"""
        expected_schema = StructType([
            StructField("vehicle_id", StringType(), False),  # NOT NULL
            StructField("route_code", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
        ])
        
        test_data = [
            ("V001", "8000-10", datetime(2025, 10, 29, 8, 0, 0), -23.5505, -46.6333),
            ("V002", "8000-20", datetime(2025, 10, 29, 8, 2, 0), -23.5613, -46.6563),
        ]
        
        df = spark_session.createDataFrame(test_data, expected_schema)
        
        # Salvar
        silver_path = f"{temp_data_dir['silver']}/test_schema"
        df.write.mode("overwrite").parquet(silver_path)
        
        # Carregar e validar schema
        loaded_df = spark_session.read.parquet(silver_path)
        
        assert loaded_df.schema == expected_schema
        assert all(not field.nullable for field in loaded_df.schema.fields)
    
    def test_silver_data_constraints(self, spark_session):
        """Testa constraints de dados na camada Silver"""
        schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("speed", DoubleType(), False),
            StructField("latitude", DoubleType(), False),
        ])
        
        test_data = [
            ("V001", 40.0, -23.5505),
            ("V002", 60.0, -23.5613),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Validar constraints
        assert df.filter(col("speed") < 0).count() == 0
        assert df.filter(col("speed") > 120).count() == 0
        assert df.filter(col("latitude") < -24.0).count() == 0
        assert df.filter(col("latitude") > -23.0).count() == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
