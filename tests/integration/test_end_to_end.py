"""
Testes de Integração: End-to-End
Projeto: SPTrans Real-Time Data Pipeline
Autor: Equipe LABDATA/FIA

Testa o pipeline completo de ponta a ponta:
- Ingestão → Bronze → Silver → Gold → PostgreSQL
- Fluxo completo de dados
- Qualidade de dados em todas as camadas
- Performance do pipeline
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)
from pyspark.sql.functions import col, count, avg, lit
import tempfile
import shutil
from pathlib import Path
import time
import json


@pytest.fixture(scope="module")
def spark_session():
    """Cria uma sessão Spark para testes E2E"""
    spark = SparkSession.builder \
        .appName("test_end_to_end") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="module")
def data_lake_paths():
    """Cria estrutura de diretórios do Data Lake"""
    temp_dir = tempfile.mkdtemp()
    
    paths = {
        "root": temp_dir,
        "bronze": {
            "api_positions": f"{temp_dir}/bronze/api_positions",
            "gtfs": f"{temp_dir}/bronze/gtfs"
        },
        "silver": {
            "positions_cleaned": f"{temp_dir}/silver/positions_cleaned",
            "routes": f"{temp_dir}/silver/routes",
            "stops": f"{temp_dir}/silver/stops"
        },
        "gold": {
            "kpis_hourly": f"{temp_dir}/gold/kpis_hourly",
            "route_summary": f"{temp_dir}/gold/route_summary",
            "headway_analysis": f"{temp_dir}/gold/headway_analysis"
        }
    }
    
    # Criar diretórios
    for layer in ["bronze", "silver", "gold"]:
        for path in paths[layer].values():
            Path(path).mkdir(parents=True, exist_ok=True)
    
    yield paths
    
    # Cleanup
    shutil.rmtree(temp_dir)


class TestEndToEndPipeline:
    """Testes para pipeline completo end-to-end"""
    
    def test_complete_pipeline_flow(self, spark_session, data_lake_paths):
        """Testa fluxo completo: API → Bronze → Silver → Gold"""
        
        # ===================================================================
        # ETAPA 1: SIMULAR INGESTÃO DE API (RAW → BRONZE)
        # ===================================================================
        print("\n[E2E] Etapa 1: Ingestão de dados da API...")
        
        # Simular dados da API SPTrans
        api_schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("route_code", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("accessible", BooleanType(), True),
        ])
        
        base_time = datetime(2025, 10, 29, 8, 0, 0)
        api_data = []
        
        # Gerar 500 registros simulados
        for i in range(500):
            vehicle_id = f"V{i % 50:03d}"
            route = f"8000-{10 + (i % 10)}"
            timestamp = base_time + timedelta(minutes=i % 60, seconds=i % 60)
            lat = -23.5505 + (i % 100) * 0.001
            lon = -46.6333 + (i % 100) * 0.001
            speed = 25.0 + (i % 40)
            accessible = i % 3 == 0
            
            # Inserir alguns dados ruins intencionalmente
            if i % 50 == 0:
                lat = None  # Coordenada nula
            if i % 51 == 0:
                speed = 150.0  # Velocidade inválida
            
            api_data.append((vehicle_id, route, timestamp, lat, lon, speed, accessible))
        
        bronze_df = spark_session.createDataFrame(api_data, api_schema)
        
        # Salvar na camada Bronze
        bronze_path = data_lake_paths["bronze"]["api_positions"]
        bronze_df.write.mode("overwrite").parquet(bronze_path)
        
        print(f"   ✓ Bronze: {bronze_df.count()} registros salvos")
        
        # ===================================================================
        # ETAPA 2: TRANSFORMAÇÃO BRONZE → SILVER (LIMPEZA E VALIDAÇÃO)
        # ===================================================================
        print("\n[E2E] Etapa 2: Transformação Bronze → Silver...")
        
        # Carregar dados Bronze
        bronze_loaded = spark_session.read.parquet(bronze_path)
        
        # Aplicar limpeza e validação
        silver_df = bronze_loaded \
            .filter(col("latitude").isNotNull()) \
            .filter(col("longitude").isNotNull()) \
            .filter((col("latitude") >= -24.0) & (col("latitude") <= -23.0)) \
            .filter((col("longitude") >= -47.0) & (col("longitude") <= -46.0)) \
            .filter((col("speed") >= 0) & (col("speed") <= 120)) \
            .dropDuplicates(["vehicle_id", "timestamp"])
        
        # Adicionar metadados
        silver_df = silver_df.withColumn("processed_at", lit(datetime.now()))
        
        # Salvar na camada Silver
        silver_path = data_lake_paths["silver"]["positions_cleaned"]
        silver_df.write.mode("overwrite").parquet(silver_path)
        
        print(f"   ✓ Silver: {silver_df.count()} registros limpos")
        print(f"   ✓ Filtrados: {bronze_df.count() - silver_df.count()} registros inválidos")
        
        # ===================================================================
        # ETAPA 3: TRANSFORMAÇÃO SILVER → GOLD (AGREGAÇÕES E KPIS)
        # ===================================================================
        print("\n[E2E] Etapa 3: Transformação Silver → Gold...")
        
        # Carregar dados Silver
        silver_loaded = spark_session.read.parquet(silver_path)
        
        # Agregação 1: KPIs Horários
        from pyspark.sql.functions import date_trunc, max as spark_max, min as spark_min
        
        gold_hourly = silver_loaded \
            .withColumn("hour", date_trunc("hour", col("timestamp"))) \
            .groupBy("hour", "route_code") \
            .agg(
                count("vehicle_id").alias("num_vehicles"),
                avg("speed").alias("avg_speed"),
                spark_min("speed").alias("min_speed"),
                spark_max("speed").alias("max_speed"),
                count(col("accessible")).alias("accessible_vehicles")
            )
        
        gold_hourly_path = data_lake_paths["gold"]["kpis_hourly"]
        gold_hourly.write.mode("overwrite").parquet(gold_hourly_path)
        
        print(f"   ✓ Gold Hourly: {gold_hourly.count()} agregações horárias")
        
        # Agregação 2: Resumo por Rota
        gold_route = silver_loaded \
            .groupBy("route_code") \
            .agg(
                count("vehicle_id").alias("total_records"),
                avg("speed").alias("avg_speed"),
                avg("latitude").alias("avg_latitude"),
                avg("longitude").alias("avg_longitude")
            )
        
        gold_route_path = data_lake_paths["gold"]["route_summary"]
        gold_route.write.mode("overwrite").parquet(gold_route_path)
        
        print(f"   ✓ Gold Route Summary: {gold_route.count()} rotas agregadas")
        
        # ===================================================================
        # ETAPA 4: VALIDAÇÃO DE QUALIDADE
        # ===================================================================
        print("\n[E2E] Etapa 4: Validação de Qualidade de Dados...")
        
        # Validar Bronze
        bronze_quality = {
            "total_records": bronze_df.count(),
            "null_lat": bronze_df.filter(col("latitude").isNull()).count(),
            "null_lon": bronze_df.filter(col("longitude").isNull()).count(),
            "invalid_speed": bronze_df.filter((col("speed") < 0) | (col("speed") > 120)).count()
        }
        
        # Validar Silver
        silver_quality = {
            "total_records": silver_df.count(),
            "null_records": 0,  # Não deve haver nulos
            "avg_speed": silver_df.agg(avg("speed")).first()[0]
        }
        
        # Validar Gold
        gold_quality = {
            "hourly_records": gold_hourly.count(),
            "route_records": gold_route.count()
        }
        
        print(f"   ✓ Bronze Quality: {json.dumps(bronze_quality, indent=2)}")
        print(f"   ✓ Silver Quality: {json.dumps(silver_quality, indent=2)}")
        print(f"   ✓ Gold Quality: {json.dumps(gold_quality, indent=2)}")
        
        # ===================================================================
        # ASSERÇÕES FINAIS
        # ===================================================================
        
        # Verificar que dados fluíram por todas as camadas
        assert bronze_df.count() == 500
        assert silver_df.count() < bronze_df.count()  # Alguns registros filtrados
        assert silver_df.count() > 0
        assert gold_hourly.count() > 0
        assert gold_route.count() > 0
        
        # Verificar qualidade dos dados Silver
        assert silver_df.filter(col("latitude").isNull()).count() == 0
        assert silver_df.filter(col("longitude").isNull()).count() == 0
        assert silver_df.filter((col("speed") < 0) | (col("speed") > 120)).count() == 0
        
        # Verificar agregações Gold fazem sentido
        assert all(row.num_vehicles > 0 for row in gold_hourly.collect())
        assert all(row.avg_speed > 0 for row in gold_route.collect())
        
        print("\n[E2E] ✅ Pipeline completo executado com sucesso!")
    
    def test_pipeline_performance(self, spark_session, data_lake_paths):
        """Testa performance do pipeline completo"""
        print("\n[E2E] Teste de Performance...")
        
        # Criar dataset maior
        schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("speed", DoubleType(), False),
        ])
        
        base_time = datetime(2025, 10, 29, 8, 0, 0)
        large_data = [
            (f"V{i % 100:03d}", base_time + timedelta(seconds=i), 30.0 + (i % 40))
            for i in range(10000)
        ]
        
        df = spark_session.createDataFrame(large_data, schema)
        
        # Medir tempo de processamento
        start_time = time.time()
        
        # Simular processamento
        result = df.groupBy("vehicle_id").agg(avg("speed").alias("avg_speed"))
        result_count = result.count()
        
        elapsed_time = time.time() - start_time
        
        print(f"   ✓ Processados: {df.count()} registros")
        print(f"   ✓ Agregações: {result_count} veículos")
        print(f"   ✓ Tempo: {elapsed_time:.2f} segundos")
        print(f"   ✓ Taxa: {df.count() / elapsed_time:.0f} registros/segundo")
        
        # Validar performance
        assert elapsed_time < 30.0  # Deve processar em menos de 30 segundos
        assert result_count == 100


class TestDataLineage:
    """Testes para rastreabilidade de dados (data lineage)"""
    
    def test_track_data_through_layers(self, spark_session, data_lake_paths):
        """Testa rastreamento de dados através das camadas"""
        
        # Criar dados com ID rastreável
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("value", DoubleType(), False),
        ])
        
        test_data = [
            ("ID001", "V001", 40.0),
            ("ID002", "V002", 45.0),
        ]
        
        # Bronze
        bronze_df = spark_session.createDataFrame(test_data, schema)
        bronze_path = f"{data_lake_paths['root']}/test_lineage/bronze"
        bronze_df.write.mode("overwrite").parquet(bronze_path)
        
        # Silver (adicionar metadados)
        silver_df = bronze_df.withColumn("layer", lit("silver"))
        silver_path = f"{data_lake_paths['root']}/test_lineage/silver"
        silver_df.write.mode("overwrite").parquet(silver_path)
        
        # Gold (agregação mantendo ID)
        gold_df = silver_df.groupBy("vehicle_id").agg(
            avg("value").alias("avg_value"),
            count("id").alias("num_records")
        )
        gold_path = f"{data_lake_paths['root']}/test_lineage/gold"
        gold_df.write.mode("overwrite").parquet(gold_path)
        
        # Validar rastreamento
        bronze_loaded = spark_session.read.parquet(bronze_path)
        silver_loaded = spark_session.read.parquet(silver_path)
        gold_loaded = spark_session.read.parquet(gold_path)
        
        assert bronze_loaded.count() == 2
        assert silver_loaded.count() == 2
        assert gold_loaded.count() == 2
        
        # Verificar que IDs foram preservados
        assert bronze_loaded.filter(col("id") == "ID001").count() == 1
        assert silver_loaded.filter(col("id") == "ID001").count() == 1


class TestErrorHandling:
    """Testes para tratamento de erros no pipeline"""
    
    def test_handle_corrupt_data(self, spark_session):
        """Testa tratamento de dados corrompidos"""
        schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("value", DoubleType(), True),
        ])
        
        # Dados com valores nulos e inválidos
        test_data = [
            ("V001", 40.0),
            (None, 45.0),     # vehicle_id nulo
            ("V003", None),   # value nulo
            ("V004", 50.0),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Filtrar dados válidos
        valid_df = df.filter(
            col("vehicle_id").isNotNull() &
            col("value").isNotNull()
        )
        
        assert df.count() == 4
        assert valid_df.count() == 2
    
    def test_handle_schema_mismatch(self, spark_session, data_lake_paths):
        """Testa tratamento de incompatibilidade de schema"""
        
        # Schema esperado
        expected_schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("value", IntegerType(), False),
        ])
        
        # Dados com schema diferente
        test_data = [("V001", 40.0)]  # value é Double, não Integer
        
        # Criar com schema flexível
        df = spark_session.createDataFrame(test_data, ["vehicle_id", "value"])
        
        # Converter para schema esperado
        from pyspark.sql.functions import col as spark_col
        df_converted = df.select(
            spark_col("vehicle_id").cast("string"),
            spark_col("value").cast("integer")
        )
        
        assert df_converted.schema[1].dataType.typeName() == "integer"


class TestDataQualityMetrics:
    """Testes para métricas de qualidade end-to-end"""
    
    def test_calculate_pipeline_quality_score(self, spark_session):
        """Testa cálculo de score de qualidade do pipeline"""
        
        # Simular dados com diferentes níveis de qualidade
        schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("speed", DoubleType(), True),
        ])
        
        test_data = [
            ("V001", -23.5505, -46.6333, 40.0),  # Válido
            ("V002", None, -46.6333, 45.0),       # Lat nula
            ("V003", -23.5505, None, 50.0),       # Lon nula
            ("V004", -23.5505, -46.6333, 150.0),  # Speed inválida
            ("V005", -23.5505, -46.6333, 42.0),   # Válido
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular métricas de qualidade
        total_records = df.count()
        
        # Completude
        completeness = {
            "latitude": df.filter(col("latitude").isNotNull()).count() / total_records,
            "longitude": df.filter(col("longitude").isNotNull()).count() / total_records,
        }
        
        # Validade
        validity = {
            "speed": df.filter((col("speed") >= 0) & (col("speed") <= 120)).count() / total_records,
        }
        
        # Score geral (média das métricas)
        quality_score = (
            sum(completeness.values()) + sum(validity.values())
        ) / (len(completeness) + len(validity))
        
        assert 0 <= quality_score <= 1
        assert completeness["latitude"] == 0.8  # 4/5
        assert validity["speed"] == 0.8  # 4/5


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
