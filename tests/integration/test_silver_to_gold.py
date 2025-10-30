"""
Testes de Integração: Silver to Gold
Projeto: SPTrans Real-Time Data Pipeline
Autor: Equipe LABDATA/FIA

Testa a transformação de dados da camada Silver para Gold:
- Agregações
- Cálculo de KPIs
- Métricas de negócio
- Análises avançadas
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, min as spark_min,
    max as spark_max, stddev, hour, date_trunc, window
)
from pyspark.sql.window import Window
import tempfile
import shutil
from pathlib import Path


@pytest.fixture(scope="module")
def spark_session():
    """Cria uma sessão Spark para testes de integração"""
    spark = SparkSession.builder \
        .appName("test_silver_to_gold") \
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
    silver_dir = Path(temp_dir) / "silver"
    gold_dir = Path(temp_dir) / "gold"
    
    silver_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)
    
    yield {
        "root": temp_dir,
        "silver": str(silver_dir),
        "gold": str(gold_dir)
    }
    
    # Cleanup
    shutil.rmtree(temp_dir)


class TestHourlyAggregations:
    """Testes para agregações horárias"""
    
    def test_aggregate_vehicles_per_hour(self, spark_session):
        """Testa agregação de veículos por hora"""
        schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("route_code", StringType(), False),
        ])
        
        # Criar dados de 2 horas
        base_time = datetime(2025, 10, 29, 8, 0, 0)
        test_data = []
        
        for i in range(120):  # 120 minutos = 2 horas
            vehicle_id = f"V{i % 10:03d}"
            timestamp = base_time + timedelta(minutes=i)
            route = f"8000-{10 + (i % 3)}"
            test_data.append((vehicle_id, timestamp, route))
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Agregar por hora
        hourly_df = df.withColumn("hour", date_trunc("hour", col("timestamp"))) \
            .groupBy("hour") \
            .agg(
                count("vehicle_id").alias("total_records"),
                count("vehicle_id").alias("unique_vehicles")
            )
        
        assert hourly_df.count() == 2
        assert all(row.total_records > 0 for row in hourly_df.collect())
    
    def test_calculate_avg_speed_per_hour(self, spark_session):
        """Testa cálculo de velocidade média por hora"""
        schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("speed", DoubleType(), False),
        ])
        
        base_time = datetime(2025, 10, 29, 8, 0, 0)
        test_data = []
        
        for i in range(100):
            vehicle_id = f"V{i % 10:03d}"
            timestamp = base_time + timedelta(minutes=i)
            speed = 30.0 + (i % 30)  # Velocidades entre 30-60 km/h
            test_data.append((vehicle_id, timestamp, speed))
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular velocidade média por hora
        hourly_speed = df.withColumn("hour", date_trunc("hour", col("timestamp"))) \
            .groupBy("hour") \
            .agg(
                avg("speed").alias("avg_speed"),
                spark_min("speed").alias("min_speed"),
                spark_max("speed").alias("max_speed")
            )
        
        results = hourly_speed.collect()
        assert len(results) == 2
        assert all(30 <= row.avg_speed <= 60 for row in results)


class TestRouteAggregations:
    """Testes para agregações por rota"""
    
    def test_metrics_by_route(self, spark_session):
        """Testa cálculo de métricas por rota"""
        schema = StructType([
            StructField("route_code", StringType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("speed", DoubleType(), False),
        ])
        
        test_data = [
            ("8000-10", "V001", datetime(2025, 10, 29, 8, 0, 0), 40.0),
            ("8000-10", "V002", datetime(2025, 10, 29, 8, 2, 0), 42.0),
            ("8000-10", "V003", datetime(2025, 10, 29, 8, 4, 0), 38.0),
            ("8000-20", "V004", datetime(2025, 10, 29, 8, 1, 0), 35.0),
            ("8000-20", "V005", datetime(2025, 10, 29, 8, 3, 0), 36.0),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Agregar por rota
        route_metrics = df.groupBy("route_code").agg(
            count("vehicle_id").alias("num_vehicles"),
            avg("speed").alias("avg_speed"),
            stddev("speed").alias("stddev_speed")
        )
        
        route_8000_10 = route_metrics.filter(col("route_code") == "8000-10").first()
        
        assert route_8000_10.num_vehicles == 3
        assert route_8000_10.avg_speed == 40.0
    
    def test_headway_calculation(self, spark_session):
        """Testa cálculo de headway (intervalo entre ônibus)"""
        schema = StructType([
            StructField("route_code", StringType(), False),
            StructField("stop_id", StringType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
        ])
        
        # Simular passagens de ônibus na mesma parada
        test_data = [
            ("8000-10", "S001", "V001", datetime(2025, 10, 29, 8, 0, 0)),
            ("8000-10", "S001", "V002", datetime(2025, 10, 29, 8, 10, 0)),  # 10 min
            ("8000-10", "S001", "V003", datetime(2025, 10, 29, 8, 20, 0)),  # 10 min
            ("8000-20", "S001", "V004", datetime(2025, 10, 29, 8, 0, 0)),
            ("8000-20", "S001", "V005", datetime(2025, 10, 29, 8, 25, 0)),  # 25 min
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular headway usando window function
        from pyspark.sql.functions import lag, unix_timestamp
        
        window_spec = Window.partitionBy("route_code", "stop_id").orderBy("timestamp")
        
        df_with_headway = df.withColumn("prev_timestamp", lag("timestamp").over(window_spec))
        df_with_headway = df_with_headway.withColumn(
            "headway_minutes",
            (unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")) / 60
        )
        
        # Filtrar apenas registros com headway calculado
        headway_df = df_with_headway.filter(col("headway_minutes").isNotNull())
        
        assert headway_df.count() == 3
        assert all(row.headway_minutes > 0 for row in headway_df.collect())


class TestKPICalculations:
    """Testes para cálculo de KPIs"""
    
    def test_fleet_utilization_kpi(self, spark_session):
        """Testa cálculo de KPI de utilização da frota"""
        schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("timestamp", TimestampType(), False),
        ])
        
        timestamp = datetime(2025, 10, 29, 8, 0, 0)
        
        # 100 veículos, 85 ativos
        test_data = [(f"V{i:03d}", "ACTIVE" if i < 85 else "INACTIVE", timestamp) 
                     for i in range(100)]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular utilização
        total_vehicles = df.count()
        active_vehicles = df.filter(col("status") == "ACTIVE").count()
        
        utilization_rate = (active_vehicles / total_vehicles) * 100
        
        assert utilization_rate == 85.0
        assert 0 <= utilization_rate <= 100
    
    def test_on_time_performance_kpi(self, spark_session):
        """Testa cálculo de KPI de pontualidade"""
        schema = StructType([
            StructField("trip_id", StringType(), False),
            StructField("scheduled_time", TimestampType(), False),
            StructField("actual_time", TimestampType(), False),
        ])
        
        base_time = datetime(2025, 10, 29, 8, 0, 0)
        
        # 10 viagens com diferentes delays
        test_data = [
            (f"T{i:03d}", 
             base_time + timedelta(minutes=i*10),
             base_time + timedelta(minutes=i*10 + delay))
            for i, delay in enumerate([0, 2, -1, 5, 1, 0, 3, -2, 6, 1])
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular delay e on-time status
        from pyspark.sql.functions import unix_timestamp, abs as spark_abs, when
        
        df_with_delay = df.withColumn(
            "delay_minutes",
            (unix_timestamp("actual_time") - unix_timestamp("scheduled_time")) / 60
        )
        
        df_with_status = df_with_delay.withColumn(
            "is_on_time",
            when(spark_abs(col("delay_minutes")) <= 3, True).otherwise(False)
        )
        
        on_time_count = df_with_status.filter(col("is_on_time") == True).count()
        on_time_rate = (on_time_count / df_with_status.count()) * 100
        
        assert on_time_rate == 80.0
    
    def test_average_occupancy_kpi(self, spark_session):
        """Testa cálculo de KPI de ocupação média"""
        schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("passengers", IntegerType(), False),
            StructField("capacity", IntegerType(), False),
        ])
        
        test_data = [
            ("V001", 45, 60),
            ("V002", 50, 60),
            ("V003", 30, 60),
            ("V004", 40, 60),
            ("V005", 55, 60),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular ocupação
        df_with_occupancy = df.withColumn(
            "occupancy_rate",
            (col("passengers") / col("capacity")) * 100
        )
        
        avg_occupancy = df_with_occupancy.agg(avg("occupancy_rate")).first()[0]
        
        assert 0 <= avg_occupancy <= 100
        assert 73.0 <= avg_occupancy <= 74.0  # Aproximadamente 73.33%


class TestGoldLayerAggregations:
    """Testes para agregações da camada Gold"""
    
    def test_create_hourly_kpis_table(self, spark_session, temp_data_dir):
        """Testa criação de tabela de KPIs horários"""
        # Criar dados Silver simulados
        schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("route_code", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("speed", DoubleType(), False),
            StructField("passengers", IntegerType(), False),
        ])
        
        base_time = datetime(2025, 10, 29, 8, 0, 0)
        test_data = []
        
        for i in range(100):
            vehicle_id = f"V{i % 10:03d}"
            route = f"8000-{10 + (i % 3)}"
            timestamp = base_time + timedelta(minutes=i)
            speed = 30.0 + (i % 30)
            passengers = 20 + (i % 40)
            test_data.append((vehicle_id, route, timestamp, speed, passengers))
        
        silver_df = spark_session.createDataFrame(test_data, schema)
        
        # Criar agregação Gold
        gold_df = silver_df.withColumn("hour", date_trunc("hour", col("timestamp"))) \
            .groupBy("hour", "route_code") \
            .agg(
                count("vehicle_id").alias("num_vehicles"),
                avg("speed").alias("avg_speed"),
                avg("passengers").alias("avg_passengers"),
                spark_sum("passengers").alias("total_passengers")
            )
        
        # Salvar na camada Gold
        gold_path = f"{temp_data_dir['gold']}/kpis_hourly"
        gold_df.write.mode("overwrite").parquet(gold_path)
        
        # Validar
        result_df = spark_session.read.parquet(gold_path)
        
        assert result_df.count() > 0
        assert "hour" in result_df.columns
        assert "route_code" in result_df.columns
        assert "avg_speed" in result_df.columns
    
    def test_create_route_summary_table(self, spark_session, temp_data_dir):
        """Testa criação de tabela de resumo por rota"""
        schema = StructType([
            StructField("route_code", StringType(), False),
            StructField("vehicle_id", StringType(), False),
            StructField("trip_id", StringType(), False),
            StructField("distance_km", DoubleType(), False),
            StructField("duration_minutes", DoubleType(), False),
        ])
        
        test_data = [
            ("8000-10", "V001", "T001", 15.5, 45.0),
            ("8000-10", "V002", "T002", 15.8, 47.0),
            ("8000-10", "V003", "T003", 15.3, 44.0),
            ("8000-20", "V004", "T004", 12.0, 35.0),
            ("8000-20", "V005", "T005", 12.5, 36.0),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Criar resumo por rota
        route_summary = df.groupBy("route_code").agg(
            count("trip_id").alias("total_trips"),
            avg("distance_km").alias("avg_distance"),
            avg("duration_minutes").alias("avg_duration"),
            (avg("distance_km") / (avg("duration_minutes") / 60)).alias("avg_speed_kmh")
        )
        
        # Salvar
        gold_path = f"{temp_data_dir['gold']}/route_summary"
        route_summary.write.mode("overwrite").parquet(gold_path)
        
        # Validar
        result_df = spark_session.read.parquet(gold_path)
        
        assert result_df.count() == 2
        route_8000_10 = result_df.filter(col("route_code") == "8000-10").first()
        assert route_8000_10.total_trips == 3


class TestBusinessRules:
    """Testes para regras de negócio"""
    
    def test_peak_hour_identification(self, spark_session):
        """Testa identificação de horários de pico"""
        schema = StructType([
            StructField("hour", IntegerType(), False),
            StructField("num_trips", IntegerType(), False),
        ])
        
        # Dados de 24 horas
        test_data = [
            (0, 10), (1, 8), (2, 5), (3, 3), (4, 5), (5, 15),
            (6, 35), (7, 50), (8, 45), (9, 30), (10, 25), (11, 30),
            (12, 35), (13, 30), (14, 28), (15, 32), (16, 40), (17, 55),
            (18, 50), (19, 35), (20, 25), (21, 20), (22, 15), (23, 12)
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Identificar picos (> 40 viagens)
        PEAK_THRESHOLD = 40
        
        df_with_peak = df.withColumn(
            "is_peak_hour",
            when(col("num_trips") > PEAK_THRESHOLD, True).otherwise(False)
        )
        
        peak_hours = df_with_peak.filter(col("is_peak_hour") == True).select("hour").rdd.flatMap(lambda x: x).collect()
        
        assert 7 in peak_hours or 8 in peak_hours  # Pico manhã
        assert 17 in peak_hours or 18 in peak_hours  # Pico tarde
    
    def test_service_quality_score(self, spark_session):
        """Testa cálculo de score de qualidade do serviço"""
        schema = StructType([
            StructField("route_code", StringType(), False),
            StructField("on_time_rate", DoubleType(), False),
            StructField("occupancy_rate", DoubleType(), False),
            StructField("avg_speed", DoubleType(), False),
        ])
        
        test_data = [
            ("8000-10", 0.95, 0.75, 35.0),
            ("8000-20", 0.85, 0.85, 30.0),
            ("8100-15", 0.90, 0.70, 40.0),
        ]
        
        df = spark_session.createDataFrame(test_data, schema)
        
        # Calcular score composto (0-100)
        # Fórmula: 40% pontualidade + 30% ocupação + 30% velocidade normalizada
        df_with_score = df.withColumn(
            "quality_score",
            (col("on_time_rate") * 40 +
             col("occupancy_rate") * 30 +
             (col("avg_speed") / 50) * 30) * 100
        )
        
        scores = df_with_score.select("quality_score").rdd.flatMap(lambda x: x).collect()
        
        assert all(0 <= score <= 100 for score in scores)


class TestSilverToGoldPipeline:
    """Testes para pipeline completo Silver → Gold"""
    
    def test_end_to_end_silver_to_gold(self, spark_session, temp_data_dir):
        """Testa transformação completa Silver → Gold"""
        # 1. Criar dados Silver
        silver_schema = StructType([
            StructField("vehicle_id", StringType(), False),
            StructField("route_code", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("speed", DoubleType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
        ])
        
        base_time = datetime(2025, 10, 29, 8, 0, 0)
        silver_data = []
        
        for i in range(200):
            vehicle_id = f"V{i % 20:03d}"
            route = f"8000-{10 + (i % 5)}"
            timestamp = base_time + timedelta(minutes=i)
            speed = 25.0 + (i % 35)
            lat = -23.5505 + (i % 100) * 0.001
            lon = -46.6333 + (i % 100) * 0.001
            silver_data.append((vehicle_id, route, timestamp, speed, lat, lon))
        
        silver_df = spark_session.createDataFrame(silver_data, silver_schema)
        
        # 2. Transformar para Gold (agregações)
        gold_hourly = silver_df.withColumn("hour", date_trunc("hour", col("timestamp"))) \
            .groupBy("hour", "route_code") \
            .agg(
                count("vehicle_id").alias("num_vehicles"),
                avg("speed").alias("avg_speed"),
                spark_min("speed").alias("min_speed"),
                spark_max("speed").alias("max_speed")
            )
        
        gold_route = silver_df.groupBy("route_code") \
            .agg(
                count("vehicle_id").alias("total_records"),
                avg("speed").alias("avg_speed")
            )
        
        # 3. Salvar Gold
        gold_hourly.write.mode("overwrite").parquet(f"{temp_data_dir['gold']}/hourly")
        gold_route.write.mode("overwrite").parquet(f"{temp_data_dir['gold']}/route")
        
        # 4. Validar
        hourly_result = spark_session.read.parquet(f"{temp_data_dir['gold']}/hourly")
        route_result = spark_session.read.parquet(f"{temp_data_dir['gold']}/route")
        
        assert hourly_result.count() > 0
        assert route_result.count() == 5  # 5 rotas diferentes
        assert all(row.avg_speed > 0 for row in route_result.collect())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
