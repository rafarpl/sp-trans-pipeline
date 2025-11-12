#!/usr/bin/env python3
"""
SPTrans Pipeline - KPIs Completos para Grafana
Atualiza a cada 3 minutos com todos os KPIs viáveis
"""

import sys
sys.path.insert(0, '/workspaces/sp-trans-pipeline')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from math import radians, sin, cos, sqrt, atan2
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import time
import random

print("""
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║     🚌  SPTrans KPI Pipeline para Grafana                ║
║                                                           ║
║     Pipeline completo com todos os KPIs viáveis          ║
║     Atualização automática a cada 3 minutos              ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
""")

# Configuração PostgreSQL
POSTGRES_URL = "jdbc:postgresql://localhost:5432/sptrans_test"
POSTGRES_USER = "test_user"
POSTGRES_PASSWORD = "test_password"

iteration = 0

def init_spark():
    """Inicializar Spark Session com suporte a MinIO"""
    
    # Lista de JARs
    jars = [
        "/usr/local/lib/postgresql-42.7.1.jar",
        "/usr/local/lib/hadoop-aws-3.3.4.jar",
        "/usr/local/lib/aws-java-sdk-bundle-1.12.262.jar"
    ]
    
    spark = SparkSession.builder \
        .appName("SPTrans-KPI-Pipeline") \
        .master("local[2]") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.driver.extraClassPath", ":".join(jars)) \
        .config("spark.executor.extraClassPath", ":".join(jars)) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark iniciado com Data Lake (MinIO)")
    return spark

def calculate_speed(lat1, lon1, lat2, lon2, time_diff_seconds):
    """
    Calcula velocidade entre dois pontos GPS
    
    Args:
        lat1, lon1: Coordenadas do ponto 1
        lat2, lon2: Coordenadas do ponto 2  
        time_diff_seconds: Diferença de tempo em segundos
        
    Returns:
        Velocidade em km/h
    """
    if time_diff_seconds == 0:
        return 0.0
    
    # Raio da Terra em km
    R = 6371.0
    
    # Converter para radianos
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)
    
    # Diferenças
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    # Fórmula de Haversine
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    # Distância em km
    distance_km = R * c
    
    # Velocidade em km/h
    speed_kmh = (distance_km / time_diff_seconds) * 3600
    
    return round(speed_kmh, 2)

def fetch_api_data():
    """Buscar dados da API SPTrans"""
    try:
        from src.ingestion.sptrans_api_client import SPTransAPIClient
        
        client = SPTransAPIClient()
        
        if client.authenticate():
            positions = client.get_vehicle_positions()
            
            if positions and len(positions) > 0:
                print(f"   ✅ API: {len(positions)} veículos")
                return positions, True
            else:
                print("   ⚠️  API retornou 0 veículos")
                return None, False
        else:
            print("   ❌ Falha na autenticação")
            return None, False
            
    except Exception as e:
        print(f"   ⚠️  Erro API: {e}")
        return None, False

def generate_synthetic_data(count=300):
    """Gerar dados sintéticos"""
    print(f"   📦 Gerando {count} registros sintéticos")
    
    lines = [f"{8000+i}" for i in range(12)]
    base_time = datetime.now()
    
    data = []
    for i in range(count):
        line_id = random.choice(lines)
        speed = random.triangular(0, 60, 25)
        
        data.append({
            "vehicle_id": f"v_{i % 60}",
            "line_id": line_id,
            "latitude": -23.55 + random.uniform(-0.05, 0.05),
            "longitude": -46.63 + random.uniform(-0.05, 0.05),
            "timestamp": base_time - timedelta(seconds=random.randint(0, 300)),
            "speed": round(speed, 2),
            "route_id": f"r_{line_id}"
        })
    
    return data, False

def write_to_postgres(spark, df, table_name, mode="append"):
    """Escrever no PostgreSQL"""
    try:
        df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", f"serving.{table_name}") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()
        
        print(f"   ✅ Salvo: serving.{table_name}")
        
    except Exception as e:
        print(f"   ❌ Erro ao salvar {table_name}: {e}")

def save_to_datalake(df, layer, table_name, partition_cols=None):
    """
    Salvar dados no Data Lake (MinIO)
    
    Args:
        df: DataFrame Spark
        layer: bronze, silver, gold
        table_name: nome da tabela
        partition_cols: lista de colunas para particionar
    """
    try:
        current_time = datetime.now()
        
        # Path no formato: s3a://bucket/layer/table/year=YYYY/month=MM/day=DD/hour=HH/
        base_path = f"s3a://sptrans-datalake/{layer}/{table_name}"
        
        # Adicionar colunas de partição
        df_to_save = df.withColumn("year", F.year(F.col("timestamp"))) \
                       .withColumn("month", F.month(F.col("timestamp"))) \
                       .withColumn("day", F.dayofmonth(F.col("timestamp"))) \
                       .withColumn("hour", F.hour(F.col("timestamp")))
        
        # Salvar como Parquet particionado
        df_to_save.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("year", "month", "day", "hour") \
            .option("compression", "snappy") \
            .save(base_path)
        
        record_count = df.count()
        print(f"   💾 Data Lake ({layer}): {record_count} registros salvos em {base_path}")
        
    except Exception as e:
        print(f"   ⚠️  Erro ao salvar no Data Lake: {e}")

def run_iteration(spark):
    """Executar uma iteração completa"""
    global iteration
    
    iteration += 1
    start_time = time.time()
    current_time = datetime.now()
    
    print("\n" + "="*70)
    print(f"🚀 ITERAÇÃO #{iteration} - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # 1. Buscar dados
    print("\n📡 [1/6] Buscando dados...")
    positions, use_real = fetch_api_data()
    
    if not use_real or not positions:
        positions, use_real = generate_synthetic_data()
    
    data_source = "api" if use_real else "synthetic"
    
    # 2. Bronze
    print("\n📦 [2/6] Camada Bronze...")
    # Schema explícito para evitar erro de inferência
    schema = StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("line_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("route_id", StringType(), True)
    ])
    
    df_bronze = spark.createDataFrame(positions, schema=schema)
    print(f"   ✅ Bronze: {df_bronze.count()} registros")
    save_to_datalake(df_bronze, "bronze", "vehicle_positions")

    # 3. Silver - Validação e Cálculo de Velocidade
    print("\n🔹 [3/6] Camada Silver...")

    # Validar dados
    df_silver = df_bronze.filter(
        (F.col("latitude").between(-24.0, -23.0)) &
        (F.col("longitude").between(-47.0, -46.0))
    ).dropDuplicates(["vehicle_id", "timestamp"])

    # Ler posições anteriores do PostgreSQL
    try:
        df_previous = spark.read \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "serving.vehicle_positions_latest") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .select(
                F.col("vehicle_id").alias("prev_vehicle_id"),
                F.col("latitude").alias("prev_lat"),
                F.col("longitude").alias("prev_lon"),
                F.col("timestamp").alias("prev_time")
            )
        
        prev_count = df_previous.count()
        print(f"   📊 Posições anteriores: {prev_count} veículos")
        
        # Join com posições anteriores
        df_with_prev = df_silver.join(
            df_previous,
            df_silver.vehicle_id == df_previous.prev_vehicle_id,
            "left"
        )
        
        # UDF para calcular velocidade
        def calc_speed_udf(lat1, lon1, lat2, lon2, time1, time2):
            if lat1 is None or lat2 is None or time1 is None or time2 is None:
                return 0.0
            
            time_diff = (time2 - time1).total_seconds()
            
            if time_diff <= 0 or time_diff > 600:  # Ignorar se > 10 min
                return 0.0
            
            return calculate_speed(lat1, lon1, lat2, lon2, time_diff)
        
        speed_udf = F.udf(calc_speed_udf, DoubleType())
        
        # Calcular velocidade
        df_silver = df_with_prev.withColumn(
            "calculated_speed",
            speed_udf("prev_lat", "prev_lon", "latitude", "longitude", "prev_time", "timestamp")
        )
        
        # Limitar velocidade
        df_silver = df_silver.withColumn(
            "speed",
            F.when(F.col("calculated_speed") > 100, 0.0)
            .when(F.col("calculated_speed") < 0, 0.0)
            .otherwise(F.col("calculated_speed"))
        )
        
        # Remover colunas auxiliares
        df_silver = df_silver.select(
            "vehicle_id", "line_id", "latitude", "longitude", 
            "timestamp", "speed", "route_id"
        )
        
        # Debug
        speed_count = df_silver.filter(F.col("speed") > 0).count()
        avg_speed = df_silver.filter(F.col("speed") > 0).agg(F.avg("speed")).collect()[0][0]
        if avg_speed:
            print(f"   🔍 Veículos com velocidade > 0: {speed_count} (média: {avg_speed:.1f} km/h)")
        else:
            print(f"   🔍 Veículos com velocidade > 0: 0")
        
    except Exception as e:
        print(f"   ⚠️  Primeira iteração ou erro: {e}")
        # Se não tem histórico, manter speed = 0
        df_silver = df_silver.withColumn("speed", F.lit(0.0))

    silver_count = df_silver.count()
    print(f"   ✅ Silver: {silver_count} registros processados")
    save_to_datalake(df_silver, "silver", "vehicle_positions_validated")
    
    # 4. KPIs
    print("\n📊 [4/6] Calculando KPIs...")
    
    # KPI Realtime
    total_vehicles = df_silver.select("vehicle_id").distinct().count()
    total_lines = df_silver.select("line_id").distinct().count()
    
    stale_threshold = current_time - timedelta(minutes=4)
    stale_count = df_silver.filter(F.col("timestamp") < F.lit(stale_threshold)).count()
    stale_pct = (stale_count / total_vehicles * 100) if total_vehicles > 0 else 0
    coverage_pct = (total_lines / 400 * 100) if total_lines > 0 else 0
    
    df_kpi_realtime = spark.createDataFrame([{
        "timestamp": current_time,
        "total_vehicles": total_vehicles,
        "total_lines": total_lines,
        "stale_vehicles": stale_count,
        "stale_percentage": round(stale_pct, 2),
        "coverage_percentage": round(coverage_pct, 2),
        "avg_update_interval_seconds": 180,
        "data_source": data_source,
        "records_processed": silver_count
    }])
    
    # KPI por Linha
    df_kpi_by_line = df_silver.groupBy("line_id").agg(
        F.count("vehicle_id").alias("total_vehicles"),
        F.avg("speed").alias("avg_speed"),
        F.max("speed").alias("max_speed"),
        F.min("speed").alias("min_speed"),
        F.sum(F.when(F.col("speed") < 20, 1).otherwise(0)).alias("vehicles_0_20kmh"),
        F.sum(F.when((F.col("speed") >= 20) & (F.col("speed") < 40), 1).otherwise(0)).alias("vehicles_20_40kmh"),
        F.sum(F.when((F.col("speed") >= 40) & (F.col("speed") < 60), 1).otherwise(0)).alias("vehicles_40_60kmh"),
        F.sum(F.when(F.col("speed") >= 60, 1).otherwise(0)).alias("vehicles_60plus_kmh")
    ).withColumn("timestamp", F.lit(current_time))
    
    # KPI Qualidade
    records_ingested = df_bronze.count()
    records_valid = silver_count
    validation_rate = (records_valid / records_ingested * 100) if records_ingested > 0 else 0
    
    df_kpi_quality = spark.createDataFrame([{
        "timestamp": current_time,
        "pipeline_status": "running",
        "execution_duration_seconds": int(time.time() - start_time),
        "records_ingested": records_ingested,
        "records_valid": records_valid,
        "records_invalid": records_ingested - records_valid,
        "validation_rate": round(validation_rate, 2),
        "vehicles_stale": stale_count,
        "staleness_percentage": round(stale_pct, 2),
        "alerts_triggered": 0
    }])
    
    # Posições Latest
    window_spec = Window.partitionBy("vehicle_id").orderBy(F.col("timestamp").desc())
    df_latest = df_silver.withColumn("row_num", F.row_number().over(window_spec)) \
        .filter(F.col("row_num") == 1) \
        .select("vehicle_id", "line_id", "latitude", "longitude", "speed", "timestamp") \
        .withColumn("updated_at", F.current_timestamp())
    
    # Timeseries
    avg_speed = df_silver.agg(F.avg("speed")).collect()[0][0] or 0
    
    timeseries_data = [
        {"timestamp": current_time, "metric_name": "total_vehicles", "metric_value": float(total_vehicles), "line_id": None},
        {"timestamp": current_time, "metric_name": "avg_speed_global", "metric_value": float(avg_speed), "line_id": None}
    ]
    
    top_lines = df_silver.groupBy("line_id").agg(F.count("vehicle_id").alias("count")).orderBy(F.desc("count")).limit(10).collect()
    
    for row in top_lines:
        timeseries_data.append({
            "timestamp": current_time,
            "metric_name": "vehicles_per_line",
            "metric_value": float(row['count']),
            "line_id": row['line_id']
        })
    
    df_timeseries = spark.createDataFrame(timeseries_data)
    
    print(f"   ✅ KPIs calculados: {total_vehicles} veículos, {total_lines} linhas")
    save_to_datalake(df_kpi_realtime, "gold", "kpi_realtime")
    save_to_datalake(df_kpi_by_line, "gold", "kpi_by_line")

    # 5. Salvar PostgreSQL
    print("\n💾 [5/6] Salvando no PostgreSQL...")
    write_to_postgres(spark, df_kpi_realtime, "kpi_realtime")
    write_to_postgres(spark, df_kpi_by_line, "kpi_by_line")
    write_to_postgres(spark, df_kpi_quality, "kpi_quality")
    write_to_postgres(spark, df_latest, "vehicle_positions_latest", mode="overwrite")
    write_to_postgres(spark, df_timeseries, "kpi_timeseries")
    
    # 6. Resumo
    duration = time.time() - start_time
    print(f"\n✅ [6/6] Iteração #{iteration} completa em {duration:.1f}s")
    print(f"📊 Resumo: {total_vehicles} veículos, {total_lines} linhas, fonte: {data_source}")
    
    return duration

def main():
    """Loop principal"""
    
    spark = init_spark()
    interval_seconds = 120  # 2 minutos
    
    print(f"\n🔄 Iniciando loop de atualização...")
    print(f"⏱️  Intervalo: {interval_seconds}s ({interval_seconds/60:.1f} min)")
    print("💡 Pressione Ctrl+C para parar\n")
    
    try:
        while True:
            try:
                run_iteration(spark)
                
                next_run = datetime.now() + timedelta(seconds=interval_seconds)
                print(f"\n⏱️  Próxima execução: {next_run.strftime('%H:%M:%S')}")
                print("─" * 70)
                
                time.sleep(interval_seconds)
                
            except Exception as e:
                print(f"\n❌ Erro na iteração: {e}")
                print("⏱️  Aguardando 60s...")
                time.sleep(60)
                
    except KeyboardInterrupt:
        print("\n\n🛑 Pipeline interrompido pelo usuário")
        print(f"📊 Total de iterações: {iteration}")
    
    finally:
        spark.stop()
        print("✅ Spark finalizado")

if __name__ == "__main__":
    main()