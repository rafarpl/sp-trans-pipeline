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
    """Inicializar Spark"""
    spark = SparkSession.builder \
        .appName("SPTrans-KPI-Pipeline") \
        .master("local[2]") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars", "/usr/local/lib/postgresql-42.7.1.jar") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark iniciado")
    return spark

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
    
    # 3. Silver - Validação
    print("\n🔹 [3/6] Camada Silver...")
    df_silver = df_bronze.filter(
        (F.col("latitude").between(-24.0, -23.0)) &
        (F.col("longitude").between(-47.0, -46.0)) &
        (F.col("speed") >= 0) &
        (F.col("speed") <= 120)
    ).dropDuplicates(["vehicle_id", "timestamp"])
    
    silver_count = df_silver.count()
    print(f"   ✅ Silver: {silver_count} registros válidos")
    
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
    interval_seconds = 180  # 3 minutos
    
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