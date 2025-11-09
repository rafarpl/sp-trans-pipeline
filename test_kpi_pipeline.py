import sys
sys.path.insert(0, '/workspaces/sp-trans-pipeline')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import random

print("🧪 Teste rápido do pipeline KPI...")

# Configuração PostgreSQL
POSTGRES_URL = "jdbc:postgresql://localhost:5432/sptrans_test"
POSTGRES_USER = "test_user"
POSTGRES_PASSWORD = "test_password"

# Iniciar Spark
spark = SparkSession.builder \
    .appName("Test-KPI") \
    .master("local[2]") \
    .config("spark.ui.enabled", "false") \
    .config("spark.jars", "/usr/local/lib/postgresql-42.7.1.jar") \
    .getOrCreate()

print("✅ Spark iniciado")

# Gerar dados de teste
data = []
lines = [f"{8000+i}" for i in range(10)]
base_time = datetime.now()

for i in range(200):
    data.append({
        "vehicle_id": f"v_{i % 40}",
        "line_id": random.choice(lines),
        "latitude": -23.55 + random.uniform(-0.03, 0.03),
        "longitude": -46.63 + random.uniform(-0.03, 0.03),
        "timestamp": base_time,
        "speed": random.uniform(10, 50),
        "route_id": f"r_{i % 10}"
    })

df = spark.createDataFrame(data)
print(f"📦 Criados {df.count()} registros de teste")

# Validar (Silver)
df_silver = df.filter(
    (F.col("latitude").between(-24.0, -23.0)) &
    (F.col("longitude").between(-47.0, -46.0))
)
print(f"🔹 Silver: {df_silver.count()} registros válidos")

# KPI Realtime
total_v = df_silver.select("vehicle_id").distinct().count()
total_l = df_silver.select("line_id").distinct().count()

kpi_realtime = spark.createDataFrame([{
    "timestamp": base_time,
    "total_vehicles": total_v,
    "total_lines": total_l,
    "stale_vehicles": 0,
    "stale_percentage": 0.0,
    "coverage_percentage": round(total_l / 400 * 100, 2),
    "avg_update_interval_seconds": 180,
    "data_source": "test",
    "records_processed": df_silver.count()
}])

print(f"📊 KPI: {total_v} veículos, {total_l} linhas")

# Salvar no PostgreSQL
try:
    kpi_realtime.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "serving.kpi_realtime") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    print("✅ Dados salvos no PostgreSQL!")
    
    # Verificar
    df_check = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "serving.kpi_realtime") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    print(f"✅ Verificação: {df_check.count()} registros na tabela")
    print("\n📋 Últimos registros:")
    df_check.orderBy(F.desc("timestamp")).show(5, truncate=False)
    
except Exception as e:
    print(f"❌ Erro ao salvar: {e}")

spark.stop()
print("✅ Teste completo!")
