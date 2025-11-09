cat > pipeline_completo.py << 'EOF'
import sys
sys.path.insert(0, '/workspaces/sp-trans-pipeline')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import os

# Configurar matplotlib
import matplotlib
matplotlib.use('Agg')

print("="*70)
print("🚀 PIPELINE COMPLETO SPTRANS")
print("="*70)

# 1. BUSCAR DADOS DA API
print("\n📡 [1/6] Buscando dados da API SPTrans...")

try:
    from src.ingestion.sptrans_api_client import SPTransAPIClient
    
    client = SPTransAPIClient()
    
    print("   🔐 Autenticando...")
    if client.authenticate():
        print("   ✅ Autenticado!")
        
        print("   📍 Buscando posições de veículos...")
        positions = client.get_vehicle_positions()
        
        if positions and len(positions) > 0:
            print(f"   ✅ Recebidos {len(positions)} veículos da API!")
            use_real_data = True
        else:
            print("   ⚠️  API retornou 0 veículos, usando dados sintéticos")
            use_real_data = False
    else:
        print("   ❌ Falha na autenticação, usando dados sintéticos")
        use_real_data = False
        
except Exception as e:
    print(f"   ⚠️  Erro na API: {e}")
    print("   📦 Usando dados sintéticos")
    use_real_data = False

# 2. INICIAR SPARK
print("\n⚡ [2/6] Iniciando Spark...")

spark = SparkSession.builder \
    .appName("SPTrans-Complete") \
    .master("local[2]") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

print("   ✅ Spark iniciado!")

# 3. CAMADA BRONZE
print("\n📦 [3/6] Criando camada BRONZE...")

if use_real_data:
    df_bronze = spark.createDataFrame(positions)
    print(f"   ✅ Bronze: {df_bronze.count()} veículos da API")
else:
    data = []
    base_time = datetime.now()
    for i in range(500):
        data.append({
            "vehicle_id": f"vehicle_{i % 50}",
            "latitude": -23.550520 + (i * 0.0005),
            "longitude": -46.633308 + (i * 0.0005),
            "timestamp": base_time,
            "speed": 15.0 + (i % 40),
            "line_id": f"{8000 + (i % 10)}",
            "route_id": f"route_{i % 10}"
        })
    df_bronze = spark.createDataFrame(data)
    print(f"   ✅ Bronze: {df_bronze.count()} registros sintéticos")

df_bronze.write.mode("overwrite").parquet("data/bronze/vehicle_positions")

# 4. CAMADA SILVER
print("\n🔹 [4/6] Criando camada SILVER...")

df_silver = df_bronze.filter(
    (F.col("latitude").between(-24.0, -23.0)) &
    (F.col("longitude").between(-47.0, -46.0)) &
    (F.col("speed") >= 0) &
    (F.col("speed") <= 120)
)

df_silver = df_silver.dropDuplicates(["vehicle_id", "timestamp"])
df_silver = df_silver.withColumn("is_valid", F.lit(True))

df_silver.write.mode("overwrite").parquet("data/silver/vehicle_positions")
print(f"   ✅ Silver: {df_silver.count()} registros válidos")

# 5. CAMADA GOLD
print("\n🥇 [5/6] Criando camada GOLD...")

kpi_vehicles = df_silver.groupBy("line_id").agg(
    F.count("vehicle_id").alias("total_vehicles"),
    F.avg("speed").alias("avg_speed"),
    F.max("speed").alias("max_speed"),
    F.min("speed").alias("min_speed")
).orderBy(F.desc("total_vehicles"))

kpi_vehicles.write.mode("overwrite").parquet("data/gold/kpi_vehicles_per_line")
print(f"   ✅ KPI: {kpi_vehicles.count()} linhas analisadas")

total_vehicles = df_silver.select("vehicle_id").distinct().count()
total_lines = df_silver.select("line_id").distinct().count()

# 6. GERAR GRÁFICOS
print("\n📊 [6/6] Gerando gráficos...")

os.makedirs("data/visualizations", exist_ok=True)

pdf = kpi_vehicles.toPandas()

# Gráfico 1
plt.figure(figsize=(12, 6))
plt.bar(pdf['line_id'].head(10), pdf['total_vehicles'].head(10), color='steelblue')
plt.xlabel('Linha de Ônibus', fontsize=12)
plt.ylabel('Número de Veículos', fontsize=12)
plt.title('Top 10 Linhas com Mais Veículos', fontsize=14, fontweight='bold')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('data/visualizations/01_vehicles_per_line.png', dpi=300)
print("   ✅ Gráfico 1 salvo")
plt.close()

# Gráfico 2
plt.figure(figsize=(12, 6))
plt.barh(pdf['line_id'].head(10), pdf['avg_speed'].head(10), color='coral')
plt.xlabel('Velocidade Média (km/h)', fontsize=12)
plt.ylabel('Linha', fontsize=12)
plt.title('Velocidade Média por Linha', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('data/visualizations/02_avg_speed.png', dpi=300)
print("   ✅ Gráfico 2 salvo")
plt.close()

# Dashboard
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Dashboard SPTrans', fontsize=16, fontweight='bold')

axes[0, 0].bar(pdf['line_id'].head(5), pdf['total_vehicles'].head(5))
axes[0, 0].set_title('Top 5 Linhas')
axes[0, 0].tick_params(axis='x', rotation=45)

axes[0, 1].bar(pdf['line_id'].head(5), pdf['avg_speed'].head(5), color='coral')
axes[0, 1].set_title('Velocidade Média')
axes[0, 1].tick_params(axis='x', rotation=45)

axes[1, 0].bar(pdf['line_id'].head(5), pdf['max_speed'].head(5), color='green')
axes[1, 0].set_title('Velocidade Máxima')
axes[1, 0].tick_params(axis='x', rotation=45)

axes[1, 1].axis('off')
metrics = f"""
MÉTRICAS GERAIS

Veículos: {total_vehicles}
Linhas: {total_lines}

Bronze: {df_bronze.count()}
Silver: {df_silver.count()}
Gold: {kpi_vehicles.count()}
"""
axes[1, 1].text(0.1, 0.5, metrics, fontsize=12, family='monospace')

plt.tight_layout()
plt.savefig('data/visualizations/03_dashboard.png', dpi=300)
print("   ✅ Gráfico 3 salvo")
plt.close()

print("\n" + "="*70)
print("✅ PIPELINE COMPLETO!")
print("="*70)
print(f"\n📊 ESTATÍSTICAS:")
print(f"   Fonte: {'API Real' if use_real_data else 'Sintético'}")
print(f"   Bronze: {df_bronze.count()}")
print(f"   Silver: {df_silver.count()}")
print(f"   Veículos: {total_vehicles}")
print(f"   Linhas: {total_lines}")
print(f"\n📊 GRÁFICOS em: data/visualizations/")

spark.stop()
EOF