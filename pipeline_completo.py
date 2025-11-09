import sys
sys.path.insert(0, '/workspaces/sp-trans-pipeline')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import os
import matplotlib
matplotlib.use('Agg')

print("="*70)
print("🚀 PIPELINE COMPLETO SPTRANS")
print("="*70)

print("\n📡 [1/6] Buscando dados da API SPTrans...")

try:
    from src.ingestion.sptrans_api_client import SPTransAPIClient
    client = SPTransAPIClient()
    print("   🔐 Autenticando...")
    if client.authenticate():
        print("   ✅ Autenticado!")
        print("   📍 Buscando posições...")
        positions = client.get_vehicle_positions()
        if positions and len(positions) > 0:
            print(f"   ✅ {len(positions)} veículos da API!")
            use_real_data = True
        else:
            print("   ⚠️  API retornou 0, usando sintéticos")
            use_real_data = False
    else:
        print("   ❌ Falha na autenticação")
        use_real_data = False
except Exception as e:
    print(f"   ⚠️  Erro: {e}")
    use_real_data = False

print("\n⚡ [2/6] Iniciando Spark...")
spark = SparkSession.builder.appName("SPTrans").master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
print("   ✅ Spark OK!")

print("\n📦 [3/6] Camada BRONZE...")
if use_real_data:
    df_bronze = spark.createDataFrame(positions)
else:
    data = []
    base_time = datetime.now()
    for i in range(500):
        data.append({"vehicle_id": f"v_{i%50}", "latitude": -23.55 + i*0.0005, "longitude": -46.63 + i*0.0005, "timestamp": base_time, "speed": 15 + i%40, "line_id": f"{8000+i%10}", "route_id": f"r_{i%10}"})
    df_bronze = spark.createDataFrame(data)
print(f"   ✅ {df_bronze.count()} registros")
df_bronze.write.mode("overwrite").parquet("data/bronze/vehicle_positions")

print("\n🔹 [4/6] Camada SILVER...")
df_silver = df_bronze.filter((F.col("latitude").between(-24.0, -23.0)) & (F.col("longitude").between(-47.0, -46.0)) & (F.col("speed") >= 0) & (F.col("speed") <= 120))
df_silver = df_silver.dropDuplicates(["vehicle_id", "timestamp"])
df_silver.write.mode("overwrite").parquet("data/silver/vehicle_positions")
print(f"   ✅ {df_silver.count()} válidos")

print("\n🥇 [5/6] Camada GOLD...")
kpi = df_silver.groupBy("line_id").agg(F.count("vehicle_id").alias("total"), F.avg("speed").alias("avg_speed"), F.max("speed").alias("max_speed")).orderBy(F.desc("total"))
kpi.write.mode("overwrite").parquet("data/gold/kpi")
total_v = df_silver.select("vehicle_id").distinct().count()
total_l = df_silver.select("line_id").distinct().count()
print(f"   ✅ {kpi.count()} linhas, {total_v} veículos")

print("\n📊 [6/6] Gerando gráficos...")
os.makedirs("data/visualizations", exist_ok=True)
pdf = kpi.toPandas()

plt.figure(figsize=(12, 6))
plt.bar(pdf['line_id'].head(10), pdf['total'].head(10), color='steelblue')
plt.xlabel('Linha')
plt.ylabel('Veículos')
plt.title('Top 10 Linhas')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('data/visualizations/01_vehicles.png', dpi=300)
plt.close()
print("   ✅ Gráfico 1")

plt.figure(figsize=(12, 6))
plt.barh(pdf['line_id'].head(10), pdf['avg_speed'].head(10), color='coral')
plt.xlabel('Velocidade Média (km/h)')
plt.ylabel('Linha')
plt.title('Velocidade por Linha')
plt.tight_layout()
plt.savefig('data/visualizations/02_speed.png', dpi=300)
plt.close()
print("   ✅ Gráfico 2")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('Dashboard SPTrans', fontsize=16, fontweight='bold')
axes[0,0].bar(pdf['line_id'].head(5), pdf['total'].head(5))
axes[0,0].set_title('Top 5 Linhas')
axes[0,0].tick_params(axis='x', rotation=45)
axes[0,1].bar(pdf['line_id'].head(5), pdf['avg_speed'].head(5), color='coral')
axes[0,1].set_title('Vel. Média')
axes[0,1].tick_params(axis='x', rotation=45)
axes[1,0].bar(pdf['line_id'].head(5), pdf['max_speed'].head(5), color='green')
axes[1,0].set_title('Vel. Máxima')
axes[1,0].tick_params(axis='x', rotation=45)
axes[1,1].axis('off')
axes[1,1].text(0.1, 0.5, f"Veículos: {total_v}\nLinhas: {total_l}\nBronze: {df_bronze.count()}\nSilver: {df_silver.count()}", fontsize=12, family='monospace')
plt.tight_layout()
plt.savefig('data/visualizations/03_dashboard.png', dpi=300)
plt.close()
print("   ✅ Gráfico 3")

print("\n" + "="*70)
print("✅ PIPELINE COMPLETO!")
print("="*70)
print(f"Fonte: {'API Real' if use_real_data else 'Sintético'}")
print(f"Bronze: {df_bronze.count()}, Silver: {df_silver.count()}")
print(f"Veículos: {total_v}, Linhas: {total_l}")
print(f"\n📊 Gráficos em: data/visualizations/")
spark.stop()