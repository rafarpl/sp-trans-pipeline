#!/usr/bin/env python3
"""
SPTrans Pipeline - KPIs Completos para Grafana
Atualiza a cada 3 minutos com todos os KPIs viáveis
"""

import sys
sys.path.insert(0, '/workspaces/sp-trans-pipeline')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import time
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuração PostgreSQL
POSTGRES_URL = "jdbc:postgresql://localhost:5432/sptrans_test"
POSTGRES_USER = "test_user"
POSTGRES_PASSWORD = "test_password"
POSTGRES_DRIVER = "org.postgresql.Driver"

class SPTransKPIPipeline:
    """Pipeline de KPIs SPTrans"""
    
    def __init__(self):
        self.spark = None
        self.iteration = 0
        
    def init_spark(self):
        """Inicializar Spark Session"""
        self.spark = SparkSession.builder \
            .appName("SPTrans-KPI-Pipeline") \
            .master("local[2]") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.jars", "/usr/local/lib/postgresql-42.7.1.jar") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark iniciado")
    
    def fetch_api_data(self):
        """Buscar dados da API SPTrans"""
        try:
            from src.ingestion.sptrans_api_client import SPTransAPIClient
            
            client = SPTransAPIClient()
            
            if client.authenticate():
                positions = client.get_vehicle_positions()
                
                if positions and len(positions) > 0:
                    logger.info(f"✅ API: {len(positions)} veículos recebidos")
                    return positions, True
                else:
                    logger.warning("⚠️  API retornou 0 veículos")
                    return None, False
            else:
                logger.error("❌ Falha na autenticação")
                return None, False
                
        except Exception as e:
            logger.error(f"❌ Erro na API: {e}")
            return None, False
    
    def generate_synthetic_data(self, count=300):
        """Gerar dados sintéticos para testes"""
        logger.info(f"📦 Gerando {count} registros sintéticos")
        
        import random
        base_time = datetime.now()
        
        lines = [f"{8000+i}" for i in range(12)]  # 12 linhas
        
        data = []
        for i in range(count):
            line_id = random.choice(lines)
            
            # Distribuição realística de velocidades
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
    
    def process_bronze_to_silver(self, df_bronze):
        """Processar Bronze → Silver com validações"""
        
        # Validações
        df_silver = df_bronze.filter(
            (F.col("latitude").between(-24.0, -23.0)) &
            (F.col("longitude").between(-47.0, -46.0)) &
            (F.col("speed") >= 0) &
            (F.col("speed") <= 120)
        )
        
        # Remover duplicatas
        df_silver = df_silver.dropDuplicates(["vehicle_id", "timestamp"])
        
        # Adicionar metadados
        df_silver = df_silver.withColumn("processed_at", F.current_timestamp())
        
        logger.info(f"🔹 Silver: {df_silver.count()} registros válidos")
        
        return df_silver
    
    def calculate_kpi_realtime(self, df_silver, data_source):
        """Calcular KPIs em Tempo Real"""
        
        current_time = datetime.now()
        
        # KPI 1: Total de veículos e linhas
        total_vehicles = df_silver.select("vehicle_id").distinct().count()
        total_lines = df_silver.select("line_id").distinct().count()
        
        # KPI 2: Staleness (veículos desatualizados >4 min)
        stale_threshold = current_time - timedelta(minutes=4)
        df_with_age = df_silver.withColumn(
            "is_stale",
            F.when(F.col("timestamp") < F.lit(stale_threshold), 1).otherwise(0)
        )
        
        stale_count = df_with_age.filter(F.col("is_stale") == 1).count()
        stale_pct = (stale_count / total_vehicles * 100) if total_vehicles > 0 else 0
        
        # KPI 4: Cobertura (% de linhas com veículos)
        # Assumir 400 linhas totais em SP (aproximado)
        coverage_pct = (total_lines / 400 * 100) if total_lines > 0 else 0
        
        # KPI 3: Intervalo médio de atualização (simplificado)
        avg_interval = 180  # 3 min padrão
        
        kpi_data = [{
            "timestamp": current_time,
            "total_vehicles": total_vehicles,
            "total_lines": total_lines,
            "stale_vehicles": stale_count,
            "stale_percentage": round(stale_pct, 2),
            "coverage_percentage": round(coverage_pct, 2),
            "avg_update_interval_seconds": avg_interval,
            "data_source": data_source,
            "records_processed": df_silver.count()
        }]
        
        df_kpi = self.spark.createDataFrame(kpi_data)
        
        logger.info(f"📊 KPI Realtime: {total_vehicles} veículos, {total_lines} linhas")
        
        return df_kpi
    
    def calculate_kpi_by_line(self, df_silver):
        """Calcular KPIs por Linha"""
        
        current_time = datetime.now()
        
        df_by_line = df_silver.groupBy("line_id").agg(
            F.count("vehicle_id").alias("total_vehicles"),
            F.avg("speed").alias("avg_speed"),
            F.max("speed").alias("max_speed"),
            F.min("speed").alias("min_speed"),
            
            # Distribuição de velocidade
            F.sum(F.when(F.col("speed") < 20, 1).otherwise(0)).alias("vehicles_0_20kmh"),
            F.sum(F.when((F.col("speed") >= 20) & (F.col("speed") < 40), 1).otherwise(0)).alias("vehicles_20_40kmh"),
            F.sum(F.when((F.col("speed") >= 40) & (F.col("speed") < 60), 1).otherwise(0)).alias("vehicles_40_60kmh"),
            F.sum(F.when(F.col("speed") >= 60, 1).otherwise(0)).alias("vehicles_60plus_kmh")
        ).withColumn("timestamp", F.lit(current_time))
        
        logger.info(f"📈 KPI por Linha: {df_by_line.count()} linhas processadas")
        
        return df_by_line
    
    def calculate_kpi_quality(self, df_bronze, df_silver, execution_time):
        """Calcular KPIs de Qualidade"""
        
        current_time = datetime.now()
        
        records_ingested = df_bronze.count()
        records_valid = df_silver.count()
        records_invalid = records_ingested - records_valid
        validation_rate = (records_valid / records_ingested * 100) if records_ingested > 0 else 0
        
        # Staleness
        stale_threshold = current_time - timedelta(minutes=4)
        stale_count = df_silver.filter(F.col("timestamp") < F.lit(stale_threshold)).count()
        total_vehicles = df_silver.select("vehicle_id").distinct().count()
        staleness_pct = (stale_count / total_vehicles * 100) if total_vehicles > 0 else 0
        
        kpi_quality = [{
            "timestamp": current_time,
            "pipeline_status": "running",
            "execution_duration_seconds": int(execution_time),
            "records_ingested": records_ingested,
            "records_valid": records_valid,
            "records_invalid": records_invalid,
            "validation_rate": round(validation_rate, 2),
            "vehicles_stale": stale_count,
            "staleness_percentage": round(staleness_pct, 2),
            "alerts_triggered": 0
        }]
        
        df_quality = self.spark.createDataFrame(kpi_quality)
        
        logger.info(f"🔍 Qualidade: {validation_rate:.1f}% válidos")
        
        return df_quality
    
    def save_latest_positions(self, df_silver):
        """Salvar últimas posições para mapa"""
        
        # Pegar posição mais recente de cada veículo
        window_spec = Window.partitionBy("vehicle_id").orderBy(F.col("timestamp").desc())
        
        df_latest = df_silver.withColumn("row_num", F.row_number().over(window_spec)) \
            .filter(F.col("row_num") == 1) \
            .select("vehicle_id", "line_id", "latitude", "longitude", "speed", "timestamp") \
            .withColumn("updated_at", F.current_timestamp())
        
        logger.info(f"🗺️  Posições: {df_latest.count()} veículos")
        
        return df_latest
    
    def save_timeseries(self, df_silver):
        """Salvar dados para séries temporais"""
        
        current_time = datetime.now()
        
        # Métricas globais
        total_vehicles = df_silver.select("vehicle_id").distinct().count()
        avg_speed = df_silver.agg(F.avg("speed")).collect()[0][0] or 0
        
        timeseries_data = [
            {"timestamp": current_time, "metric_name": "total_vehicles", "metric_value": float(total_vehicles), "line_id": None},
            {"timestamp": current_time, "metric_name": "avg_speed_global", "metric_value": float(avg_speed), "line_id": None}
        ]
        
        # Adicionar por linha (top 10)
        top_lines = df_silver.groupBy("line_id") \
            .agg(F.count("vehicle_id").alias("count")) \
            .orderBy(F.desc("count")) \
            .limit(10) \
            .collect()
        
        for row in top_lines:
            timeseries_data.append({
                "timestamp": current_time,
                "metric_name": "vehicles_per_line",
                "metric_value": float(row['count']),
                "line_id": row['line_id']
            })
        
        df_timeseries = self.spark.createDataFrame(timeseries_data)
        
        return df_timeseries
    
    def write_to_postgres(self, df, table_name, mode="append"):
        """Escrever DataFrame no PostgreSQL"""
        try:
            df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", f"serving.{table_name}") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", POSTGRES_DRIVER) \
                .mode(mode) \
                .save()
            
            logger.info(f"✅ Salvo em serving.{table_name}")
            
        except Exception as e:
            logger.error(f"❌ Erro ao salvar {table_name}: {e}")
    
    def run_iteration(self):
        """Executar uma iteração completa do pipeline"""
        
        self.iteration += 1
        start_time = time.time()
        
        logger.info("="*70)
        logger.info(f"🚀 ITERAÇÃO #{self.iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70)
        
        # 1. Buscar dados
        positions, use_real = self.fetch_api_data()
        
        if not use_real or not positions:
            positions, use_real = self.generate_synthetic_data()
        
        data_source = "api" if use_real else "synthetic"
        
        # 2. Criar DataFrame Bronze
        df_bronze = self.spark.createDataFrame(positions)
        logger.info(f"📦 Bronze: {df_bronze.count()} registros")
        
        # 3. Processar para Silver
        df_silver = self.process_bronze_to_silver(df_bronze)
        
        # 4. Calcular KPIs
        execution_time = time.time() - start_time
        
        df_kpi_realtime = self.calculate_kpi_realtime(df_silver, data_source)
        df_kpi_by_line = self.calculate_kpi_by_line(df_silver)
        df_kpi_quality = self.calculate_kpi_quality(df_bronze, df_silver, execution_time)
        df_latest_positions = self.save_latest_positions(df_silver)
        df_timeseries = self.save_timeseries(df_silver)
        
        # 5. Salvar no PostgreSQL
        self.write_to_postgres(df_kpi_realtime, "kpi_realtime")
        self.write_to_postgres(df_kpi_by_line, "kpi_by_line")
        self.write_to_postgres(df_kpi_quality, "kpi_quality")
        self.write_to_postgres(df_latest_positions, "vehicle_positions_latest", mode="overwrite")
        self.write_to_postgres(df_timeseries, "kpi_timeseries")
        
        # 6. Estatísticas
        duration = time.time() - start_time
        logger.info(f"✅ Iteração #{self.iteration} completa em {duration:.1f}s")
        logger.info(f"📊 Resumo: {df_silver.select('vehicle_id').distinct().count()} veículos, "
                   f"{df_silver.select('line_id').distinct().count()} linhas")
        
        return duration
    
    def run_loop(self, interval_seconds=180):
        """Loop principal - executa a cada N segundos"""
        
        logger.info("🔄 Iniciando loop de atualização...")
        logger.info(f"⏱️  Intervalo: {interval_seconds}s ({interval_seconds/60:.1f} minutos)")
        logger.info("💡 Pressione Ctrl+C para parar")
        
        try:
            while True:
                try:
                    duration = self.run_iteration()
                    
                    # Aguardar próxima iteração
                    next_run = datetime.now() + timedelta(seconds=interval_seconds)
                    logger.info(f"⏱️  Próxima execução: {next_run.strftime('%H:%M:%S')}")
                    logger.info("")
                    
                    time.sleep(interval_seconds)
                    
                except Exception as e:
                    logger.error(f"❌ Erro na iteração: {e}")
                    logger.info("⏱️  Aguardando 60s antes de tentar novamente...")
                    time.sleep(60)
                    
        except KeyboardInterrupt:
            logger.info("\n🛑 Pipeline interrompido pelo usuário")
            logger.info(f"📊 Total de iterações executadas: {self.iteration}")
        
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("✅ Spark finalizado")

def main():
    """Função principal"""
    
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
    
    pipeline = SPTransKPIPipeline()
    pipeline.init_spark()
    pipeline.run_loop(interval_seconds=180)  # 3 minutos

if __name__ == "__main__":
    main()