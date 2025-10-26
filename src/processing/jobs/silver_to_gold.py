"""
Silver to Gold Transformation Job
==================================
Job Spark para agregação de dados Silver → Gold com cálculo de KPIs.

Operações:
- Agregações por hora/dia/rota
- Cálculo de KPIs operacionais
- Análise de headway
- Métricas de performance

Autor: Rafael - SPTrans Pipeline
"""

from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

from src.common.logging_config import get_logger
from src.common.config import Config
from src.common.metrics import track_job_execution, reporter

logger = get_logger(__name__)


class SilverToGoldJob:
    """Job de agregação Silver → Gold."""
    
    def __init__(self,
                 silver_path: str,
                 gold_path: str,
                 execution_date: Optional[datetime] = None,
                 aggregation_level: str = 'hourly'):
        """
        Inicializa job.
        
        Args:
            silver_path: Caminho da camada Silver
            gold_path: Caminho da camada Gold
            execution_date: Data de execução
            aggregation_level: 'hourly' ou 'daily'
        """
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.execution_date = execution_date or datetime.now()
        self.aggregation_level = aggregation_level
        
        self.config = Config()
        
        # Estatísticas
        self.stats = {
            'records_processed': 0,
            'kpis_generated': 0,
            'routes_processed': 0,
            'headway_records': 0
        }
        
        logger.info(f"SilverToGoldJob inicializado: "
                   f"silver={silver_path}, gold={gold_path}, "
                   f"level={aggregation_level}")
    
    def _create_spark_session(self) -> SparkSession:
        """Cria sessão Spark."""
        logger.info("Criando sessão Spark")
        
        spark = SparkSession.builder \
            .appName("Silver_to_Gold_Aggregation") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.hadoop.fs.s3a.endpoint", self.config.MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", self.config.MINIO_ROOT_USER) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config.MINIO_ROOT_PASSWORD) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    def _read_silver_data(self, spark: SparkSession) -> DataFrame:
        """
        Lê dados da camada Silver.
        
        Args:
            spark: Sessão Spark
        
        Returns:
            DataFrame com dados enriquecidos
        """
        logger.info(f"Lendo dados da Silver: {self.silver_path}")
        
        try:
            df = spark.read.parquet(self.silver_path)
            
            # Filtrar por data
            if self.aggregation_level == 'hourly':
                # Última hora
                df_filtered = df.filter(
                    F.col('silver_processing_timestamp') >= 
                    F.expr("current_timestamp() - interval 1 hour")
                )
            else:
                # Dia completo
                df_filtered = df.filter(
                    F.col('date') == F.lit(self.execution_date.date())
                )
            
            count = df_filtered.count()
            self.stats['records_processed'] = count
            
            logger.info(f"Registros lidos: {count:,}")
            
            return df_filtered
        
        except Exception as e:
            logger.error(f"Erro ao ler Silver: {e}")
            raise
    
    def _calculate_hourly_kpis(self, df: DataFrame) -> DataFrame:
        """
        Calcula KPIs por hora.
        
        Args:
            df: DataFrame Silver
        
        Returns:
            DataFrame com KPIs agregados
        """
        logger.info("Calculando KPIs por hora...")
        
        # Agregações por data, hora e rota
        kpis = df.groupBy(
            'date',
            'hour',
            'route_code',
            'route_short_name',
            'route_long_name'
        ).agg(
            # Contagens
            F.countDistinct('vehicle_id').alias('total_vehicles'),
            F.count('*').alias('total_records'),
            
            # Veículos em movimento vs parados
            F.sum(F.when(F.col('is_moving'), 1).otherwise(0)).alias('vehicles_moving'),
            F.sum(F.when(~F.col('is_moving'), 1).otherwise(0)).alias('vehicles_stopped'),
            
            # Velocidades
            F.avg('speed_kmh').alias('avg_speed_kmh'),
            F.max('speed_kmh').alias('max_speed_kmh'),
            F.min('speed_kmh').alias('min_speed_kmh'),
            F.stddev('speed_kmh').alias('speed_stddev'),
            
            # Qualidade de dados
            F.avg('data_quality_score').alias('data_quality_score'),
            
            # Acessibilidade
            F.sum(F.when(F.col('has_accessibility'), 1).otherwise(0)).alias('vehicles_with_accessibility'),
            
            # Timestamps
            F.min('timestamp').alias('first_observation'),
            F.max('timestamp').alias('last_observation')
        )
        
        # Calcular métricas derivadas
        kpis = kpis.withColumn(
            'pct_moving',
            F.round(F.col('vehicles_moving') / F.col('total_vehicles') * 100, 2)
        ).withColumn(
            'pct_with_accessibility',
            F.round(F.col('vehicles_with_accessibility') / F.col('total_vehicles') * 100, 2)
        ).withColumn(
            'observations_per_vehicle',
            F.round(F.col('total_records') / F.col('total_vehicles'), 2)
        )
        
        # Adicionar timestamp de processamento
        kpis = kpis.withColumn('processing_timestamp', F.lit(datetime.now()))
        
        count = kpis.count()
        self.stats['kpis_generated'] = count
        
        logger.info(f"KPIs calculados: {count:,} registros")
        
        return kpis
    
    def _calculate_daily_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calcula métricas diárias por rota.
        
        Args:
            df: DataFrame Silver
        
        Returns:
            DataFrame com métricas diárias
        """
        logger.info("Calculando métricas diárias...")
        
        # Window para cálculo de distância
        window_vehicle = Window.partitionBy('vehicle_id', 'date').orderBy('timestamp')
        
        # Calcular distância entre pontos
        df_with_distance = df.withColumn(
            'prev_lat',
            F.lag('latitude', 1).over(window_vehicle)
        ).withColumn(
            'prev_lon',
            F.lag('longitude', 1).over(window_vehicle)
        ).withColumn(
            'prev_timestamp',
            F.lag('timestamp', 1).over(window_vehicle)
        )
        
        # Distância aproximada (Haversine simplificado)
        df_with_distance = df_with_distance.withColumn(
            'distance_km',
            F.when(
                F.col('prev_lat').isNotNull(),
                F.sqrt(
                    F.pow(F.col('latitude') - F.col('prev_lat'), 2) +
                    F.pow(F.col('longitude') - F.col('prev_lon'), 2)
                ) * 111  # Conversão aproximada para km
            ).otherwise(0)
        )
        
        # Tempo entre observações (em horas)
        df_with_distance = df_with_distance.withColumn(
            'time_diff_hours',
            F.when(
                F.col('prev_timestamp').isNotNull(),
                (F.unix_timestamp('timestamp') - F.unix_timestamp('prev_timestamp')) / 3600.0
            ).otherwise(0)
        )
        
        # Agregar por data e rota
        daily_metrics = df_with_distance.groupBy(
            'date',
            'route_code',
            'route_short_name',
            'route_long_name',
            'route_type'
        ).agg(
            # Contagens
            F.countDistinct('vehicle_id').alias('total_vehicles'),
            F.count('*').alias('total_observations'),
            
            # Velocidade
            F.avg('speed_kmh').alias('avg_speed'),
            F.max('speed_kmh').alias('max_speed'),
            
            # Distância total percorrida
            F.sum('distance_km').alias('distance_traveled_km'),
            
            # Horas de operação
            F.sum('time_diff_hours').alias('total_operation_hours'),
            
            # Horas únicas de operação (vehicle_id + hour)
            F.countDistinct(
                F.concat_ws('_', 'vehicle_id', 'hour')
            ).alias('vehicle_hour_combinations'),
            
            # Qualidade
            F.avg('data_quality_score').alias('data_quality_score'),
            
            # Acessibilidade
            F.avg(F.when(F.col('has_accessibility'), 1.0).otherwise(0.0)).alias('pct_accessibility')
        )
        
        # Calcular horas de serviço por veículo
        daily_metrics = daily_metrics.withColumn(
            'service_hours_per_vehicle',
            F.round(F.col('vehicle_hour_combinations') / F.col('total_vehicles'), 2)
        )
        
        # Adicionar timestamp
        daily_metrics = daily_metrics.withColumn(
            'processing_timestamp',
            F.lit(datetime.now())
        )
        
        count = daily_metrics.count()
        self.stats['routes_processed'] = count
        
        logger.info(f"Métricas diárias calculadas: {count:,} rotas")
        
        return daily_metrics
    
    def _calculate_headway_analysis(self, df: DataFrame) -> DataFrame:
        """
        Calcula análise de headway (intervalo entre ônibus).
        
        Args:
            df: DataFrame Silver
        
        Returns:
            DataFrame com análise de headway
        """
        logger.info("Calculando análise de headway...")
        
        # Window para calcular tempo entre veículos da mesma rota
        window_headway = Window.partitionBy('route_code', 'hour').orderBy('timestamp')
        
        # Adicionar timestamp e vehicle_id anteriores
        df_headway = df.withColumn(
            'prev_timestamp',
            F.lag('timestamp', 1).over(window_headway)
        ).withColumn(
            'prev_vehicle_id',
            F.lag('vehicle_id', 1).over(window_headway)
        )
        
        # Calcular headway apenas quando for veículo diferente
        df_headway = df_headway.withColumn(
            'headway_minutes',
            F.when(
                (F.col('prev_timestamp').isNotNull()) &
                (F.col('vehicle_id') != F.col('prev_vehicle_id')),
                (F.unix_timestamp('timestamp') - F.unix_timestamp('prev_timestamp')) / 60.0
            )
        ).filter(F.col('headway_minutes').isNotNull())
        
        # Filtrar headways anormais (< 1 min ou > 60 min)
        df_headway_clean = df_headway.filter(
            (F.col('headway_minutes') >= 1) &
            (F.col('headway_minutes') <= 60)
        )
        
        # Agregar estatísticas de headway
        headway_stats = df_headway_clean.groupBy(
            'date',
            'hour',
            'route_code',
            'route_short_name'
        ).agg(
            # Estatísticas básicas
            F.avg('headway_minutes').alias('avg_headway_minutes'),
            F.min('headway_minutes').alias('min_headway_minutes'),
            F.max('headway_minutes').alias('max_headway_minutes'),
            F.stddev('headway_minutes').alias('headway_std_dev'),
            
            # Percentis
            F.expr('percentile_approx(headway_minutes, 0.5)').alias('median_headway_minutes'),
            F.expr('percentile_approx(headway_minutes, 0.75)').alias('p75_headway_minutes'),
            F.expr('percentile_approx(headway_minutes, 0.95)').alias('p95_headway_minutes'),
            
            # Contagens
            F.count('*').alias('observations')
        )
        
        # Calcular coeficiente de variação (regularidade)
        headway_stats = headway_stats.withColumn(
            'headway_regularity_score',
            F.round(
                F.when(
                    F.col('avg_headway_minutes') > 0,
                    100 * (1 - F.col('headway_std_dev') / F.col('avg_headway_minutes'))
                ).otherwise(0),
                2
            )
        )
        
        # Classificar qualidade do serviço baseado no headway
        headway_stats = headway_stats.withColumn(
            'service_quality',
            F.when(F.col('avg_headway_minutes') <= 10, 'Excellent')
            .when(F.col('avg_headway_minutes') <= 15, 'Good')
            .when(F.col('avg_headway_minutes') <= 20, 'Fair')
            .otherwise('Poor')
        )
        
        # Adicionar timestamp
        headway_stats = headway_stats.withColumn(
            'processing_timestamp',
            F.lit(datetime.now())
        )
        
        count = headway_stats.count()
        self.stats['headway_records'] = count
        
        logger.info(f"Análise de headway concluída: {count:,} registros")
        
        return headway_stats
    
    def _write_to_gold(self, df: DataFrame, table_name: str):
        """
        Escreve dados na camada Gold.
        
        Args:
            df: DataFrame para escrever
            table_name: Nome da tabela (kpis_hourly, metrics_by_route, etc)
        """
        output_path = f"{self.gold_path}/{table_name}"
        logger.info(f"Escrevendo dados em: {output_path}")
        
        # Particionar por data para otimizar queries
        df.write \
            .mode('append') \
            .partitionBy('date') \
            .parquet(output_path)
        
        logger.info(f"Dados escritos com sucesso: {table_name}")
    
    @track_job_execution('silver_to_gold')
    def run(self) -> Dict[str, Any]:
        """
        Executa job de agregação.
        
        Returns:
            Dict com estatísticas
        """
        logger.info("=" * 80)
        logger.info("INICIANDO SILVER TO GOLD AGGREGATION JOB")
        logger.info("=" * 80)
        
        spark = None
        
        try:
            # Criar sessão Spark
            spark = self._create_spark_session()
            
            # 1. Ler dados da Silver
            df_silver = self._read_silver_data(spark)
            
            # 2. Calcular KPIs por hora
            df_kpis = self._calculate_hourly_kpis(df_silver)
            self._write_to_gold(df_kpis, 'kpis_hourly')
            
            # 3. Calcular métricas diárias (se aggregation_level == 'daily')
            if self.aggregation_level == 'daily':
                df_daily = self._calculate_daily_metrics(df_silver)
                self._write_to_gold(df_daily, 'metrics_by_route')
            
            # 4. Calcular análise de headway
            df_headway = self._calculate_headway_analysis(df_silver)
            self._write_to_gold(df_headway, 'headway_analysis')
            
            # Reportar métricas
            reporter.report_processing_stats(
                layer='gold',
                source='aggregations',
                total=self.stats['kpis_generated'] + self.stats['headway_records'],
                invalid=0,
                duplicated=0
            )
            
            # Resultado
            logger.info("\n" + "=" * 80)
            logger.info("JOB CONCLUÍDO COM SUCESSO")
            logger.info("=" * 80)
            logger.info(f"Registros processados: {self.stats['records_processed']:,}")
            logger.info(f"KPIs gerados: {self.stats['kpis_generated']:,}")
            logger.info(f"Rotas processadas: {self.stats['routes_processed']:,}")
            logger.info(f"Headway records: {self.stats['headway_records']:,}")
            
            return {
                'success': True,
                'execution_date': self.execution_date.isoformat(),
                'aggregation_level': self.aggregation_level,
                'statistics': self.stats,
                'output_path': self.gold_path,
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Erro fatal no job: {e}")
            raise
        
        finally:
            if spark:
                logger.info("Encerrando sessão Spark")
                spark.stop()


def main():
    """Entry point para execução standalone."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Silver to Gold Aggregation Job')
    parser.add_argument('--silver-path', required=True, help='Path da camada Silver')
    parser.add_argument('--gold-path', required=True, help='Path da camada Gold')
    parser.add_argument('--execution-date', help='Data de execução (YYYY-MM-DD)')
    parser.add_argument('--level', choices=['hourly', 'daily'], default='hourly',
                       help='Nível de agregação')
    
    args = parser.parse_args()
    
    # Parse execution date
    execution_date = None
    if args.execution_date:
        execution_date = datetime.strptime(args.execution_date, '%Y-%m-%d')
    
    # Executar job
    job = SilverToGoldJob(
        silver_path=args.silver_path,
        gold_path=args.gold_path,
        execution_date=execution_date,
        aggregation_level=args.level
    )
    
    result = job.run()
    
    print(f"\n{'=' * 80}")
    print("RESULTADO:")
    print(f"{'=' * 80}")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
