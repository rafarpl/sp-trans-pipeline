"""
Bronze to Silver Transformation Job
====================================
Job Spark para transformação de dados Bronze → Silver.

Operações:
- Limpeza e validação de dados
- Deduplicação
- Enriquecimento com GTFS
- Aplicação de regras de qualidade
- Cálculo de campos derivados
"""

from typing import Dict, Any, Optional
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

from src.common.logging_config import get_logger
from src.common.config import Config
from src.common.validators import (
    CoordinateValidator,
    TimestampValidator,
    VehicleValidator,
    DataQualityValidator
)
from src.common.metrics import track_job_execution, reporter

logger = get_logger(__name__)


class BronzeToSilverJob:
    """Job de transformação Bronze → Silver."""
    
    def __init__(self, 
                 bronze_path: str,
                 silver_path: str,
                 execution_date: Optional[datetime] = None,
                 lookback_hours: int = 24):
        """
        Inicializa job.
        
        Args:
            bronze_path: Caminho da camada Bronze
            silver_path: Caminho da camada Silver
            execution_date: Data de execução
            lookback_hours: Horas para trás para processar
        """
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.execution_date = execution_date or datetime.now()
        self.lookback_hours = lookback_hours
        
        self.config = Config()
        
        # Estatísticas
        self.stats = {
            'initial_count': 0,
            'after_dedup': 0,
            'after_validation': 0,
            'after_enrichment': 0,
            'duplicates_removed': 0,
            'invalid_removed': 0,
            'quality_score': 0.0
        }
        
        logger.info(f"BronzeToSilverJob inicializado: "
                   f"bronze={bronze_path}, silver={silver_path}, "
                   f"lookback={lookback_hours}h")
    
    def _create_spark_session(self) -> SparkSession:
        """Cria sessão Spark."""
        logger.info("Criando sessão Spark")
        
        spark = SparkSession.builder \
            .appName("Bronze_to_Silver_Transformation") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.endpoint", self.config.MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", self.config.MINIO_ROOT_USER) \
            .config("spark.hadoop.fs.s3a.secret.key", self.config.MINIO_ROOT_PASSWORD) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    def _read_bronze_data(self, spark: SparkSession) -> DataFrame:
        """
        Lê dados da camada Bronze.
        
        Args:
            spark: Sessão Spark
        
        Returns:
            DataFrame com dados brutos
        """
        logger.info(f"Lendo dados da Bronze: {self.bronze_path}")
        
        # Calcular intervalo de tempo
        start_time = self.execution_date - timedelta(hours=self.lookback_hours)
        
        try:
            df = spark.read.parquet(self.bronze_path)
            
            # Filtrar por intervalo de tempo
            df_filtered = df.filter(
                (F.col('ingestion_timestamp') >= F.lit(start_time)) &
                (F.col('ingestion_timestamp') <= F.lit(self.execution_date))
            )
            
            count = df_filtered.count()
            self.stats['initial_count'] = count
            
            logger.info(f"Registros lidos: {count:,}")
            
            return df_filtered
        
        except Exception as e:
            logger.error(f"Erro ao ler Bronze: {e}")
            raise
    
    def _deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicatas.
        
        Args:
            df: DataFrame com dados
        
        Returns:
            DataFrame sem duplicatas
        """
        logger.info("Removendo duplicatas...")
        
        initial_count = df.count()
        
        # Criar window para selecionar registro mais recente por vehicle_id + timestamp
        window_spec = Window.partitionBy('vehicle_id', 'timestamp') \
            .orderBy(F.col('ingestion_timestamp').desc())
        
        # Adicionar rank
        df_ranked = df.withColumn('rank', F.row_number().over(window_spec))
        
        # Manter apenas rank = 1 (mais recente)
        df_dedup = df_ranked.filter(F.col('rank') == 1).drop('rank')
        
        final_count = df_dedup.count()
        duplicates = initial_count - final_count
        
        self.stats['after_dedup'] = final_count
        self.stats['duplicates_removed'] = duplicates
        
        logger.info(f"Duplicatas removidas: {duplicates:,} ({duplicates/initial_count*100:.2f}%)")
        
        return df_dedup
    
    def _validate_data(self, df: DataFrame) -> DataFrame:
        """
        Valida dados e adiciona flags de qualidade.
        
        Args:
            df: DataFrame para validar
        
        Returns:
            DataFrame com flags de validação
        """
        logger.info("Validando dados...")
        
        # 1. Validar coordenadas
        df_validated = CoordinateValidator.validate_coordinates_df(
            df,
            lat_col='latitude',
            lon_col='longitude',
            strict=True
        )
        
        # 2. Validar timestamps
        df_validated = TimestampValidator.validate_timestamps_df(
            df_validated,
            ts_col='timestamp',
            max_future_seconds=300,
            max_past_hours=self.lookback_hours
        )
        
        # 3. Validar vehicle IDs
        df_validated = VehicleValidator.validate_vehicles_df(
            df_validated,
            vehicle_col='vehicle_id'
        )
        
        # 4. Criar flag de qualidade geral
        df_validated = df_validated.withColumn(
            'is_valid',
            F.col('is_valid_coordinate') &
            F.col('is_valid_timestamp') &
            F.col('is_valid_vehicle_id')
        )
        
        # Estatísticas de validação
        total = df_validated.count()
        valid = df_validated.filter(F.col('is_valid')).count()
        invalid = total - valid
        
        self.stats['after_validation'] = valid
        self.stats['invalid_removed'] = invalid
        self.stats['quality_score'] = (valid / total * 100) if total > 0 else 0
        
        logger.info(f"Registros válidos: {valid:,} ({self.stats['quality_score']:.2f}%)")
        logger.info(f"Registros inválidos: {invalid:,}")
        
        # Filtrar apenas válidos
        df_clean = df_validated.filter(F.col('is_valid'))
        
        return df_clean
    
    def _calculate_derived_fields(self, df: DataFrame) -> DataFrame:
        """
        Calcula campos derivados.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame com campos calculados
        """
        logger.info("Calculando campos derivados...")
        
        # 1. Extrair data/hora separadamente
        df = df.withColumn('date', F.to_date('timestamp')) \
               .withColumn('hour', F.hour('timestamp')) \
               .withColumn('day_of_week', F.dayofweek('timestamp')) \
               .withColumn('is_weekend', F.dayofweek('timestamp').isin([1, 7]))
        
        # 2. Calcular velocidade (se tiver dados anteriores)
        window_speed = Window.partitionBy('vehicle_id').orderBy('timestamp')
        
        # Lag de coordenadas e timestamp
        df = df.withColumn('prev_lat', F.lag('latitude', 1).over(window_speed)) \
               .withColumn('prev_lon', F.lag('longitude', 1).over(window_speed)) \
               .withColumn('prev_timestamp', F.lag('timestamp', 1).over(window_speed))
        
        # Calcular distância usando fórmula de Haversine (simplificada)
        # Note: Para produção, usar biblioteca geoespacial completa
        df = df.withColumn(
            'distance_km',
            F.when(
                F.col('prev_lat').isNotNull(),
                F.sqrt(
                    F.pow(F.col('latitude') - F.col('prev_lat'), 2) +
                    F.pow(F.col('longitude') - F.col('prev_lon'), 2)
                ) * 111  # Conversão aproximada para km
            ).otherwise(0)
        )
        
        # Calcular tempo decorrido em segundos
        df = df.withColumn(
            'time_diff_seconds',
            F.when(
                F.col('prev_timestamp').isNotNull(),
                (F.unix_timestamp('timestamp') - F.unix_timestamp('prev_timestamp'))
            ).otherwise(0)
        )
        
        # Calcular velocidade (km/h)
        df = df.withColumn(
            'speed_kmh',
            F.when(
                (F.col('time_diff_seconds') > 0) & (F.col('distance_km') > 0),
                (F.col('distance_km') / F.col('time_diff_seconds')) * 3600
            ).otherwise(0)
        )
        
        # Limitar velocidade máxima (outliers)
        df = df.withColumn(
            'speed_kmh',
            F.when(F.col('speed_kmh') > 120, 0).otherwise(F.col('speed_kmh'))
        )
        
        # 3. Status do veículo (parado/movimento)
        df = df.withColumn(
            'is_moving',
            F.col('speed_kmh') > 5  # Considerado parado se < 5 km/h
        )
        
        # Remover colunas temporárias
        df = df.drop('prev_lat', 'prev_lon', 'prev_timestamp', 'distance_km', 'time_diff_seconds')
        
        logger.info("Campos derivados calculados")
        
        return df
    
    def _enrich_with_gtfs(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        """
        Enriquece com dados GTFS.
        
        Args:
            df: DataFrame com posições
            spark: Sessão Spark
        
        Returns:
            DataFrame enriquecido
        """
        logger.info("Enriquecendo com dados GTFS...")
        
        try:
            # Ler dados GTFS mais recentes
            routes_path = f"{self.bronze_path.replace('api_positions', 'gtfs/routes')}"
            
            routes_df = spark.read.parquet(routes_path)
            
            # Selecionar versão mais recente do GTFS
            latest_date = routes_df.agg(F.max('ingestion_date')).collect()[0][0]
            routes_latest = routes_df.filter(F.col('ingestion_date') == latest_date)
            
            # Join com rotas
            df_enriched = df.join(
                routes_latest.select(
                    F.col('route_id').alias('route_id_gtfs'),
                    'route_short_name',
                    'route_long_name',
                    'route_type'
                ),
                df.route_code == routes_latest.route_id,
                'left'
            )
            
            # Adicionar flag de enriquecimento
            df_enriched = df_enriched.withColumn(
                'has_route_info',
                F.col('route_short_name').isNotNull()
            )
            
            enriched_count = df_enriched.filter(F.col('has_route_info')).count()
            total_count = df_enriched.count()
            enrichment_rate = (enriched_count / total_count * 100) if total_count > 0 else 0
            
            logger.info(f"Registros enriquecidos: {enriched_count:,} ({enrichment_rate:.2f}%)")
            
            self.stats['after_enrichment'] = df_enriched.count()
            
            return df_enriched
        
        except Exception as e:
            logger.warning(f"Erro ao enriquecer com GTFS: {e}")
            logger.info("Continuando sem enriquecimento GTFS")
            return df
    
    def _add_silver_metadata(self, df: DataFrame) -> DataFrame:
        """
        Adiciona metadados da camada Silver.
        
        Args:
            df: DataFrame
        
        Returns:
            DataFrame com metadados
        """
        return df \
            .withColumn('silver_processing_timestamp', F.lit(datetime.now())) \
            .withColumn('silver_processing_date', F.lit(self.execution_date.date())) \
            .withColumn('data_quality_score', F.lit(self.stats['quality_score'])) \
            .withColumn('layer', F.lit('silver'))
    
    def _write_to_silver(self, df: DataFrame):
        """
        Escreve dados na camada Silver.
        
        Args:
            df: DataFrame para escrever
        """
        logger.info(f"Escrevendo dados na Silver: {self.silver_path}")
        
        # Particionar por data para facilitar queries
        df.write \
            .mode('append') \
            .partitionBy('silver_processing_date') \
            .parquet(self.silver_path)
        
        logger.info("Dados escritos com sucesso na Silver")
    
    @track_job_execution('bronze_to_silver')
    def run(self) -> Dict[str, Any]:
        """
        Executa job de transformação.
        
        Returns:
            Dict com estatísticas
        """
        logger.info("=" * 80)
        logger.info("INICIANDO BRONZE TO SILVER TRANSFORMATION JOB")
        logger.info("=" * 80)
        
        spark = None
        
        try:
            # Criar sessão Spark
            spark = self._create_spark_session()
            
            # 1. Ler dados da Bronze
            df_bronze = self._read_bronze_data(spark)
            
            # 2. Deduplicar
            df_dedup = self._deduplicate(df_bronze)
            
            # 3. Validar
            df_validated = self._validate_data(df_dedup)
            
            # 4. Calcular campos derivados
            df_derived = self._calculate_derived_fields(df_validated)
            
            # 5. Enriquecer com GTFS
            df_enriched = self._enrich_with_gtfs(df_derived, spark)
            
            # 6. Adicionar metadados Silver
            df_silver = self._add_silver_metadata(df_enriched)
            
            # 7. Escrever na Silver
            self._write_to_silver(df_silver)
            
            # Reportar métricas
            reporter.report_processing_stats(
                layer='silver',
                source='api_positions',
                total=self.stats['after_enrichment'],
                invalid=self.stats['invalid_removed'],
                duplicated=self.stats['duplicates_removed']
            )
            
            reporter.report_data_quality(
                layer='silver',
                quality_metrics={
                    'completeness': self.stats['quality_score'],
                    'accuracy': self.stats['quality_score'],
                    'validity': self.stats['quality_score']
                }
            )
            
            # Resultado
            logger.info("\n" + "=" * 80)
            logger.info("JOB CONCLUÍDO COM SUCESSO")
            logger.info("=" * 80)
            logger.info(f"Registros iniciais: {self.stats['initial_count']:,}")
            logger.info(f"Após deduplicação: {self.stats['after_dedup']:,}")
            logger.info(f"Após validação: {self.stats['after_validation']:,}")
            logger.info(f"Final (Silver): {self.stats['after_enrichment']:,}")
            logger.info(f"Score de qualidade: {self.stats['quality_score']:.2f}%")
            
            return {
                'success': True,
                'execution_date': self.execution_date.isoformat(),
                'statistics': self.stats,
                'output_path': self.silver_path,
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
    
    parser = argparse.ArgumentParser(description='Bronze to Silver Transformation Job')
    parser.add_argument('--bronze-path', required=True, help='Path da camada Bronze')
    parser.add_argument('--silver-path', required=True, help='Path da camada Silver')
    parser.add_argument('--execution-date', help='Data de execução (YYYY-MM-DD)')
    parser.add_argument('--lookback-hours', type=int, default=24, help='Horas para trás')
    
    args = parser.parse_args()
    
    # Parse execution date
    execution_date = None
    if args.execution_date:
        execution_date = datetime.strptime(args.execution_date, '%Y-%m-%d')
    
    # Executar job
    job = BronzeToSilverJob(
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        execution_date=execution_date,
        lookback_hours=args.lookback_hours
    )
    
    result = job.run()
    
    print(f"\n{'=' * 80}")
    print("RESULTADO:")
    print(f"{'=' * 80}")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
