"""
Job Spark: Transformação Silver → Gold.

Agregações e cálculo de KPIs de negócio a partir dos dados
limpos da camada Silver para métricas na camada Gold.
"""

from datetime import datetime
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ...common.config import Config
from ...common.constants import (
    BUCKET_GOLD,
    BUCKET_SILVER,
    FORMAT_GOLD,
    PARTITION_COLS_GOLD,
)
from ...common.exceptions import ProcessingException
from ...common.logging_config import get_logger
from ...common.metrics import track_pipeline_run, update_processing_metrics
from ...common.utils import generate_uuid, get_s3_path
from ...ingestion.schema_definitions import get_gold_schema

logger = get_logger(__name__)


class SilverToGoldJob:
    """
    Job Spark para transformação Silver → Gold.
    
    Responsabilidades:
    - Ler dados da camada Silver
    - Calcular métricas horárias por linha
    - Calcular velocidades médias
    - Detectar congestionamentos
    - Calcular headway (intervalo entre veículos)
    - Gerar sumarizações diárias
    - Calcular KPIs de performance
    - Salvar agregações em Delta Lake na Gold
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        config: Optional[Config] = None,
        job_id: Optional[str] = None,
    ):
        """
        Inicializa job.

        Args:
            spark: SparkSession
            config: Configuração
            job_id: ID do job
        """
        self.config = config or Config()
        self.job_id = job_id or generate_uuid()
        
        self.spark = spark or self._create_spark_session()
        
        # Paths
        self.silver_bucket = BUCKET_SILVER
        self.silver_prefix = "vehicle_positions"
        self.gold_bucket = BUCKET_GOLD
        
        logger.info(
            "SilverToGoldJob initialized",
            extra={"job_id": self.job_id},
        )

    def _create_spark_session(self) -> SparkSession:
        """Cria SparkSession com Delta Lake."""
        return (
            SparkSession.builder
            .appName(f"SPTrans_Silver_to_Gold_{self.job_id}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", self.config.minio.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.config.minio.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.config.minio.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

    @track_pipeline_run(stage="gold", job_name="silver_to_gold")
    def run(
        self,
        date: Optional[str] = None,
        aggregation_type: str = "hourly",
    ) -> Dict[str, any]:
        """
        Executa transformação Silver → Gold.

        Args:
            date: Data a processar (YYYY-MM-DD, None = hoje)
            aggregation_type: Tipo de agregação ('hourly', 'daily', 'line_performance')

        Returns:
            Estatísticas do job

        Raises:
            ProcessingException: Se job falhar
        """
        try:
            logger.info(
                "Starting Silver to Gold transformation",
                extra={
                    "job_id": self.job_id,
                    "date": date,
                    "aggregation_type": aggregation_type,
                },
            )

            # 1. Ler dados do Silver
            silver_df = self._read_silver(date)
            input_count = silver_df.count()
            
            logger.info(f"Read {input_count} records from Silver")

            # 2. Calcular telemetria (velocidade, distância)
            telemetry_df = self._calculate_telemetry(silver_df)

            # 3. Executar agregação baseada no tipo
            if aggregation_type == "hourly":
                gold_df = self._aggregate_hourly_metrics(telemetry_df)
                gold_prefix = "hourly_metrics"
            elif aggregation_type == "daily":
                gold_df = self._aggregate_daily_summary(telemetry_df)
                gold_prefix = "daily_summary"
            elif aggregation_type == "line_performance":
                gold_df = self._aggregate_line_performance(telemetry_df)
                gold_prefix = "line_performance"
            else:
                raise ValueError(f"Unknown aggregation type: {aggregation_type}")

            # 4. Salvar na Gold
            output_count = gold_df.count()
            stats = self._save_to_gold(gold_df, gold_prefix)
            
            # 5. Atualizar métricas
            self._update_metrics(input_count, output_count)
            
            stats.update({
                "input_count": input_count,
                "output_count": output_count,
                "aggregation_type": aggregation_type,
            })
            
            logger.info(
                "Silver to Gold transformation completed",
                extra={"job_id": self.job_id, "stats": stats},
            )
            
            return stats

        except Exception as e:
            logger.error(
                f"Silver to Gold transformation failed: {e}",
                extra={"job_id": self.job_id, "error": str(e)},
                exc_info=True,
            )
            raise ProcessingException(
                transformation="silver_to_gold",
                reason=str(e)
            )

    def _read_silver(self, date: Optional[str] = None) -> DataFrame:
        """
        Lê dados da camada Silver.

        Args:
            date: Data (YYYY-MM-DD)

        Returns:
            DataFrame do Silver
        """
        input_path = get_s3_path(
            bucket=self.silver_bucket,
            prefix=self.silver_prefix,
        )

        logger.info(f"Reading from Silver: {input_path}")

        df = self.spark.read.format("delta").load(input_path)

        # Filtrar por data se especificado
        if date:
            year, month, day = date.split("-")
            df = df.filter(
                (F.col("year") == int(year))
                & (F.col("month") == int(month))
                & (F.col("day") == int(day))
            )

        # Filtrar apenas registros não-duplicados
        df = df.filter(~F.col("is_duplicate"))

        return df

    def _calculate_telemetry(self, df: DataFrame) -> DataFrame:
        """
        Calcula telemetria: velocidade, distância, heading.

        Args:
            df: DataFrame Silver

        Returns:
            DataFrame com telemetria calculada
        """
        logger.info("Calculating telemetry")

        # Window por veículo ordenado por timestamp
        window = Window.partitionBy("vehicle_id").orderBy("position_timestamp")

        # Posição anterior
        df = df.withColumn("prev_latitude", F.lag("latitude").over(window))
        df = df.withColumn("prev_longitude", F.lag("longitude").over(window))
        df = df.withColumn("prev_timestamp", F.lag("position_timestamp").over(window))

        # Calcular distância usando fórmula de Haversine
        df = df.withColumn(
            "distance_km",
            self._haversine_distance(
                F.col("prev_latitude"),
                F.col("prev_longitude"),
                F.col("latitude"),
                F.col("longitude"),
            ),
        )

        # Calcular tempo decorrido (segundos)
        df = df.withColumn(
            "time_diff_seconds",
            (F.unix_timestamp("position_timestamp") - F.unix_timestamp("prev_timestamp")),
        )

        # Calcular velocidade (km/h)
        df = df.withColumn(
            "speed_kmh",
            F.when(
                (F.col("time_diff_seconds") > 0) & (F.col("distance_km").isNotNull()),
                (F.col("distance_km") / (F.col("time_diff_seconds") / 3600))
            ).otherwise(None),
        )

        # Limpar velocidades irreais
        df = df.withColumn(
            "speed_kmh",
            F.when(
                (F.col("speed_kmh") >= 0) & (F.col("speed_kmh") <= 120),
                F.col("speed_kmh")
            ).otherwise(None),
        )

        return df

    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """
        Calcula distância Haversine entre dois pontos.

        Args:
            lat1, lon1, lat2, lon2: Coordenadas

        Returns:
            Distância em km
        """
        # Raio da Terra em km
        R = 6371.0

        # Converter para radianos
        lat1_rad = F.radians(lat1)
        lon1_rad = F.radians(lon1)
        lat2_rad = F.radians(lat2)
        lon2_rad = F.radians(lon2)

        # Diferenças
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        # Fórmula de Haversine
        a = (
            F.sin(dlat / 2) ** 2
            + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon / 2) ** 2
        )
        c = 2 * F.asin(F.sqrt(a))

        return R * c

    def _aggregate_hourly_metrics(self, df: DataFrame) -> DataFrame:
        """
        Agrega métricas horárias por linha.

        Args:
            df: DataFrame com telemetria

        Returns:
            DataFrame com métricas horárias
        """
        logger.info("Aggregating hourly metrics")

        # Timestamp da hora (truncado)
        df = df.withColumn(
            "hour_timestamp",
            F.date_trunc("hour", "position_timestamp")
        )

        # Agregação
        hourly = df.groupBy(
            "line_id",
            "line_name",
            "hour_timestamp",
            "year",
            "month",
        ).agg(
            # Métricas de frota
            F.countDistinct("vehicle_id").alias("vehicles_active"),
            F.sum(F.when(F.col("accessible"), 1).otherwise(0)).alias("vehicles_accessible"),
            
            # Métricas de velocidade
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.min("speed_kmh").alias("min_speed_kmh"),
            F.max("speed_kmh").alias("max_speed_kmh"),
            F.stddev("speed_kmh").alias("std_speed_kmh"),
            
            # Métricas de distância
            F.sum("distance_km").alias("total_distance_km"),
            
            # Qualidade
            F.avg("data_quality_score").alias("data_quality_score"),
            F.count("*").alias("total_records"),
            F.sum(F.when(F.col("data_quality_score") >= 0.9, 1).otherwise(0)).alias("valid_records"),
        )

        # Calcular distância média por veículo
        hourly = hourly.withColumn(
            "avg_distance_per_vehicle_km",
            F.col("total_distance_km") / F.col("vehicles_active")
        )

        logger.info(f"Hourly aggregation complete: {hourly.count()} records")

        return hourly

    def _aggregate_daily_summary(self, df: DataFrame) -> DataFrame:
        """
        Agrega sumário diário.

        Args:
            df: DataFrame com telemetria

        Returns:
            DataFrame com sumário diário
        """
        logger.info("Aggregating daily summary")

        # Data
        df = df.withColumn(
            "date",
            F.date_format("position_timestamp", "yyyy-MM-dd")
        )

        # Agregação diária
        daily = df.groupBy("date", "year", "month").agg(
            F.countDistinct("vehicle_id").alias("total_vehicles"),
            F.countDistinct("line_id").alias("total_lines"),
            F.count("*").alias("total_trips"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            F.sum("distance_km").alias("total_distance_km"),
            F.avg("data_quality_score").alias("data_quality_score"),
        )

        # Calcular hora de pico (mais veículos)
        hourly_counts = df.groupBy(
            "date", F.hour("position_timestamp").alias("hour")
        ).agg(
            F.countDistinct("vehicle_id").alias("vehicle_count")
        )

        # Window para pegar hora de maior contagem
        window = Window.partitionBy("date").orderBy(F.desc("vehicle_count"))
        peak_hour_df = (
            hourly_counts
            .withColumn("rank", F.row_number().over(window))
            .filter(F.col("rank") == 1)
            .select(
                "date",
                F.col("vehicle_count").alias("peak_hour_vehicles"),
                F.col("hour").alias("peak_hour"),
            )
        )

        # Join com agregação diária
        daily = daily.join(peak_hour_df, on="date", how="left")

        # Calcular uptime (simplificado)
        daily = daily.withColumn("uptime_percentage", F.lit(0.98))  # TODO: calcular real

        logger.info(f"Daily aggregation complete: {daily.count()} records")

        return daily

    def _aggregate_line_performance(self, df: DataFrame) -> DataFrame:
        """
        Agrega performance por linha.

        Args:
            df: DataFrame com telemetria

        Returns:
            DataFrame com performance por linha
        """
        logger.info("Aggregating line performance")

        # Período de análise
        period_start = df.agg(F.min("position_timestamp")).collect()[0][0]
        period_end = df.agg(F.max("position_timestamp")).collect()[0][0]

        # Agregação por linha
        performance = df.groupBy("line_id", "line_name", "year", "month").agg(
            # KPIs de velocidade
            F.avg("speed_kmh").alias("avg_speed_kmh"),
            
            # KPIs de frota
            F.avg(F.countDistinct("vehicle_id")).alias("avg_vehicles_operating"),
            
            # Qualidade
            F.avg("data_quality_score").alias("data_quality_score"),
        )

        # Adicionar informações do período
        performance = (
            performance
            .withColumn("analysis_period", F.lit("daily"))
            .withColumn("period_start", F.lit(period_start))
            .withColumn("period_end", F.lit(period_end))
        )

        # Calcular índice de congestionamento (simplificado)
        # 0-100, onde 100 = muito congestionado
        performance = performance.withColumn(
            "congestion_index",
            F.when(F.col("avg_speed_kmh") < 10, 80)
            .when(F.col("avg_speed_kmh") < 15, 50)
            .when(F.col("avg_speed_kmh") < 20, 30)
            .otherwise(10)
        )

        # Reliability score (simplificado)
        performance = performance.withColumn(
            "reliability_score",
            F.col("data_quality_score") * 100
        )

        # Headway e utilização (placeholder - requer lógica mais complexa)
        performance = (
            performance
            .withColumn("avg_headway_minutes", F.lit(None).cast("double"))
            .withColumn("std_headway_minutes", F.lit(None).cast("double"))
            .withColumn("utilization_rate", F.lit(None).cast("double"))
        )

        logger.info(f"Line performance aggregation complete: {performance.count()} records")

        return performance

    def _save_to_gold(self, df: DataFrame, prefix: str) -> Dict[str, any]:
        """
        Salva DataFrame na Gold usando Delta Lake.

        Args:
            df: DataFrame a salvar
            prefix: Prefixo (subdiretório)

        Returns:
            Estatísticas
        """
        output_path = get_s3_path(
            bucket=self.gold_bucket,
            prefix=prefix,
        )

        logger.info(f"Saving to Gold (Delta Lake): {output_path}")

        # Salvar com merge
        (
            df.write
            .mode("append")
            .partitionBy(*PARTITION_COLS_GOLD)
            .format("delta")
            .option("mergeSchema", "true")
            .save(output_path)
        )

        stats = {
            "output_path": output_path,
            "record_count": df.count(),
            "format": "delta",
        }

        logger.info("Saved to Gold successfully", extra=stats)

        return stats

    def _update_metrics(self, input_count: int, output_count: int) -> None:
        """Atualiza métricas."""
        update_processing_metrics(
            stage="gold",
            input_count=input_count,
            output_count=output_count,
            bytes_written_count=0,
        )


# Função standalone
def run_silver_to_gold_job(
    date: Optional[str] = None,
    aggregation_type: str = "hourly",
    **kwargs
) -> Dict[str, any]:
    """Função standalone para Airflow."""
    job = SilverToGoldJob()
    stats = job.run(date=date, aggregation_type=aggregation_type)
    return stats


# Exemplo de uso
if __name__ == "__main__":
    from ...common.logging_config import setup_logging

    setup_logging(log_level="INFO", log_format="console")

    job = SilverToGoldJob()
    
    # Executar agregação horária
    stats = job.run(aggregation_type="hourly")

    print(f"\nJob completed!")
    print(f"Input: {stats['input_count']} records")
    print(f"Output: {stats['output_count']} aggregated records")
    print(f"Type: {stats['aggregation_type']}")
