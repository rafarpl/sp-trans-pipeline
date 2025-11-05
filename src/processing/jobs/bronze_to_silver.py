"""
Job Spark: Transformação Bronze → Silver.

Limpeza, normalização e enriquecimento de dados da camada Bronze
para a camada Silver, aplicando regras de qualidade e transformações.
"""

from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ...common.config import Config
from ...common.constants import (
    BUCKET_BRONZE,
    BUCKET_SILVER,
    FORMAT_SILVER,
    PARTITION_COLS_SILVER,
    PREFIX_API_POSITIONS,
    PipelineStage,
)
from ...common.exceptions import ProcessingException
from ...common.logging_config import get_logger
from ...common.metrics import (
    records_processed_total,
    track_pipeline_run,
    update_dq_metrics,
    update_processing_metrics,
)
from ...common.utils import generate_uuid, get_s3_path
from ...common.validators import (
    run_all_validations,
    validate_coordinate_range,
    validate_required_fields,
)
from ...ingestion.schema_definitions import get_silver_schema

logger = get_logger(__name__)


class BronzeToSilverJob:
    """
    Job Spark para transformação Bronze → Silver.
    
    Responsabilidades:
    - Ler dados brutos da camada Bronze
    - Explode arrays aninhados (linhas → veículos)
    - Normalizar estrutura de dados
    - Aplicar limpeza e validações
    - Detectar e marcar duplicatas
    - Calcular score de qualidade
    - Salvar em formato Delta Lake na Silver
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
        self.bronze_bucket = BUCKET_BRONZE
        self.bronze_prefix = PREFIX_API_POSITIONS
        self.silver_bucket = BUCKET_SILVER
        self.silver_prefix = "vehicle_positions"
        
        logger.info(
            "BronzeToSilverJob initialized",
            extra={"job_id": self.job_id},
        )

    def _create_spark_session(self) -> SparkSession:
        """Cria SparkSession com suporte a Delta Lake."""
        return (
            SparkSession.builder
            .appName(f"SPTrans_Bronze_to_Silver_{self.job_id}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", self.config.minio.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.config.minio.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.config.minio.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

    @track_pipeline_run(stage="silver", job_name="bronze_to_silver")
    def run(
        self,
        date: Optional[str] = None,
        hour: Optional[int] = None,
    ) -> Dict[str, any]:
        """
        Executa transformação Bronze → Silver.

        Args:
            date: Data a processar (YYYY-MM-DD, None = hoje)
            hour: Hora a processar (None = última hora)

        Returns:
            Estatísticas do job

        Raises:
            ProcessingException: Se job falhar
        """
        try:
            logger.info(
                "Starting Bronze to Silver transformation",
                extra={"job_id": self.job_id, "date": date, "hour": hour},
            )

            # 1. Ler dados do Bronze
            bronze_df = self._read_bronze(date, hour)
            input_count = bronze_df.count()
            
            logger.info(f"Read {input_count} records from Bronze")

            # 2. Explode e normalizar
            normalized_df = self._explode_and_normalize(bronze_df)
            
            # 3. Limpeza e transformações
            cleaned_df = self._clean_and_transform(normalized_df)
            
            # 4. Detectar duplicatas
            deduped_df = self._detect_duplicates(cleaned_df)
            
            # 5. Calcular data quality score
            scored_df = self._calculate_dq_score(deduped_df)
            
            # 6. Validações
            dq_results = self._run_validations(scored_df)
            
            # 7. Salvar na Silver
            output_count = scored_df.count()
            stats = self._save_to_silver(scored_df)
            
            # 8. Atualizar métricas
            self._update_metrics(input_count, output_count, dq_results)
            
            stats.update({
                "input_count": input_count,
                "output_count": output_count,
                "dq_results": dq_results,
            })
            
            logger.info(
                "Bronze to Silver transformation completed",
                extra={"job_id": self.job_id, "stats": stats},
            )
            
            return stats

        except Exception as e:
            logger.error(
                f"Bronze to Silver transformation failed: {e}",
                extra={"job_id": self.job_id, "error": str(e)},
                exc_info=True,
            )
            raise ProcessingException(
                transformation="bronze_to_silver",
                reason=str(e)
            )

    def _read_bronze(
        self, date: Optional[str] = None, hour: Optional[int] = None
    ) -> DataFrame:
        """
        Lê dados da camada Bronze.

        Args:
            date: Data (YYYY-MM-DD)
            hour: Hora

        Returns:
            DataFrame do Bronze
        """
        input_path = get_s3_path(
            bucket=self.bronze_bucket,
            prefix=self.bronze_prefix,
        )

        logger.info(f"Reading from Bronze: {input_path}")

        df = self.spark.read.parquet(input_path)

        # Filtrar por data/hora se especificado
        if date:
            df = df.filter(F.col("ingestion_date") == date)
        if hour is not None:
            df = df.filter(F.col("hour") == hour)

        return df

    def _explode_and_normalize(self, df: DataFrame) -> DataFrame:
        """
        Explode arrays aninhados e normaliza estrutura.

        Estrutura Bronze: hr → l (array) → vs (array)
        Estrutura Silver: uma linha por veículo

        Args:
            df: DataFrame Bronze

        Returns:
            DataFrame normalizado
        """
        logger.info("Exploding and normalizing nested structures")

        # Explode array de linhas
        df_lines = df.withColumn("line", F.explode("l"))

        # Explode array de veículos
        df_vehicles = df_lines.withColumn("vehicle", F.explode("line.vs"))

        # Extrair campos
        normalized = df_vehicles.select(
            # Identificadores
            F.col("vehicle.p").cast("string").alias("vehicle_id"),
            F.col("line.cl").alias("line_id"),
            F.col("line.sl").alias("line_direction"),
            
            # Informações da linha
            F.col("line.lt0").alias("line_name"),
            F.col("line.lt1").alias("line_destination"),
            
            # Posição
            F.col("vehicle.py").alias("latitude"),
            F.col("vehicle.px").alias("longitude"),
            
            # Características
            F.col("vehicle.a").alias("accessible"),
            
            # Timestamps
            F.to_timestamp("vehicle.ta", "yyyy-MM-dd HH:mm:ss").alias("position_timestamp"),
            F.col("ingestion_timestamp").alias("processed_timestamp"),
            
            # Particionamento
            F.col("year"),
            F.col("month"),
            F.col("day"),
        )

        logger.info(
            "Normalization complete",
            extra={"normalized_count": normalized.count()},
        )

        return normalized

    def _clean_and_transform(self, df: DataFrame) -> DataFrame:
        """
        Aplica limpeza e transformações.

        Args:
            df: DataFrame normalizado

        Returns:
            DataFrame limpo
        """
        logger.info("Applying cleaning and transformations")

        # 1. Remover nulos em campos críticos
        df = df.filter(
            F.col("vehicle_id").isNotNull()
            & F.col("line_id").isNotNull()
            & F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
            & F.col("position_timestamp").isNotNull()
        )

        # 2. Filtrar coordenadas válidas (São Paulo bounds)
        df = df.filter(
            F.col("latitude").between(-23.9, -23.3)
            & F.col("longitude").between(-46.9, -46.3)
        )

        # 3. Limpar strings
        df = (
            df.withColumn("line_name", F.trim(F.col("line_name")))
            .withColumn("line_destination", F.trim(F.col("line_destination")))
        )

        # 4. Garantir tipos corretos
        df = (
            df.withColumn("line_id", F.col("line_id").cast("int"))
            .withColumn("line_direction", F.col("line_direction").cast("int"))
            .withColumn("accessible", F.col("accessible").cast("boolean"))
        )

        logger.info("Cleaning complete")

        return df

    def _detect_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Detecta duplicatas baseado em (vehicle_id, position_timestamp).

        Args:
            df: DataFrame limpo

        Returns:
            DataFrame com flag de duplicata
        """
        logger.info("Detecting duplicates")

        # Window para detectar duplicatas
        window = Window.partitionBy("vehicle_id", "position_timestamp").orderBy("processed_timestamp")

        df = df.withColumn("_row_num", F.row_number().over(window))
        
        # Marcar duplicatas (keep first)
        df = df.withColumn(
            "is_duplicate",
            F.when(F.col("_row_num") > 1, True).otherwise(False)
        )

        df = df.drop("_row_num")

        duplicate_count = df.filter(F.col("is_duplicate")).count()
        logger.info(f"Detected {duplicate_count} duplicates")

        return df

    def _calculate_dq_score(self, df: DataFrame) -> DataFrame:
        """
        Calcula score de qualidade dos dados (0-1).

        Args:
            df: DataFrame

        Returns:
            DataFrame com DQ score
        """
        logger.info("Calculating data quality scores")

        # Score baseado em completude e validade
        df = df.withColumn(
            "data_quality_score",
            # Penalidades
            F.when(F.col("is_duplicate"), 0.5)  # Duplicata = -50%
            .when(F.col("line_name").isNull(), 0.8)  # Sem nome = -20%
            .when(F.col("line_destination").isNull(), 0.9)  # Sem destino = -10%
            .otherwise(1.0)  # Perfeito = 100%
        )

        return df

    def _run_validations(self, df: DataFrame) -> Dict[str, any]:
        """
        Executa validações de data quality.

        Args:
            df: DataFrame a validar

        Returns:
            Resultados das validações
        """
        logger.info("Running data quality validations")

        try:
            dq_results = run_all_validations(
                df,
                stage="silver",
                required_columns=["vehicle_id", "line_id", "latitude", "longitude"],
                key_columns=["vehicle_id", "position_timestamp"],
            )
            return dq_results

        except Exception as e:
            logger.warning(
                f"Validation error (non-blocking): {e}",
                extra={"error": str(e)},
            )
            return {"overall_status": "warning", "error": str(e)}

    def _save_to_silver(self, df: DataFrame) -> Dict[str, any]:
        """
        Salva DataFrame na Silver usando Delta Lake.

        Args:
            df: DataFrame a salvar

        Returns:
            Estatísticas
        """
        output_path = get_s3_path(
            bucket=self.silver_bucket,
            prefix=self.silver_prefix,
        )

        logger.info(f"Saving to Silver (Delta Lake): {output_path}")

        # Salvar como Delta com merge
        (
            df.write
            .mode("append")
            .partitionBy(*PARTITION_COLS_SILVER)
            .format("delta")
            .option("mergeSchema", "true")
            .save(output_path)
        )

        stats = {
            "output_path": output_path,
            "record_count": df.count(),
            "format": "delta",
            "partition_cols": PARTITION_COLS_SILVER,
        }

        logger.info("Saved to Silver successfully", extra=stats)

        return stats

    def _update_metrics(
        self, input_count: int, output_count: int, dq_results: Dict
    ) -> None:
        """Atualiza métricas."""
        update_processing_metrics(
            stage="silver",
            input_count=input_count,
            output_count=output_count,
            bytes_written_count=0,
        )

        update_dq_metrics(stage="silver", dq_results=dq_results)


# Função standalone
def run_bronze_to_silver_job(
    date: Optional[str] = None,
    hour: Optional[int] = None,
    **kwargs
) -> Dict[str, any]:
    """Função standalone para Airflow."""
    job = BronzeToSilverJob()
    stats = job.run(date=date, hour=hour)
    return stats


# Exemplo de uso
if __name__ == "__main__":
    from ...common.logging_config import setup_logging

    setup_logging(log_level="INFO", log_format="console")

    job = BronzeToSilverJob()
    stats = job.run()

    print(f"\nJob completed!")
    print(f"Input: {stats['input_count']} records")
    print(f"Output: {stats['output_count']} records")
    print(f"DQ Status: {stats['dq_results']['overall_status']}")
