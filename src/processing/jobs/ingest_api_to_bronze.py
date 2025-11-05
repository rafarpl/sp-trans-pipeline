"""
Job Spark: Ingestão de dados da API SPTrans para camada Bronze.

Consome dados da API Olho Vivo, valida, particiona e armazena
no Data Lake (MinIO/S3) em formato Parquet na camada Bronze.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from ...common.config import Config
from ...common.constants import (
    BUCKET_BRONZE,
    FORMAT_BRONZE,
    PARTITION_COLS_BRONZE,
    PREFIX_API_POSITIONS,
    PipelineStage,
)
from ...common.exceptions import (
    APIException,
    ProcessingException,
    StorageWriteException,
)
from ...common.logging_config import get_logger
from ...common.metrics import (
    records_processed_total,
    track_pipeline_run,
    update_processing_metrics,
)
from ...common.utils import generate_uuid, get_s3_path
from ...ingestion.schema_definitions import get_bronze_schema
from ...ingestion.sptrans_api_client import SPTransAPIClient

logger = get_logger(__name__)


class APIToBronzeJob:
    """
    Job Spark para ingestão de dados da API para Bronze.
    
    Responsabilidades:
    - Chamar API SPTrans para obter posições de veículos
    - Converter resposta JSON para DataFrame Spark
    - Adicionar metadados de ingestão
    - Particionar por data/hora
    - Salvar em formato Parquet no MinIO/S3
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
            spark: SparkSession (cria nova se None)
            config: Configuração (usa Config() se None)
            job_id: ID do job (gera UUID se None)
        """
        self.config = config or Config()
        self.job_id = job_id or generate_uuid()
        
        # SparkSession
        self.spark = spark or self._create_spark_session()
        
        # Cliente API
        self.api_client = SPTransAPIClient(config=self.config)
        
        # Paths
        self.bronze_bucket = BUCKET_BRONZE
        self.bronze_prefix = PREFIX_API_POSITIONS
        
        logger.info(
            "APIToBronzeJob initialized",
            extra={
                "job_id": self.job_id,
                "bronze_bucket": self.bronze_bucket,
                "bronze_prefix": self.bronze_prefix,
            },
        )

    def _create_spark_session(self) -> SparkSession:
        """
        Cria SparkSession configurada.

        Returns:
            SparkSession
        """
        return (
            SparkSession.builder
            .appName(f"SPTrans_API_to_Bronze_{self.job_id}")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.hadoop.fs.s3a.endpoint", self.config.minio.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.config.minio.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.config.minio.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

    @track_pipeline_run(stage="bronze", job_name="api_to_bronze")
    def run(
        self, 
        line_id: Optional[int] = None,
        save_mode: str = "append"
    ) -> Dict[str, any]:
        """
        Executa job de ingestão.

        Args:
            line_id: ID da linha específica (None = todas)
            save_mode: Modo de salvamento ('append', 'overwrite')

        Returns:
            Dicionário com estatísticas do job

        Raises:
            ProcessingException: Se job falhar
        """
        try:
            logger.info(
                "Starting API to Bronze ingestion",
                extra={"job_id": self.job_id, "line_id": line_id},
            )

            # 1. Buscar dados da API
            api_data = self._fetch_api_data(line_id)
            
            # 2. Converter para DataFrame
            df = self._convert_to_dataframe(api_data)
            
            # 3. Adicionar metadados
            df = self._add_metadata(df)
            
            # 4. Validar schema
            self._validate_schema(df)
            
            # 5. Salvar no Bronze
            stats = self._save_to_bronze(df, save_mode)
            
            # 6. Atualizar métricas
            self._update_metrics(stats)
            
            logger.info(
                "API to Bronze ingestion completed successfully",
                extra={"job_id": self.job_id, "stats": stats},
            )
            
            return stats

        except APIException as e:
            logger.error(
                f"API error in Bronze ingestion: {e}",
                extra={"job_id": self.job_id, "error": str(e)},
            )
            raise ProcessingException(
                transformation="api_to_bronze",
                reason=f"API error: {str(e)}"
            )
        except Exception as e:
            logger.error(
                f"Unexpected error in Bronze ingestion: {e}",
                extra={"job_id": self.job_id, "error": str(e)},
                exc_info=True,
            )
            raise ProcessingException(
                transformation="api_to_bronze",
                reason=str(e)
            )

    def _fetch_api_data(self, line_id: Optional[int] = None) -> Dict:
        """
        Busca dados da API SPTrans.

        Args:
            line_id: ID da linha (opcional)

        Returns:
            Dicionário com dados da API

        Raises:
            APIException: Se chamada falhar
        """
        logger.info(
            "Fetching data from SPTrans API",
            extra={"line_id": line_id},
        )

        with self.api_client:
            data = self.api_client.get_positions(line_id=line_id)

        # Validar resposta
        if not data or "l" not in data:
            raise APIException(
                message="Invalid API response",
                error_code="INVALID_API_RESPONSE",
                details={"data": str(data)[:200]},
            )

        logger.info(
            "Data fetched successfully",
            extra={
                "lines_count": len(data.get("l", [])),
                "timestamp": data.get("hr"),
            },
        )

        return data

    def _convert_to_dataframe(self, api_data: Dict) -> DataFrame:
        """
        Converte resposta JSON da API para DataFrame Spark.

        Args:
            api_data: Dados da API

        Returns:
            DataFrame Spark
        """
        logger.info("Converting API data to DataFrame")

        # Converter para JSON string
        json_str = json.dumps(api_data)

        # Criar DataFrame com schema
        schema = get_bronze_schema("positions")
        
        # Criar RDD com o JSON
        rdd = self.spark.sparkContext.parallelize([json_str])
        
        # Ler como DataFrame
        df = self.spark.read.json(rdd, schema=schema)

        logger.info(
            "DataFrame created",
            extra={"row_count": df.count(), "columns": len(df.columns)},
        )

        return df

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Adiciona colunas de metadados de ingestão.

        Args:
            df: DataFrame original

        Returns:
            DataFrame com metadados
        """
        logger.info("Adding ingestion metadata")

        current_timestamp = datetime.now()

        df = (
            df.withColumn("ingestion_timestamp", F.lit(current_timestamp))
            .withColumn("ingestion_date", F.lit(current_timestamp.strftime("%Y-%m-%d")))
            .withColumn("year", F.lit(current_timestamp.year))
            .withColumn("month", F.lit(current_timestamp.month))
            .withColumn("day", F.lit(current_timestamp.day))
            .withColumn("hour", F.lit(current_timestamp.hour))
        )

        return df

    def _validate_schema(self, df: DataFrame) -> None:
        """
        Valida schema do DataFrame.

        Args:
            df: DataFrame a validar

        Raises:
            ProcessingException: Se schema inválido
        """
        logger.info("Validating DataFrame schema")

        expected_schema = get_bronze_schema("positions")
        expected_cols = {field.name for field in expected_schema.fields}
        actual_cols = set(df.columns)

        missing_cols = expected_cols - actual_cols
        if missing_cols:
            raise ProcessingException(
                transformation="schema_validation",
                reason=f"Missing columns: {missing_cols}"
            )

        logger.info("Schema validation passed")

    def _save_to_bronze(
        self, df: DataFrame, save_mode: str = "append"
    ) -> Dict[str, any]:
        """
        Salva DataFrame no Bronze (MinIO/S3).

        Args:
            df: DataFrame a salvar
            save_mode: Modo de salvamento

        Returns:
            Estatísticas do salvamento

        Raises:
            StorageWriteException: Se falhar ao salvar
        """
        try:
            output_path = get_s3_path(
                bucket=self.bronze_bucket,
                prefix=self.bronze_prefix,
            )

            logger.info(
                f"Saving to Bronze: {output_path}",
                extra={
                    "save_mode": save_mode,
                    "partition_cols": PARTITION_COLS_BRONZE,
                },
            )

            # Salvar particionado
            (
                df.write
                .mode(save_mode)
                .partitionBy(*PARTITION_COLS_BRONZE)
                .format(FORMAT_BRONZE)
                .save(output_path)
            )

            # Coletar estatísticas
            record_count = df.count()
            
            stats = {
                "output_path": output_path,
                "record_count": record_count,
                "save_mode": save_mode,
                "partition_cols": PARTITION_COLS_BRONZE,
                "format": FORMAT_BRONZE,
            }

            logger.info(
                "Data saved successfully to Bronze",
                extra=stats,
            )

            records_processed_total.labels(
                stage="bronze", status="written"
            ).inc(record_count)

            return stats

        except Exception as e:
            raise StorageWriteException(
                location=output_path,
                reason=str(e)
            )

    def _update_metrics(self, stats: Dict[str, any]) -> None:
        """
        Atualiza métricas Prometheus.

        Args:
            stats: Estatísticas do job
        """
        update_processing_metrics(
            stage="bronze",
            input_count=stats.get("record_count", 0),
            output_count=stats.get("record_count", 0),
            bytes_written_count=0,  # TODO: calcular tamanho real
        )


# Função standalone para uso em Airflow
def run_api_to_bronze_job(
    line_id: Optional[int] = None,
    save_mode: str = "append",
    **kwargs
) -> Dict[str, any]:
    """
    Função standalone para executar job.

    Args:
        line_id: ID da linha (opcional)
        save_mode: Modo de salvamento
        **kwargs: Argumentos adicionais

    Returns:
        Estatísticas do job
    """
    job = APIToBronzeJob()
    stats = job.run(line_id=line_id, save_mode=save_mode)
    return stats


# Exemplo de uso
if __name__ == "__main__":
    from ...common.logging_config import setup_logging

    # Setup
    setup_logging(log_level="INFO", log_format="console")

    # Executar job
    job = APIToBronzeJob()
    stats = job.run()

    print(f"\nJob completed successfully!")
    print(f"Records ingested: {stats['record_count']}")
    print(f"Output path: {stats['output_path']}")
