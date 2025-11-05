"""
Job Spark: Ingestão de dados GTFS para camada Bronze.

Faz download dos arquivos GTFS estáticos, converte para formato
Parquet e armazena no Data Lake na camada Bronze.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ...common.config import Config
from ...common.constants import (
    BUCKET_BRONZE,
    FORMAT_BRONZE,
    PREFIX_GTFS_STATIC,
)
from ...common.exceptions import ProcessingException, StorageWriteException
from ...common.logging_config import get_logger
from ...common.metrics import (
    records_processed_total,
    track_pipeline_run,
    update_processing_metrics,
)
from ...common.utils import generate_uuid, get_s3_path
from ...ingestion.gtfs_downloader import GTFSDownloader
from ...ingestion.schema_definitions import get_gtfs_schema

logger = get_logger(__name__)

# Arquivos GTFS a processar
GTFS_FILES_TO_PROCESS = [
    "stops",
    "routes",
    "trips",
    "stop_times",
    "shapes",
]


class GTFSToBronzeJob:
    """
    Job Spark para ingestão de dados GTFS para Bronze.
    
    Responsabilidades:
    - Download do feed GTFS
    - Extração dos arquivos
    - Conversão para Parquet com schema validado
    - Armazenamento no Data Lake (MinIO/S3)
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
            config: Configuração
            job_id: ID do job
        """
        self.config = config or Config()
        self.job_id = job_id or generate_uuid()
        
        # SparkSession
        self.spark = spark or self._create_spark_session()
        
        # GTFS Downloader
        self.gtfs_downloader = GTFSDownloader(config=self.config)
        
        # Paths
        self.bronze_bucket = BUCKET_BRONZE
        self.bronze_prefix = PREFIX_GTFS_STATIC
        
        logger.info(
            "GTFSToBronzeJob initialized",
            extra={
                "job_id": self.job_id,
                "bronze_bucket": self.bronze_bucket,
                "bronze_prefix": self.bronze_prefix,
            },
        )

    def _create_spark_session(self) -> SparkSession:
        """Cria SparkSession configurada."""
        return (
            SparkSession.builder
            .appName(f"SPTrans_GTFS_to_Bronze_{self.job_id}")
            .config("spark.hadoop.fs.s3a.endpoint", self.config.minio.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.config.minio.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.config.minio.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

    @track_pipeline_run(stage="bronze", job_name="gtfs_to_bronze")
    def run(
        self,
        force_download: bool = False,
        files_to_process: Optional[List[str]] = None,
    ) -> Dict[str, any]:
        """
        Executa job de ingestão GTFS.

        Args:
            force_download: Se True, força novo download do GTFS
            files_to_process: Lista de arquivos GTFS (None = todos)

        Returns:
            Dicionário com estatísticas

        Raises:
            ProcessingException: Se job falhar
        """
        try:
            logger.info(
                "Starting GTFS to Bronze ingestion",
                extra={"job_id": self.job_id, "force_download": force_download},
            )

            files_to_process = files_to_process or GTFS_FILES_TO_PROCESS

            # 1. Download e extração
            extract_dir = self._download_and_extract(force_download)
            
            # 2. Processar cada arquivo GTFS
            results = {}
            for file_type in files_to_process:
                try:
                    result = self._process_gtfs_file(file_type, extract_dir)
                    results[file_type] = result
                except Exception as e:
                    logger.error(
                        f"Error processing {file_type}: {e}",
                        extra={"file_type": file_type, "error": str(e)},
                    )
                    results[file_type] = {"status": "error", "error": str(e)}

            # 3. Compilar estatísticas
            stats = self._compile_stats(results)
            
            logger.info(
                "GTFS to Bronze ingestion completed",
                extra={"job_id": self.job_id, "stats": stats},
            )
            
            return stats

        except Exception as e:
            logger.error(
                f"GTFS ingestion failed: {e}",
                extra={"job_id": self.job_id, "error": str(e)},
                exc_info=True,
            )
            raise ProcessingException(
                transformation="gtfs_to_bronze",
                reason=str(e)
            )

    def _download_and_extract(self, force: bool = False) -> Path:
        """
        Download e extração do GTFS.

        Args:
            force: Forçar novo download

        Returns:
            Path do diretório extraído
        """
        logger.info(f"Downloading GTFS (force={force})")
        
        # Download
        zip_path = self.gtfs_downloader.download_gtfs(force=force)
        
        # Extração
        extract_dir = self.gtfs_downloader.extract_gtfs(zip_path)
        
        logger.info(
            "GTFS downloaded and extracted",
            extra={"extract_dir": str(extract_dir)},
        )
        
        return extract_dir

    def _process_gtfs_file(
        self, file_type: str, extract_dir: Path
    ) -> Dict[str, any]:
        """
        Processa um arquivo GTFS específico.

        Args:
            file_type: Tipo do arquivo ('stops', 'routes', etc)
            extract_dir: Diretório com arquivos extraídos

        Returns:
            Dicionário com estatísticas
        """
        logger.info(f"Processing GTFS file: {file_type}")

        # 1. Ler arquivo CSV
        df = self._read_gtfs_csv(file_type, extract_dir)
        
        # 2. Aplicar schema
        df = self._apply_schema(df, file_type)
        
        # 3. Adicionar metadados
        df = self._add_metadata(df, file_type)
        
        # 4. Salvar no Bronze
        stats = self._save_to_bronze(df, file_type)
        
        return stats

    def _read_gtfs_csv(self, file_type: str, extract_dir: Path) -> DataFrame:
        """
        Lê arquivo CSV do GTFS.

        Args:
            file_type: Tipo do arquivo
            extract_dir: Diretório extraído

        Returns:
            DataFrame Spark
        """
        csv_path = extract_dir / f"{file_type}.txt"
        
        logger.info(f"Reading CSV: {csv_path}")

        df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "false")  # Vamos aplicar schema manualmente
            .option("encoding", "UTF-8")
            .csv(str(csv_path))
        )

        logger.info(
            f"CSV read successfully",
            extra={"file_type": file_type, "row_count": df.count()},
        )

        return df

    def _apply_schema(self, df: DataFrame, file_type: str) -> DataFrame:
        """
        Aplica schema tipado ao DataFrame.

        Args:
            df: DataFrame original
            file_type: Tipo do arquivo

        Returns:
            DataFrame com schema aplicado
        """
        logger.info(f"Applying schema for {file_type}")

        schema = get_gtfs_schema(file_type)
        
        # Criar DataFrame com schema correto
        # Fazendo cast das colunas
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

        return df

    def _add_metadata(self, df: DataFrame, file_type: str) -> DataFrame:
        """
        Adiciona colunas de metadados.

        Args:
            df: DataFrame original
            file_type: Tipo do arquivo

        Returns:
            DataFrame com metadados
        """
        current_timestamp = datetime.now()

        df = (
            df.withColumn("gtfs_file_type", F.lit(file_type))
            .withColumn("ingestion_timestamp", F.lit(current_timestamp))
            .withColumn("ingestion_date", F.lit(current_timestamp.strftime("%Y-%m-%d")))
        )

        return df

    def _save_to_bronze(
        self, df: DataFrame, file_type: str
    ) -> Dict[str, any]:
        """
        Salva DataFrame no Bronze.

        Args:
            df: DataFrame a salvar
            file_type: Tipo do arquivo

        Returns:
            Estatísticas do salvamento
        """
        try:
            output_path = get_s3_path(
                bucket=self.bronze_bucket,
                prefix=f"{self.bronze_prefix}/{file_type}",
            )

            logger.info(
                f"Saving {file_type} to Bronze: {output_path}",
            )

            # Salvar (overwrite para dados estáticos)
            (
                df.write
                .mode("overwrite")
                .format(FORMAT_BRONZE)
                .save(output_path)
            )

            record_count = df.count()
            
            stats = {
                "status": "success",
                "output_path": output_path,
                "record_count": record_count,
                "format": FORMAT_BRONZE,
            }

            logger.info(
                f"{file_type} saved successfully",
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

    def _compile_stats(self, results: Dict[str, Dict]) -> Dict[str, any]:
        """
        Compila estatísticas finais.

        Args:
            results: Resultados por arquivo

        Returns:
            Estatísticas compiladas
        """
        total_records = 0
        successful_files = 0
        failed_files = 0

        for file_type, result in results.items():
            if result.get("status") == "success":
                successful_files += 1
                total_records += result.get("record_count", 0)
            else:
                failed_files += 1

        stats = {
            "total_files_processed": len(results),
            "successful_files": successful_files,
            "failed_files": failed_files,
            "total_records": total_records,
            "results_by_file": results,
        }

        return stats


# Função standalone para Airflow
def run_gtfs_to_bronze_job(
    force_download: bool = False,
    **kwargs
) -> Dict[str, any]:
    """
    Função standalone para executar job.

    Args:
        force_download: Forçar download
        **kwargs: Argumentos adicionais

    Returns:
        Estatísticas do job
    """
    job = GTFSToBronzeJob()
    stats = job.run(force_download=force_download)
    return stats


# Exemplo de uso
if __name__ == "__main__":
    from ...common.logging_config import setup_logging

    setup_logging(log_level="INFO", log_format="console")

    # Executar job
    job = GTFSToBronzeJob()
    stats = job.run(force_download=False)

    print(f"\nJob completed!")
    print(f"Files processed: {stats['total_files_processed']}")
    print(f"Total records: {stats['total_records']}")
    print(f"Success: {stats['successful_files']}")
    print(f"Failed: {stats['failed_files']}")
