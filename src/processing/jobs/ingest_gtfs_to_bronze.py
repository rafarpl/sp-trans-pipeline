"""
GTFS to Bronze Ingestion Job
=============================
Job Spark para ingestão de dados GTFS (CSV) para a camada Bronze (Parquet).

Processa os arquivos:
- routes.txt
- trips.txt
- stops.txt
- stop_times.txt
- shapes.txt
- calendar.txt
- agency.txt
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from src.common.logging_config import get_logger
from src.common.config import Config
from src.common.metrics import track_job_execution, reporter
from src.ingestion.schema_definitions import GTFSSchemas

logger = get_logger(__name__)


class GTFSToBronzeJob:
    """Job de ingestão GTFS para Bronze layer."""
    
    def __init__(self, 
                 input_path: str,
                 output_path: str,
                 execution_date: Optional[datetime] = None):
        """
        Inicializa job.
        
        Args:
            input_path: Caminho dos arquivos CSV do GTFS
            output_path: Caminho de saída no Data Lake (S3/MinIO)
            execution_date: Data de execução (usa agora se None)
        """
        self.input_path = Path(input_path)
        self.output_path = output_path
        self.execution_date = execution_date or datetime.now()
        
        # Configurações
        self.config = Config()
        
        # Schemas GTFS
        self.schemas = GTFSSchemas()
        
        # Estatísticas
        self.stats = {
            'files_processed': 0,
            'total_records': 0,
            'failed_files': []
        }
        
        logger.info(f"GTFSToBronzeJob inicializado: input={input_path}, output={output_path}")
    
    def _create_spark_session(self) -> SparkSession:
        """Cria sessão Spark com configurações otimizadas."""
        logger.info("Criando sessão Spark")
        
        spark = SparkSession.builder \
            .appName("GTFS_to_Bronze_Ingestion") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
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
        
        logger.info(f"Spark session criada: {spark.version}")
        
        return spark
    
    def _get_partition_path(self) -> str:
        """
        Retorna path particionado por data.
        
        Returns:
            Path com partições (ex: year=2025/month=01/day=20/)
        """
        return (
            f"year={self.execution_date.year}/"
            f"month={self.execution_date.month:02d}/"
            f"day={self.execution_date.day:02d}"
        )
    
    def _read_gtfs_file(self, spark: SparkSession, 
                       filename: str, 
                       schema: StructType) -> Optional[DataFrame]:
        """
        Lê arquivo GTFS CSV.
        
        Args:
            spark: Sessão Spark
            filename: Nome do arquivo (ex: 'routes.txt')
            schema: Schema Spark para o arquivo
        
        Returns:
            DataFrame ou None se falhar
        """
        filepath = self.input_path / filename
        
        if not filepath.exists():
            logger.warning(f"Arquivo não encontrado: {filepath}")
            return None
        
        logger.info(f"Lendo {filename}...")
        
        try:
            df = spark.read.csv(
                str(filepath),
                header=True,
                schema=schema,
                encoding='utf-8',
                quote='"',
                escape='"',
                multiLine=True
            )
            
            count = df.count()
            logger.info(f"  {filename}: {count:,} registros")
            
            return df
        
        except Exception as e:
            logger.error(f"Erro ao ler {filename}: {e}")
            self.stats['failed_files'].append(filename)
            return None
    
    def _add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """
        Adiciona colunas de metadados.
        
        Args:
            df: DataFrame original
        
        Returns:
            DataFrame com metadados
        """
        return df \
            .withColumn('ingestion_timestamp', F.lit(datetime.now())) \
            .withColumn('ingestion_date', F.lit(self.execution_date.date())) \
            .withColumn('source_system', F.lit('sptrans_gtfs')) \
            .withColumn('data_quality_flag', F.lit('raw'))
    
    def _write_to_bronze(self, df: DataFrame, table_name: str):
        """
        Escreve DataFrame na camada Bronze.
        
        Args:
            df: DataFrame para escrever
            table_name: Nome da tabela (ex: 'routes')
        """
        if df is None or df.count() == 0:
            logger.warning(f"DataFrame vazio para {table_name}, pulando escrita")
            return
        
        # Adicionar metadados
        df_with_metadata = self._add_metadata_columns(df)
        
        # Path de saída com partições
        partition_path = self._get_partition_path()
        output_full_path = f"{self.output_path}/{table_name}/{partition_path}"
        
        logger.info(f"Escrevendo {table_name} para {output_full_path}")
        
        try:
            # Escrever em Parquet (formato colunar otimizado)
            df_with_metadata.write \
                .mode('overwrite') \
                .parquet(output_full_path)
            
            record_count = df_with_metadata.count()
            self.stats['total_records'] += record_count
            self.stats['files_processed'] += 1
            
            logger.info(f"  ✓ {table_name}: {record_count:,} registros escritos")
            
            # Reportar métricas
            reporter.report_processing_stats(
                layer='bronze',
                source=f'gtfs_{table_name}',
                total=record_count,
                invalid=0,
                duplicated=0
            )
        
        except Exception as e:
            logger.error(f"Erro ao escrever {table_name}: {e}")
            self.stats['failed_files'].append(table_name)
            raise
    
    @track_job_execution('gtfs_to_bronze')
    def run(self) -> Dict[str, Any]:
        """
        Executa job de ingestão.
        
        Returns:
            Dict com estatísticas da execução
        """
        logger.info("=" * 80)
        logger.info("INICIANDO GTFS TO BRONZE INGESTION JOB")
        logger.info("=" * 80)
        
        spark = None
        
        try:
            # Criar sessão Spark
            spark = self._create_spark_session()
            
            # Processar cada arquivo GTFS
            gtfs_files = [
                ('routes', 'routes.txt', self.schemas.routes_schema),
                ('trips', 'trips.txt', self.schemas.trips_schema),
                ('stops', 'stops.txt', self.schemas.stops_schema),
                ('stop_times', 'stop_times.txt', self.schemas.stop_times_schema),
                ('shapes', 'shapes.txt', self.schemas.shapes_schema),
                ('calendar', 'calendar.txt', self.schemas.calendar_schema),
                ('agency', 'agency.txt', self.schemas.agency_schema),
            ]
            
            for table_name, filename, schema in gtfs_files:
                logger.info(f"\nProcessando {table_name}...")
                
                # Ler arquivo
                df = self._read_gtfs_file(spark, filename, schema)
                
                if df is not None:
                    # Escrever na Bronze
                    self._write_to_bronze(df, table_name)
                else:
                    logger.warning(f"Pulando {table_name} (arquivo não encontrado ou erro)")
            
            # Resultados
            logger.info("\n" + "=" * 80)
            logger.info("JOB CONCLUÍDO COM SUCESSO")
            logger.info("=" * 80)
            logger.info(f"Arquivos processados: {self.stats['files_processed']}")
            logger.info(f"Total de registros: {self.stats['total_records']:,}")
            
            if self.stats['failed_files']:
                logger.warning(f"Arquivos com falha: {self.stats['failed_files']}")
            
            result = {
                'success': True,
                'execution_date': self.execution_date.isoformat(),
                'files_processed': self.stats['files_processed'],
                'total_records': self.stats['total_records'],
                'failed_files': self.stats['failed_files'],
                'output_path': self.output_path,
                'timestamp': datetime.now().isoformat()
            }
            
            return result
        
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
    
    parser = argparse.ArgumentParser(description='GTFS to Bronze Ingestion Job')
    parser.add_argument('--input-path', required=True, help='Path dos arquivos GTFS')
    parser.add_argument('--output-path', required=True, help='Path de saída (S3/MinIO)')
    parser.add_argument('--execution-date', help='Data de execução (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Parse execution date
    execution_date = None
    if args.execution_date:
        execution_date = datetime.strptime(args.execution_date, '%Y-%m-%d')
    
    # Executar job
    job = GTFSToBronzeJob(
        input_path=args.input_path,
        output_path=args.output_path,
        execution_date=execution_date
    )
    
    result = job.run()
    
    print(f"\n{'=' * 80}")
    print("RESULTADO:")
    print(f"{'=' * 80}")
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
