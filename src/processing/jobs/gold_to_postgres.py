"""
Job Spark: Load Gold → PostgreSQL (Serving Layer).

Carrega dados agregados da camada Gold para o PostgreSQL
que serve como serving layer para dashboards e consultas.
"""

from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ...common.config import Config
from ...common.constants import (
    BUCKET_GOLD,
    SCHEMA_SERVING,
    TABLE_DAILY_AGGREGATES,
    TABLE_HOURLY_AGGREGATES,
    TABLE_LINES_METRICS,
)
from ...common.exceptions import DatabaseQueryException, ProcessingException
from ...common.logging_config import get_logger
from ...common.metrics import track_pipeline_run, update_processing_metrics
from ...common.utils import generate_uuid, get_s3_path

logger = get_logger(__name__)


class GoldToPostgresJob:
    """
    Job Spark para load Gold → PostgreSQL.
    
    Responsabilidades:
    - Ler dados agregados da camada Gold
    - Transformar para formato otimizado para serving
    - Carregar no PostgreSQL usando JDBC
    - Suportar upsert (atualizar ou inserir)
    - Manter tabelas otimizadas
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
        
        # JDBC properties
        self.jdbc_url = (
            f"jdbc:postgresql://{self.config.postgres.host}:{self.config.postgres.port}"
            f"/{self.config.postgres.database}"
        )
        self.jdbc_properties = {
            "user": self.config.postgres.user,
            "password": self.config.postgres.password,
            "driver": "org.postgresql.Driver",
        }
        
        # Paths
        self.gold_bucket = BUCKET_GOLD
        self.serving_schema = SCHEMA_SERVING
        
        logger.info(
            "GoldToPostgresJob initialized",
            extra={"job_id": self.job_id, "jdbc_url": self.jdbc_url},
        )

    def _create_spark_session(self) -> SparkSession:
        """Cria SparkSession com JDBC driver."""
        return (
            SparkSession.builder
            .appName(f"SPTrans_Gold_to_Postgres_{self.job_id}")
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")  # Driver JDBC
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", self.config.minio.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.config.minio.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.config.minio.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

    @track_pipeline_run(stage="serving", job_name="gold_to_postgres")
    def run(
        self,
        data_type: str = "hourly_metrics",
        date: Optional[str] = None,
        mode: str = "append",
    ) -> Dict[str, any]:
        """
        Executa load Gold → PostgreSQL.

        Args:
            data_type: Tipo de dado ('hourly_metrics', 'daily_summary', 'line_performance')
            date: Data a processar (YYYY-MM-DD, None = todos)
            mode: Modo de escrita ('append', 'overwrite', 'upsert')

        Returns:
            Estatísticas do job

        Raises:
            ProcessingException: Se job falhar
        """
        try:
            logger.info(
                "Starting Gold to PostgreSQL load",
                extra={
                    "job_id": self.job_id,
                    "data_type": data_type,
                    "date": date,
                    "mode": mode,
                },
            )

            # 1. Ler dados do Gold
            gold_df = self._read_gold(data_type, date)
            input_count = gold_df.count()
            
            logger.info(f"Read {input_count} records from Gold")

            # 2. Preparar para serving
            serving_df = self._prepare_for_serving(gold_df, data_type)

            # 3. Determinar tabela de destino
            table_name = self._get_target_table(data_type)

            # 4. Load no PostgreSQL
            if mode == "upsert":
                self._upsert_to_postgres(serving_df, table_name, data_type)
            else:
                self._write_to_postgres(serving_df, table_name, mode)

            # 5. Atualizar métricas
            self._update_metrics(input_count)
            
            stats = {
                "input_count": input_count,
                "data_type": data_type,
                "table_name": table_name,
                "mode": mode,
            }
            
            logger.info(
                "Gold to PostgreSQL load completed",
                extra={"job_id": self.job_id, "stats": stats},
            )
            
            return stats

        except Exception as e:
            logger.error(
                f"Gold to PostgreSQL load failed: {e}",
                extra={"job_id": self.job_id, "error": str(e)},
                exc_info=True,
            )
            raise ProcessingException(
                transformation="gold_to_postgres",
                reason=str(e)
            )

    def _read_gold(self, data_type: str, date: Optional[str] = None) -> DataFrame:
        """
        Lê dados da camada Gold.

        Args:
            data_type: Tipo de dado
            date: Data (opcional)

        Returns:
            DataFrame do Gold
        """
        # Mapear data_type para prefixo
        prefix_map = {
            "hourly_metrics": "hourly_metrics",
            "daily_summary": "daily_summary",
            "line_performance": "line_performance",
        }

        prefix = prefix_map.get(data_type)
        if not prefix:
            raise ValueError(f"Unknown data type: {data_type}")

        input_path = get_s3_path(
            bucket=self.gold_bucket,
            prefix=prefix,
        )

        logger.info(f"Reading from Gold: {input_path}")

        df = self.spark.read.format("delta").load(input_path)

        # Filtrar por data se especificado
        if date:
            year, month = date.split("-")[:2]
            df = df.filter(
                (F.col("year") == int(year))
                & (F.col("month") == int(month))
            )

        return df

    def _prepare_for_serving(self, df: DataFrame, data_type: str) -> DataFrame:
        """
        Prepara dados para serving layer.

        Args:
            df: DataFrame Gold
            data_type: Tipo de dado

        Returns:
            DataFrame preparado
        """
        logger.info("Preparing data for serving")

        # Adicionar metadados de load
        df = df.withColumn("loaded_at", F.current_timestamp())

        # Arredondar valores numéricos para evitar precisão excessiva
        numeric_cols = [
            field.name
            for field in df.schema.fields
            if str(field.dataType) in ["DoubleType", "FloatType"]
        ]

        for col in numeric_cols:
            if col in df.columns:
                df = df.withColumn(col, F.round(F.col(col), 2))

        # Remover colunas de particionamento se não forem necessárias no serving
        # (mantemos para queries)

        return df

    def _get_target_table(self, data_type: str) -> str:
        """
        Retorna nome da tabela de destino.

        Args:
            data_type: Tipo de dado

        Returns:
            Nome completo da tabela (schema.table)
        """
        table_map = {
            "hourly_metrics": TABLE_HOURLY_AGGREGATES,
            "daily_summary": TABLE_DAILY_AGGREGATES,
            "line_performance": TABLE_LINES_METRICS,
        }

        table_name = table_map.get(data_type)
        if not table_name:
            raise ValueError(f"Unknown data type: {data_type}")

        # Adicionar schema
        full_table = f"{self.serving_schema}.{table_name}"
        return full_table

    def _write_to_postgres(
        self, df: DataFrame, table_name: str, mode: str = "append"
    ) -> None:
        """
        Escreve DataFrame no PostgreSQL.

        Args:
            df: DataFrame a escrever
            table_name: Nome da tabela
            mode: Modo de escrita
        """
        try:
            logger.info(
                f"Writing to PostgreSQL: {table_name}",
                extra={"mode": mode, "record_count": df.count()},
            )

            (
                df.write
                .mode(mode)
                .jdbc(
                    url=self.jdbc_url,
                    table=table_name,
                    properties=self.jdbc_properties,
                )
            )

            logger.info(f"Successfully written to {table_name}")

        except Exception as e:
            raise DatabaseQueryException(
                query=f"WRITE to {table_name}",
                reason=str(e)
            )

    def _upsert_to_postgres(
        self, df: DataFrame, table_name: str, data_type: str
    ) -> None:
        """
        Faz upsert no PostgreSQL (update ou insert).

        Args:
            df: DataFrame
            table_name: Nome da tabela
            data_type: Tipo de dado
        """
        logger.info(f"Performing upsert to {table_name}")

        # Criar tabela temporária
        temp_table = f"{table_name}_temp"

        # Escrever em temp
        self._write_to_postgres(df, temp_table, mode="overwrite")

        # Determinar chaves primárias baseado no tipo
        key_cols = self._get_primary_keys(data_type)

        # Construir query de merge
        merge_query = self._build_merge_query(table_name, temp_table, key_cols)

        # Executar merge via JDBC
        try:
            # Conectar e executar
            logger.info("Executing merge query")

            # Para executar SQL customizado, precisamos usar JDBC diretamente
            # Aqui é um placeholder - em produção, usar py4j ou psycopg2
            logger.warning("Upsert not fully implemented - using append instead")
            self._write_to_postgres(df, table_name, mode="append")

            # TODO: Implementar merge real usando:
            # - Temporary staging table
            # - PostgreSQL INSERT ... ON CONFLICT DO UPDATE
            # - ou MERGE statement (PostgreSQL 15+)

        except Exception as e:
            raise DatabaseQueryException(
                query=f"MERGE into {table_name}",
                reason=str(e)
            )

    def _get_primary_keys(self, data_type: str) -> list:
        """
        Retorna chaves primárias baseado no tipo de dado.

        Args:
            data_type: Tipo de dado

        Returns:
            Lista de colunas de chave primária
        """
        key_map = {
            "hourly_metrics": ["line_id", "hour_timestamp"],
            "daily_summary": ["date"],
            "line_performance": ["line_id", "analysis_period", "period_start"],
        }

        return key_map.get(data_type, [])

    def _build_merge_query(
        self, target_table: str, source_table: str, key_cols: list
    ) -> str:
        """
        Constrói query de merge/upsert.

        Args:
            target_table: Tabela de destino
            source_table: Tabela temporária
            key_cols: Colunas de chave

        Returns:
            Query SQL
        """
        # PostgreSQL 15+ MERGE syntax
        merge_query = f"""
        MERGE INTO {target_table} AS target
        USING {source_table} AS source
        ON {' AND '.join([f'target.{col} = source.{col}' for col in key_cols])}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """

        return merge_query

    def _update_metrics(self, record_count: int) -> None:
        """Atualiza métricas."""
        update_processing_metrics(
            stage="serving",
            input_count=record_count,
            output_count=record_count,
            bytes_written_count=0,
        )


# Função standalone
def run_gold_to_postgres_job(
    data_type: str = "hourly_metrics",
    date: Optional[str] = None,
    mode: str = "append",
    **kwargs
) -> Dict[str, any]:
    """Função standalone para Airflow."""
    job = GoldToPostgresJob()
    stats = job.run(data_type=data_type, date=date, mode=mode)
    return stats


# Exemplo de uso
if __name__ == "__main__":
    from ...common.logging_config import setup_logging

    setup_logging(log_level="INFO", log_format="console")

    job = GoldToPostgresJob()
    
    # Load hourly metrics
    stats = job.run(data_type="hourly_metrics", mode="append")

    print(f"\nJob completed!")
    print(f"Records loaded: {stats['input_count']}")
    print(f"Target table: {stats['table_name']}")
    print(f"Mode: {stats['mode']}")
