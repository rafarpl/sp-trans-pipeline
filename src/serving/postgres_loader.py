"""
PostgreSQL Loader Module

Responsável por carregar dados processados no PostgreSQL:
- Tabelas materializadas
- Dados agregados
- KPIs calculados
"""

from typing import Dict, List, Optional
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

from src.common.config import get_config
from src.common.logging_config import get_logger
from src.common.exceptions import StorageException
from src.common.metrics import MetricsCollector

logger = get_logger(__name__)


class PostgresLoader:
    """
    Classe para carregar dados no PostgreSQL.
    
    Suporta:
    - Escrita via Spark JDBC
    - Escrita via psycopg2 (bulk insert)
    - Escrita via SQLAlchemy (ORM)
    - Atualização de tabelas materializadas
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Inicializa o PostgresLoader.
        
        Args:
            config: Configuração opcional (usa config padrão se None)
        """
        self.config = config or get_config()
        self.metrics = MetricsCollector()
        self.logger = get_logger(self.__class__.__name__)
        
        # Connection string
        self.jdbc_url = self._build_jdbc_url()
        self.sqlalchemy_url = self._build_sqlalchemy_url()
    
    def _build_jdbc_url(self) -> str:
        """Constrói URL JDBC para Spark."""
        return (
            f"jdbc:postgresql://{self.config.POSTGRES_HOST}:{self.config.POSTGRES_PORT}/"
            f"{self.config.POSTGRES_DB}"
        )
    
    def _build_sqlalchemy_url(self) -> str:
        """Constrói URL SQLAlchemy."""
        return (
            f"postgresql+psycopg2://{self.config.POSTGRES_USER}:{self.config.POSTGRES_PASSWORD}@"
            f"{self.config.POSTGRES_HOST}:{self.config.POSTGRES_PORT}/{self.config.POSTGRES_DB}"
        )
    
    def load_dataframe(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
        partition_column: Optional[str] = None,
        num_partitions: int = 10
    ) -> None:
        """
        Carrega DataFrame do Spark no PostgreSQL via JDBC.
        
        Args:
            df: DataFrame do Spark
            table_name: Nome da tabela destino
            mode: Modo de escrita ('overwrite', 'append', 'error', 'ignore')
            partition_column: Coluna para particionar escrita (opcional)
            num_partitions: Número de partições para escrita paralela
        """
        self.logger.info(f"Loading DataFrame to PostgreSQL table '{table_name}' (mode={mode})")
        
        try:
            # Configurações JDBC
            jdbc_properties = {
                "user": self.config.POSTGRES_USER,
                "password": self.config.POSTGRES_PASSWORD,
                "driver": "org.postgresql.Driver",
                "batchsize": "10000",
                "rewriteBatchedStatements": "true",
            }
            
            # Reparticionar se necessário
            if num_partitions:
                df = df.repartition(num_partitions)
            
            # Escrever
            start_time = datetime.now()
            
            if partition_column:
                df.write.jdbc(
                    url=self.jdbc_url,
                    table=table_name,
                    mode=mode,
                    properties=jdbc_properties,
                    partitionColumn=partition_column,
                    numPartitions=num_partitions
                )
            else:
                df.write.jdbc(
                    url=self.jdbc_url,
                    table=table_name,
                    mode=mode,
                    properties=jdbc_properties
                )
            
            duration = (datetime.now() - start_time).total_seconds()
            record_count = df.count()
            
            self.logger.info(
                f"Successfully loaded {record_count} records to '{table_name}' "
                f"in {duration:.2f} seconds"
            )
            
            self.metrics.counter(f"postgres.loaded_records.{table_name}", record_count)
            self.metrics.histogram(f"postgres.load_duration.{table_name}", duration)
            
        except Exception as e:
            self.logger.error(f"Failed to load data to PostgreSQL: {e}")
            raise StorageException(f"PostgreSQL load failed: {e}")
    
    def execute_query(self, query: str) -> None:
        """
        Executa uma query SQL no PostgreSQL.
        
        Args:
            query: Query SQL a executar
        """
        self.logger.info(f"Executing query: {query[:100]}...")
        
        try:
            engine = create_engine(self.sqlalchemy_url)
            with engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
            
            self.logger.info("Query executed successfully")
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise StorageException(f"Query execution failed: {e}")
    
    def refresh_materialized_view(self, view_name: str) -> None:
        """
        Atualiza uma materialized view.
        
        Args:
            view_name: Nome da materialized view
        """
        self.logger.info(f"Refreshing materialized view '{view_name}'")
        
        start_time = datetime.now()
        
        try:
            query = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name};"
            self.execute_query(query)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(
                f"Materialized view '{view_name}' refreshed in {duration:.2f} seconds"
            )
            
            self.metrics.histogram(f"postgres.refresh_mv_duration.{view_name}", duration)
            
        except Exception as e:
            self.logger.error(f"Failed to refresh materialized view: {e}")
            raise StorageException(f"Materialized view refresh failed: {e}")
    
    def refresh_all_materialized_views(self, schema: str = "serving") -> None:
        """
        Atualiza todas as materialized views de um schema.
        
        Args:
            schema: Nome do schema
        """
        self.logger.info(f"Refreshing all materialized views in schema '{schema}'")
        
        # Buscar todas as MVs do schema
        query = f"""
        SELECT schemaname, matviewname
        FROM pg_matviews
        WHERE schemaname = '{schema}';
        """
        
        try:
            engine = create_engine(self.sqlalchemy_url)
            with engine.connect() as conn:
                result = conn.execute(text(query))
                views = result.fetchall()
            
            self.logger.info(f"Found {len(views)} materialized views to refresh")
            
            for schema_name, view_name in views:
                full_name = f"{schema_name}.{view_name}"
                self.refresh_materialized_view(full_name)
            
            self.logger.info("All materialized views refreshed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to refresh materialized views: {e}")
            raise StorageException(f"Materialized views refresh failed: {e}")
    
    def upsert_dataframe(
        self,
        df: DataFrame,
        table_name: str,
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None
    ) -> None:
        """
        Faz upsert (INSERT ... ON CONFLICT UPDATE) de um DataFrame.
        
        Args:
            df: DataFrame do Spark
            table_name: Nome da tabela
            conflict_columns: Colunas para detectar conflito (chave única)
            update_columns: Colunas a atualizar em caso de conflito (None = todas)
        """
        self.logger.info(f"Upserting DataFrame to '{table_name}'")
        
        # Coletar dados (para tabelas pequenas/médias)
        data = df.collect()
        columns = df.columns
        
        if update_columns is None:
            update_columns = [col for col in columns if col not in conflict_columns]
        
        # Construir query
        conflict_clause = ", ".join(conflict_columns)
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)
        
        query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES %s
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {update_clause};
        """
        
        try:
            # Conectar e executar
            conn = psycopg2.connect(
                host=self.config.POSTGRES_HOST,
                port=self.config.POSTGRES_PORT,
                database=self.config.POSTGRES_DB,
                user=self.config.POSTGRES_USER,
                password=self.config.POSTGRES_PASSWORD
            )
            
            cursor = conn.cursor()
            
            # Converter Row objects para tuples
            values = [tuple(row) for row in data]
            
            execute_values(cursor, query, values, template=f"({placeholders})")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Upserted {len(data)} records to '{table_name}'")
            self.metrics.counter(f"postgres.upserted_records.{table_name}", len(data))
            
        except Exception as e:
            self.logger.error(f"Upsert failed: {e}")
            raise StorageException(f"Upsert failed: {e}")
    
    def truncate_table(self, table_name: str) -> None:
        """
        Trunca uma tabela (remove todos os registros).
        
        Args:
            table_name: Nome da tabela
        """
        self.logger.warning(f"Truncating table '{table_name}'")
        
        query = f"TRUNCATE TABLE {table_name} CASCADE;"
        self.execute_query(query)
        
        self.logger.info(f"Table '{table_name}' truncated")
    
    def vacuum_table(self, table_name: str, full: bool = False) -> None:
        """
        Executa VACUUM em uma tabela.
        
        Args:
            table_name: Nome da tabela
            full: Se deve fazer VACUUM FULL (mais agressivo)
        """
        vacuum_type = "FULL" if full else ""
        self.logger.info(f"Running VACUUM {vacuum_type} on '{table_name}'")
        
        query = f"VACUUM {vacuum_type} {table_name};"
        self.execute_query(query)
        
        self.logger.info(f"VACUUM completed on '{table_name}'")
    
    def analyze_table(self, table_name: str) -> None:
        """
        Executa ANALYZE em uma tabela (atualiza estatísticas).
        
        Args:
            table_name: Nome da tabela
        """
        self.logger.info(f"Analyzing table '{table_name}'")
        
        query = f"ANALYZE {table_name};"
        self.execute_query(query)
        
        self.logger.info(f"Table '{table_name}' analyzed")


def load_to_postgres(
    df: DataFrame,
    table_name: str,
    mode: str = "append"
) -> None:
    """
    Função utilitária para carregar DataFrame no PostgreSQL.
    
    Args:
        df: DataFrame do Spark
        table_name: Nome da tabela destino
        mode: Modo de escrita
    """
    loader = PostgresLoader()
    loader.load_dataframe(df, table_name, mode)


def load_kpis_to_serving(
    spark: SparkSession,
    kpis_path: str,
    schema: str = "serving"
) -> None:
    """
    Carrega KPIs calculados para a camada serving.
    
    Args:
        spark: SparkSession
        kpis_path: Caminho base dos KPIs
        schema: Schema PostgreSQL destino
    """
    logger.info(f"Loading KPIs to serving layer (schema={schema})")
    
    loader = PostgresLoader()
    
    # Lista de KPIs para carregar
    kpis = [
        "fleet_coverage",
        "average_speed",
        "trip_stats",
        "daily_summary",
        "punctuality"
    ]
    
    for kpi_name in kpis:
        try:
            kpi_df = spark.read.parquet(f"{kpis_path}/{kpi_name}")
            table_name = f"{schema}.kpi_{kpi_name}"
            
            loader.load_dataframe(
                kpi_df,
                table_name,
                mode="overwrite"
            )
            
            # Analisar tabela após carga
            loader.analyze_table(table_name)
            
        except Exception as e:
            logger.warning(f"Could not load KPI '{kpi_name}': {e}")
    
    # Atualizar materialized views
    loader.refresh_all_materialized_views(schema)
    
    logger.info("KPIs loaded to serving layer successfully")