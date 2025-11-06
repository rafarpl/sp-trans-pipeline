"""
Deduplication Module

Responsável por remover registros duplicados:
- Deduplicação por chave primária
- Deduplicação por janela temporal
- Estratégias de resolução de conflitos
"""

from typing import List, Optional
from enum import Enum

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.logging_config import get_logger
from src.common.metrics import MetricsCollector

logger = get_logger(__name__)


class DeduplicationStrategy(Enum):
    """Estratégias de deduplicação."""
    KEEP_FIRST = "first"  # Mantém o primeiro registro
    KEEP_LAST = "last"    # Mantém o último registro (mais recente)
    KEEP_BEST_QUALITY = "best_quality"  # Mantém o de melhor qualidade


class Deduplicator:
    """
    Classe para deduplicação de registros.
    
    Remove duplicatas usando diferentes estratégias:
    - Por chave única (vehicle_id + timestamp)
    - Por janela temporal
    - Por qualidade dos dados
    """
    
    def __init__(
        self,
        spark: SparkSession,
        strategy: DeduplicationStrategy = DeduplicationStrategy.KEEP_LAST
    ):
        """
        Inicializa o Deduplicator.
        
        Args:
            spark: SparkSession ativa
            strategy: Estratégia de deduplicação
        """
        self.spark = spark
        self.strategy = strategy
        self.metrics = MetricsCollector()
        self.logger = get_logger(self.__class__.__name__)
    
    def deduplicate_by_key(
        self,
        df: DataFrame,
        key_columns: List[str]
    ) -> DataFrame:
        """
        Remove duplicatas por chave composta.
        
        Args:
            df: DataFrame de entrada
            key_columns: Lista de colunas que formam a chave
            
        Returns:
            DataFrame sem duplicatas
        """
        self.logger.info(f"Deduplicating by key: {key_columns}")
        
        original_count = df.count()
        
        # Remover duplicatas exatas
        df_dedup = df.dropDuplicates(key_columns)
        
        dedup_count = df_dedup.count()
        removed_count = original_count - dedup_count
        
        if removed_count > 0:
            self.logger.warning(
                f"Removed {removed_count} duplicate records "
                f"({(removed_count/original_count)*100:.2f}%)"
            )
            self.metrics.counter("deduplication.removed_records", removed_count)
        else:
            self.logger.info("No duplicates found")
        
        return df_dedup
    
    def deduplicate_by_window(
        self,
        df: DataFrame,
        partition_columns: List[str],
        order_column: str = "timestamp",
        ascending: bool = False
    ) -> DataFrame:
        """
        Remove duplicatas usando window function.
        
        Mantém apenas um registro por partição, ordenado por ordem especificada.
        
        Args:
            df: DataFrame de entrada
            partition_columns: Colunas para particionar (ex: vehicle_id)
            order_column: Coluna para ordenar (ex: timestamp)
            ascending: Se True, mantém o primeiro; se False, mantém o último
            
        Returns:
            DataFrame dedupli cado
        """
        self.logger.info(
            f"Deduplicating by window: partition={partition_columns}, "
            f"order={order_column}, ascending={ascending}"
        )
        
        original_count = df.count()
        
        # Criar window spec
        window_spec = Window.partitionBy(*partition_columns).orderBy(
            F.col(order_column).asc() if ascending else F.col(order_column).desc()
        )
        
        # Adicionar row number
        df_with_row_num = df.withColumn("row_num", F.row_number().over(window_spec))
        
        # Manter apenas o primeiro registro de cada partição
        df_dedup = df_with_row_num.filter(F.col("row_num") == 1).drop("row_num")
        
        dedup_count = df_dedup.count()
        removed_count = original_count - dedup_count
        
        if removed_count > 0:
            self.logger.warning(
                f"Removed {removed_count} duplicate records by window "
                f"({(removed_count/original_count)*100:.2f}%)"
            )
            self.metrics.counter("deduplication.removed_by_window", removed_count)
        else:
            self.logger.info("No duplicates found by window")
        
        return df_dedup
    
    def deduplicate_vehicles_by_time_window(
        self,
        df: DataFrame,
        time_window_seconds: int = 60
    ) -> DataFrame:
        """
        Remove duplicatas de veículos dentro de uma janela de tempo.
        
        Para cada veículo, mantém apenas uma posição a cada X segundos.
        
        Args:
            df: DataFrame com colunas vehicle_id, timestamp, latitude, longitude
            time_window_seconds: Janela de tempo em segundos
            
        Returns:
            DataFrame dedupli cado
        """
        self.logger.info(
            f"Deduplicating vehicles with {time_window_seconds}s time window"
        )
        
        original_count = df.count()
        
        # Criar timestamp truncado (arredondado para a janela)
        df_with_window = df.withColumn(
            "time_window",
            F.floor(F.unix_timestamp("timestamp") / time_window_seconds) * time_window_seconds
        )
        
        # Window spec por veículo e janela de tempo
        window_spec = Window.partitionBy("vehicle_id", "time_window").orderBy(
            F.col("timestamp").desc()
        )
        
        # Manter apenas o registro mais recente de cada janela
        df_dedup = (
            df_with_window
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num", "time_window")
        )
        
        dedup_count = df_dedup.count()
        removed_count = original_count - dedup_count
        
        if removed_count > 0:
            self.logger.warning(
                f"Removed {removed_count} duplicate records within time window "
                f"({(removed_count/original_count)*100:.2f}%)"
            )
            self.metrics.counter("deduplication.removed_by_time_window", removed_count)
        else:
            self.logger.info("No duplicates found within time window")
        
        return df_dedup
    
    def deduplicate_by_quality(
        self,
        df: DataFrame,
        partition_columns: List[str],
        quality_column: str = "quality_score"
    ) -> DataFrame:
        """
        Remove duplicatas mantendo o registro de melhor qualidade.
        
        Args:
            df: DataFrame com coluna de qualidade
            partition_columns: Colunas para particionar
            quality_column: Nome da coluna de qualidade
            
        Returns:
            DataFrame dedupli cado
        """
        self.logger.info(
            f"Deduplicating by quality: partition={partition_columns}, "
            f"quality_col={quality_column}"
        )
        
        original_count = df.count()
        
        # Window spec ordenado por qualidade (maior = melhor)
        window_spec = Window.partitionBy(*partition_columns).orderBy(
            F.col(quality_column).desc()
        )
        
        # Manter apenas o registro de melhor qualidade
        df_dedup = (
            df.withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )
        
        dedup_count = df_dedup.count()
        removed_count = original_count - dedup_count
        
        if removed_count > 0:
            self.logger.warning(
                f"Removed {removed_count} duplicate records by quality "
                f"({(removed_count/original_count)*100:.2f}%)"
            )
            self.metrics.counter("deduplication.removed_by_quality", removed_count)
        else:
            self.logger.info("No duplicates found by quality")
        
        return df_dedup
    
    def detect_duplicates(
        self,
        df: DataFrame,
        key_columns: List[str]
    ) -> DataFrame:
        """
        Detecta e marca registros duplicados sem removê-los.
        
        Args:
            df: DataFrame de entrada
            key_columns: Colunas que formam a chave
            
        Returns:
            DataFrame com coluna 'is_duplicate' adicionada
        """
        self.logger.info(f"Detecting duplicates by key: {key_columns}")
        
        # Contar ocorrências de cada chave
        window_spec = Window.partitionBy(*key_columns)
        
        df_with_flag = (
            df.withColumn("duplicate_count", F.count("*").over(window_spec))
            .withColumn("is_duplicate", F.col("duplicate_count") > 1)
        )
        
        duplicate_count = df_with_flag.filter(F.col("is_duplicate")).count()
        total_count = df.count()
        
        if duplicate_count > 0:
            self.logger.warning(
                f"Found {duplicate_count} duplicate records "
                f"({(duplicate_count/total_count)*100:.2f}%)"
            )
            self.metrics.gauge("deduplication.duplicate_records", duplicate_count)
        else:
            self.logger.info("No duplicates detected")
        
        return df_with_flag
    
    def remove_exact_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Remove registros 100% idênticos (todas as colunas iguais).
        
        Args:
            df: DataFrame de entrada
            
        Returns:
            DataFrame sem duplicatas exatas
        """
        self.logger.info("Removing exact duplicates (all columns)")
        
        original_count = df.count()
        
        df_dedup = df.dropDuplicates()
        
        dedup_count = df_dedup.count()
        removed_count = original_count - dedup_count
        
        if removed_count > 0:
            self.logger.warning(
                f"Removed {removed_count} exact duplicate records "
                f"({(removed_count/original_count)*100:.2f}%)"
            )
            self.metrics.counter("deduplication.removed_exact", removed_count)
        else:
            self.logger.info("No exact duplicates found")
        
        return df_dedup


def deduplicate_vehicles(
    df: DataFrame,
    strategy: str = "last",
    time_window_seconds: int = 60
) -> DataFrame:
    """
    Função utilitária para deduplicar posições de veículos.
    
    Args:
        df: DataFrame com vehicle_id e timestamp
        strategy: 'first', 'last', ou 'time_window'
        time_window_seconds: Janela de tempo (se strategy='time_window')
        
    Returns:
        DataFrame dedupli cado
    """
    spark = df.sparkSession
    deduplicator = Deduplicator(spark)
    
    if strategy == "time_window":
        return deduplicator.deduplicate_vehicles_by_time_window(
            df,
            time_window_seconds
        )
    elif strategy == "first":
        return deduplicator.deduplicate_by_window(
            df,
            partition_columns=["vehicle_id"],
            order_column="timestamp",
            ascending=True
        )
    elif strategy == "last":
        return deduplicator.deduplicate_by_window(
            df,
            partition_columns=["vehicle_id"],
            order_column="timestamp",
            ascending=False
        )
    else:
        raise ValueError(f"Invalid strategy: {strategy}")