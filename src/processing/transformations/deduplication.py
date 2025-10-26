# =============================================================================
# DEDUPLICATION
# =============================================================================
# Remoção de registros duplicados
# =============================================================================

"""
Deduplication Module

Estratégias para remover duplicatas:
    - Dedup exato: Remove registros idênticos
    - Dedup por chave: Remove duplicatas baseado em colunas específicas
    - Dedup temporal: Mantém registro mais recente
    - Dedup espacial: Remove posições muito próximas
"""

import logging
from typing import List, Optional
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# DEDUPLICATION FUNCTIONS
# =============================================================================

def dedup_positions(
    df: DataFrame,
    key_columns: Optional[List[str]] = None,
    timestamp_column: str = "timestamp",
    keep: str = "last"
) -> DataFrame:
    """
    Remove duplicatas de posições de veículos.
    
    Args:
        df: DataFrame com posições
        key_columns: Colunas que identificam unicidade (default: vehicle_id + timestamp)
        timestamp_column: Coluna de timestamp
        keep: 'first' ou 'last' - qual registro manter
    
    Returns:
        DataFrame sem duplicatas
    
    Example:
        >>> df_clean = dedup_positions(df, keep='last')
    """
    logger.info("Iniciando deduplicação de posições...")
    
    initial_count = df.count()
    
    # Colunas padrão para dedup
    if key_columns is None:
        key_columns = ["vehicle_id", timestamp_column]
    
    # Verificar se colunas existem
    missing_cols = [col for col in key_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Colunas faltando: {missing_cols}")
        return df
    
    # Estratégia: Ordenar por timestamp e manter primeiro/último
    window_spec = Window.partitionBy(key_columns).orderBy(
        F.col(timestamp_column).desc() if keep == "last" else F.col(timestamp_column).asc()
    )
    
    # Adicionar row number
    df_with_row = df.withColumn("row_num", F.row_number().over(window_spec))
    
    # Manter apenas row_num = 1
    df_deduped = df_with_row.filter(F.col("row_num") == 1).drop("row_num")
    
    final_count = df_deduped.count()
    duplicates_removed = initial_count - final_count
    
    logger.info(f"Duplicatas removidas: {duplicates_removed} ({(duplicates_removed/initial_count*100):.2f}%)")
    logger.info(f"Registros finais: {final_count}")
    
    return df_deduped


def remove_duplicates(
    df: DataFrame,
    subset: Optional[List[str]] = None,
    keep: str = "first"
) -> DataFrame:
    """
    Remove duplicatas exatas do DataFrame.
    
    Args:
        df: DataFrame
        subset: Colunas a considerar (None = todas)
        keep: 'first' ou 'last'
    
    Returns:
        DataFrame sem duplicatas
    
    Example:
        >>> df_unique = remove_duplicates(df, subset=['vehicle_id', 'timestamp'])
    """
    logger.info("Removendo duplicatas exatas...")
    
    initial_count = df.count()
    
    if subset:
        # Verificar se colunas existem
        missing_cols = [col for col in subset if col not in df.columns]
        if missing_cols:
            logger.error(f"Colunas faltando: {missing_cols}")
            return df
        
        # Drop duplicates baseado em subset
        df_unique = df.dropDuplicates(subset)
    else:
        # Drop duplicates em todas as colunas
        df_unique = df.dropDuplicates()
    
    final_count = df_unique.count()
    duplicates_removed = initial_count - final_count
    
    logger.info(f"Duplicatas removidas: {duplicates_removed}")
    
    return df_unique


def deduplicate_by_key(
    df: DataFrame,
    key_columns: List[str],
    order_column: str,
    ascending: bool = False
) -> DataFrame:
    """
    Remove duplicatas mantendo registro baseado em ordenação.
    
    Args:
        df: DataFrame
        key_columns: Colunas que definem chave única
        order_column: Coluna para ordenar
        ascending: Ordem crescente ou decrescente
    
    Returns:
        DataFrame deduplicado
    
    Example:
        >>> # Manter posição mais recente de cada veículo
        >>> df_clean = deduplicate_by_key(
        ...     df,
        ...     key_columns=['vehicle_id'],
        ...     order_column='timestamp',
        ...     ascending=False
        ... )
    """
    logger.info(f"Deduplicando por chave: {key_columns}")
    
    initial_count = df.count()
    
    # Window spec
    window_spec = Window.partitionBy(key_columns).orderBy(
        F.col(order_column).asc() if ascending else F.col(order_column).desc()
    )
    
    # Adicionar rank
    df_ranked = df.withColumn("rank", F.rank().over(window_spec))
    
    # Manter apenas rank = 1
    df_deduped = df_ranked.filter(F.col("rank") == 1).drop("rank")
    
    final_count = df_deduped.count()
    
    logger.info(f"Registros removidos: {initial_count - final_count}")
    
    return df_deduped


def remove_spatial_duplicates(
    df: DataFrame,
    vehicle_id_col: str = "vehicle_id",
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    timestamp_col: str = "timestamp",
    distance_threshold_meters: float = 10.0,
    time_threshold_seconds: int = 60
) -> DataFrame:
    """
    Remove duplicatas espaciais (posições muito próximas no espaço e tempo).
    
    Args:
        df: DataFrame
        vehicle_id_col: Coluna de ID do veículo
        lat_col: Coluna de latitude
        lon_col: Coluna de longitude
        timestamp_col: Coluna de timestamp
        distance_threshold_meters: Distância mínima em metros
        time_threshold_seconds: Tempo mínimo em segundos
    
    Returns:
        DataFrame sem duplicatas espaciais
    """
    logger.info("Removendo duplicatas espaciais...")
    
    initial_count = df.count()
    
    # Window para acessar registro anterior
    window_spec = Window.partitionBy(vehicle_id_col).orderBy(timestamp_col)
    
    # Adicionar lat/lon e timestamp do registro anterior
    df_with_prev = df.withColumn("prev_lat", F.lag(lat_col).over(window_spec)) \
                     .withColumn("prev_lon", F.lag(lon_col).over(window_spec)) \
                     .withColumn("prev_timestamp", F.lag(timestamp_col).over(window_spec))
    
    # Calcular distância e diferença de tempo
    # Fórmula simplificada de distância (haversine aproximada)
    df_with_distance = df_with_prev.withColumn(
        "distance_meters",
        F.when(
            F.col("prev_lat").isNotNull(),
            # Aproximação simples: 111km por grau
            F.sqrt(
                F.pow((F.col(lat_col) - F.col("prev_lat")) * 111000, 2) +
                F.pow((F.col(lon_col) - F.col("prev_lon")) * 111000 * 
                      F.cos(F.radians(F.col(lat_col))), 2)
            )
        ).otherwise(999999)
    )
    
    # Calcular diferença de tempo em segundos
    df_with_time_diff = df_with_distance.withColumn(
        "time_diff_seconds",
        F.when(
            F.col("prev_timestamp").isNotNull(),
            F.unix_timestamp(timestamp_col) - F.unix_timestamp("prev_timestamp")
        ).otherwise(999999)
    )
    
    # Filtrar apenas registros que NÃO são duplicatas espaciais
    df_filtered = df_with_time_diff.filter(
        (F.col("distance_meters") > distance_threshold_meters) |
        (F.col("time_diff_seconds") > time_threshold_seconds)
    )
    
    # Remover colunas auxiliares
    df_clean = df_filtered.drop("prev_lat", "prev_lon", "prev_timestamp", 
                                 "distance_meters", "time_diff_seconds")
    
    final_count = df_clean.count()
    spatial_duplicates = initial_count - final_count
    
    logger.info(f"Duplicatas espaciais removidas: {spatial_duplicates}")
    
    return df_clean


def remove_consecutive_duplicates(
    df: DataFrame,
    partition_cols: List[str],
    compare_cols: List[str],
    order_col: str = "timestamp"
) -> DataFrame:
    """
    Remove registros consecutivos idênticos.
    
    Útil para remover posições onde o veículo não se moveu.
    
    Args:
        df: DataFrame
        partition_cols: Colunas de partição (ex: vehicle_id)
        compare_cols: Colunas a comparar (ex: latitude, longitude)
        order_col: Coluna de ordenação
    
    Returns:
        DataFrame sem consecutivos duplicados
    
    Example:
        >>> # Remover posições consecutivas com mesma lat/lon
        >>> df_clean = remove_consecutive_duplicates(
        ...     df,
        ...     partition_cols=['vehicle_id'],
        ...     compare_cols=['latitude', 'longitude']
        ... )
    """
    logger.info("Removendo duplicatas consecutivas...")
    
    initial_count = df.count()
    
    window_spec = Window.partitionBy(partition_cols).orderBy(order_col)
    
    # Criar flag de mudança
    # Se qualquer coluna mudou, não é duplicata
    is_different = F.lit(False)
    
    for col in compare_cols:
        prev_col = f"prev_{col}"
        df = df.withColumn(prev_col, F.lag(col).over(window_spec))
        
        is_different = is_different | (F.col(col) != F.col(prev_col)) | F.col(prev_col).isNull()
        
        df = df.drop(prev_col)
    
    # Manter apenas registros diferentes
    df_filtered = df.filter(is_different)
    
    final_count = df_filtered.count()
    
    logger.info(f"Duplicatas consecutivas removidas: {initial_count - final_count}")
    
    return df_filtered


def get_duplicate_stats(df: DataFrame, key_columns: List[str]) -> DataFrame:
    """
    Retorna estatísticas sobre duplicatas.
    
    Args:
        df: DataFrame
        key_columns: Colunas que definem chave
    
    Returns:
        DataFrame com estatísticas de duplicatas
    
    Example:
        >>> stats = get_duplicate_stats(df, ['vehicle_id', 'timestamp'])
        >>> stats.show()
    """
    logger.info("Calculando estatísticas de duplicatas...")
    
    # Contar ocorrências de cada chave
    duplicate_counts = df.groupBy(key_columns) \
        .agg(F.count("*").alias("count")) \
        .filter(F.col("count") > 1) \
        .orderBy(F.col("count").desc())
    
    # Estatísticas gerais
    total_duplicates = duplicate_counts.count()
    
    if total_duplicates > 0:
        max_duplicates = duplicate_counts.agg(F.max("count")).collect()[0][0]
        avg_duplicates = duplicate_counts.agg(F.avg("count")).collect()[0][0]
        
        logger.info(f"Total de chaves duplicadas: {total_duplicates}")
        logger.info(f"Máximo de duplicatas: {max_duplicates}")
        logger.info(f"Média de duplicatas: {avg_duplicates:.2f}")
    else:
        logger.info("Nenhuma duplicata encontrada")
    
    return duplicate_counts

# =============================================================================
# END
# =============================================================================
