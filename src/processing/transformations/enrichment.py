# =============================================================================
# ENRICHMENT
# =============================================================================
# Enriquecimento de dados com informações GTFS
# =============================================================================

"""
Enrichment Module

Enriquece dados de posições com informações estáticas do GTFS:
    - Informações de rotas (nome, tipo, operadora)
    - Informações de paradas (nome, localização)
    - Informações de viagens (horários, direção)
    - Metadados adicionais
"""

import logging
from typing import Optional, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# MAIN ENRICHMENT FUNCTION
# =============================================================================

def enrich_with_gtfs(
    positions_df: DataFrame,
    gtfs_routes_df: DataFrame,
    gtfs_stops_df: Optional[DataFrame] = None,
    gtfs_trips_df: Optional[DataFrame] = None,
    join_key: str = "route_code"
) -> DataFrame:
    """
    Enriquece DataFrame de posições com dados GTFS.
    
    Args:
        positions_df: DataFrame com posições dos veículos
        gtfs_routes_df: DataFrame com rotas GTFS
        gtfs_stops_df: DataFrame com paradas GTFS (opcional)
        gtfs_trips_df: DataFrame com viagens GTFS (opcional)
        join_key: Coluna para join (geralmente route_code ou route_id)
    
    Returns:
        DataFrame enriquecido
    
    Example:
        >>> enriched_df = enrich_with_gtfs(positions_df, routes_df, stops_df)
    """
    logger.info("Iniciando enriquecimento com dados GTFS...")
    
    initial_count = positions_df.count()
    
    # Join com rotas
    df_enriched = join_with_routes(positions_df, gtfs_routes_df, join_key)
    
    # Join com paradas (se fornecido)
    if gtfs_stops_df is not None:
        df_enriched = join_with_stops(df_enriched, gtfs_stops_df)
    
    # Join com viagens (se fornecido)
    if gtfs_trips_df is not None:
        df_enriched = join_with_trips(df_enriched, gtfs_trips_df)
    
    final_count = df_enriched.count()
    
    logger.info(f"Enriquecimento concluído: {initial_count} → {final_count} registros")
    
    return df_enriched


def join_with_routes(
    df: DataFrame,
    routes_df: DataFrame,
    join_key: str = "route_code"
) -> DataFrame:
    """
    Join com informações de rotas.
    
    Args:
        df: DataFrame original
        routes_df: DataFrame com rotas
        join_key: Coluna para join
    
    Returns:
        DataFrame com informações de rotas
    """
    logger.info("Juntando com dados de rotas...")
    
    # Verificar se join_key existe
    if join_key not in df.columns:
        logger.warning(f"Coluna {join_key} não encontrada em positions_df")
        return df
    
    if join_key not in routes_df.columns:
        logger.warning(f"Coluna {join_key} não encontrada em routes_df")
        return df
    
    # Selecionar colunas relevantes de routes
    route_columns = [
        join_key,
        "route_short_name",
        "route_long_name",
        "route_type",
        "route_color",
        "agency_id"
    ]
    
    # Filtrar apenas colunas que existem
    available_columns = [col for col in route_columns if col in routes_df.columns]
    routes_subset = routes_df.select(available_columns).distinct()
    
    # Renomear colunas para evitar conflito
    for col in available_columns:
        if col != join_key and col in df.columns:
            routes_subset = routes_subset.withColumnRenamed(col, f"gtfs_{col}")
    
    # Left join
    df_joined = df.join(routes_subset, on=join_key, how="left")
    
    # Log resultados
    matched = df_joined.filter(F.col("route_short_name").isNotNull()).count()
    logger.info(f"Rotas matched: {matched}/{df.count()} ({matched/df.count()*100:.1f}%)")
    
    return df_joined


def join_with_stops(
    df: DataFrame,
    stops_df: DataFrame,
    distance_threshold_meters: float = 100.0
) -> DataFrame:
    """
    Join com paradas próximas (spatial join).
    
    Args:
        df: DataFrame com posições
        stops_df: DataFrame com paradas
        distance_threshold_meters: Distância máxima para considerar "próximo"
    
    Returns:
        DataFrame com informação da parada mais próxima
    """
    logger.info("Juntando com dados de paradas...")
    
    # Verificar colunas necessárias
    if "latitude" not in df.columns or "longitude" not in df.columns:
        logger.warning("Coordenadas não encontradas em positions_df")
        return df
    
    if "stop_lat" not in stops_df.columns or "stop_lon" not in stops_df.columns:
        logger.warning("Coordenadas não encontradas em stops_df")
        return df
    
    # Fazer cross join (cuidado com performance!)
    # Em produção, considere usar spatial index ou grid-based approach
    
    # Adicionar prefix para evitar conflitos
    stops_df = stops_df.select(
        F.col("stop_id").alias("nearest_stop_id"),
        F.col("stop_name").alias("nearest_stop_name"),
        F.col("stop_lat").alias("stop_lat"),
        F.col("stop_lon").alias("stop_lon")
    )
    
    # Cross join (limitado por route para reduzir tamanho)
    if "route_code" in df.columns and "route_code" in stops_df.columns:
        df_cross = df.join(stops_df, on="route_code", how="left")
    else:
        # Cross join completo (CUIDADO: pode ser muito grande!)
        logger.warning("Cross join sem filtro - pode ser lento")
        df_cross = df.crossJoin(stops_df)
    
    # Calcular distância (fórmula simplificada)
    df_with_distance = df_cross.withColumn(
        "distance_to_stop",
        F.sqrt(
            F.pow((F.col("latitude") - F.col("stop_lat")) * 111000, 2) +
            F.pow((F.col("longitude") - F.col("stop_lon")) * 111000 * 
                  F.cos(F.radians(F.col("latitude"))), 2)
        )
    )
    
    # Filtrar apenas paradas dentro do threshold
    df_filtered = df_with_distance.filter(
        F.col("distance_to_stop") <= distance_threshold_meters
    )
    
    # Manter apenas a parada mais próxima
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("vehicle_id", "timestamp") \
                        .orderBy("distance_to_stop")
    
    df_with_rank = df_filtered.withColumn("rank", F.row_number().over(window_spec))
    df_nearest = df_with_rank.filter(F.col("rank") == 1).drop("rank")
    
    # Remover colunas intermediárias
    df_result = df_nearest.drop("stop_lat", "stop_lon")
    
    logger.info("Join com paradas concluído")
    
    return df_result


def join_with_trips(
    df: DataFrame,
    trips_df: DataFrame
) -> DataFrame:
    """
    Join com informações de viagens.
    
    Args:
        df: DataFrame original
        trips_df: DataFrame com viagens
    
    Returns:
        DataFrame com informações de viagens
    """
    logger.info("Juntando com dados de viagens...")
    
    # Join por route_id
    join_key = "route_code" if "route_code" in df.columns else "route_id"
    
    if join_key not in trips_df.columns:
        logger.warning(f"Coluna {join_key} não encontrada em trips_df")
        return df
    
    # Selecionar colunas relevantes
    trip_columns = [
        join_key,
        "trip_id",
        "trip_headsign",
        "direction_id",
        "shape_id"
    ]
    
    available_columns = [col for col in trip_columns if col in trips_df.columns]
    trips_subset = trips_df.select(available_columns).distinct()
    
    # Join
    df_joined = df.join(trips_subset, on=join_key, how="left")
    
    logger.info("Join com viagens concluído")
    
    return df_joined


def add_route_info(
    df: DataFrame,
    route_short_name: Optional[str] = None,
    route_long_name: Optional[str] = None,
    route_type: Optional[int] = None
) -> DataFrame:
    """
    Adiciona informações de rota manualmente.
    
    Args:
        df: DataFrame
        route_short_name: Nome curto da rota
        route_long_name: Nome longo da rota
        route_type: Tipo de rota (GTFS route_type)
    
    Returns:
        DataFrame com informações adicionadas
    """
    if route_short_name:
        df = df.withColumn("route_short_name", F.lit(route_short_name))
    
    if route_long_name:
        df = df.withColumn("route_long_name", F.lit(route_long_name))
    
    if route_type:
        df = df.withColumn("route_type", F.lit(route_type))
    
    return df


def add_time_features(df: DataFrame, timestamp_col: str = "timestamp") -> DataFrame:
    """
    Adiciona features temporais derivadas.
    
    Args:
        df: DataFrame
        timestamp_col: Coluna de timestamp
    
    Returns:
        DataFrame com features temporais
    """
    logger.info("Adicionando features temporais...")
    
    df = df.withColumn("hour", F.hour(timestamp_col)) \
           .withColumn("day_of_week", F.dayofweek(timestamp_col)) \
           .withColumn("day_of_month", F.dayofmonth(timestamp_col)) \
           .withColumn("month", F.month(timestamp_col)) \
           .withColumn("year", F.year(timestamp_col)) \
           .withColumn("is_weekend", 
                      F.when(F.dayofweek(timestamp_col).isin([1, 7]), True).otherwise(False)) \
           .withColumn("is_peak_hour",
                      F.when(F.hour(timestamp_col).between(7, 9) | 
                            F.hour(timestamp_col).between(17, 19), True).otherwise(False))
    
    return df


def add_movement_features(df: DataFrame) -> DataFrame:
    """
    Adiciona features de movimento.
    
    Args:
        df: DataFrame com coordenadas e timestamps
    
    Returns:
        DataFrame com features de movimento
    """
    logger.info("Adicionando features de movimento...")
    
    from pyspark.sql.window import Window
    
    # Window por veículo ordenado por tempo
    window_spec = Window.partitionBy("vehicle_id").orderBy("timestamp")
    
    # Coordenadas anteriores
    df = df.withColumn("prev_latitude", F.lag("latitude").over(window_spec)) \
           .withColumn("prev_longitude", F.lag("longitude").over(window_spec)) \
           .withColumn("prev_timestamp", F.lag("timestamp").over(window_spec))
    
    # Calcular distância percorrida
    df = df.withColumn(
        "distance_meters",
        F.when(
            F.col("prev_latitude").isNotNull(),
            F.sqrt(
                F.pow((F.col("latitude") - F.col("prev_latitude")) * 111000, 2) +
                F.pow((F.col("longitude") - F.col("prev_longitude")) * 111000 * 
                      F.cos(F.radians(F.col("latitude"))), 2)
            )
        )
    )
    
    # Calcular tempo decorrido
    df = df.withColumn(
        "time_diff_seconds",
        F.when(
            F.col("prev_timestamp").isNotNull(),
            F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")
        )
    )
    
    # Calcular velocidade
    df = df.withColumn(
        "calculated_speed_kmh",
        F.when(
            (F.col("distance_meters").isNotNull()) & 
            (F.col("time_diff_seconds") > 0),
            (F.col("distance_meters") / F.col("time_diff_seconds")) * 3.6
        )
    )
    
    # Flag de movimento
    df = df.withColumn(
        "is_moving",
        F.when(F.col("calculated_speed_kmh") > 5, True).otherwise(False)
    )
    
    # Limpar colunas auxiliares
    df = df.drop("prev_latitude", "prev_longitude", "prev_timestamp")
    
    return df

# =============================================================================
# END
# =============================================================================
