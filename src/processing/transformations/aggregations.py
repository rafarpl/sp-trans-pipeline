# =============================================================================
# AGGREGATIONS
# =============================================================================
# Agregações e cálculo de KPIs para camada Gold
# =============================================================================

"""
Aggregations Module

Calcula KPIs e métricas agregadas:
    - KPIs por hora: Total veículos, velocidade média, etc
    - Métricas por rota: Performance de cada rota
    - Análise de headway: Intervalos entre ônibus
    - Resumo do sistema: Visão geral do transporte
"""

import logging
from typing import Optional
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# HOURLY KPIS
# =============================================================================

def calculate_hourly_kpis(
    df: DataFrame,
    date_col: str = "date",
    hour_col: str = "hour"
) -> DataFrame:
    """
    Calcula KPIs agregados por hora e rota.
    
    Args:
        df: DataFrame Silver com posições enriquecidas
        date_col: Coluna de data
        hour_col: Coluna de hora
    
    Returns:
        DataFrame com KPIs horários
    
    Example:
        >>> kpis_df = calculate_hourly_kpis(silver_df)
    """
    logger.info("Calculando KPIs horários...")
    
    # Group by date, hour, route
    kpis = df.groupBy(date_col, hour_col, "route_code").agg(
        # Contagem de veículos
        F.countDistinct("vehicle_id").alias("total_vehicles"),
        
        # Total de registros
        F.count("*").alias("total_records"),
        
        # Velocidade
        F.avg("speed").alias("avg_speed_kmh"),
        F.max("speed").alias("max_speed_kmh"),
        F.min("speed").alias("min_speed_kmh"),
        F.stddev("speed").alias("stddev_speed_kmh"),
        
        # Movimento
        F.sum(F.when(F.col("is_moving") == True, 1).otherwise(0)).alias("moving_count"),
        
        # Acessibilidade
        F.sum(F.when(F.col("accessibility") == True, 1).otherwise(0)).alias("accessible_count"),
        
        # Qualidade de dados
        F.avg("data_quality_score").alias("avg_data_quality_score")
    )
    
    # Calcular percentuais
    kpis = kpis.withColumn(
        "pct_moving",
        (F.col("moving_count") / F.col("total_records") * 100)
    ).withColumn(
        "pct_with_accessibility",
        (F.col("accessible_count") / F.col("total_records") * 100)
    )
    
    # Adicionar timestamp de processamento
    kpis = kpis.withColumn("processing_timestamp", F.current_timestamp())
    
    logger.info(f"KPIs calculados: {kpis.count()} registros")
    
    return kpis


def calculate_route_metrics(
    df: DataFrame,
    date_col: str = "date"
) -> DataFrame:
    """
    Calcula métricas agregadas por rota (diárias).
    
    Args:
        df: DataFrame Silver
        date_col: Coluna de data
    
    Returns:
        DataFrame com métricas por rota
    """
    logger.info("Calculando métricas por rota...")
    
    metrics = df.groupBy(date_col, "route_code").agg(
        # Veículos
        F.countDistinct("vehicle_id").alias("total_vehicles"),
        F.count("*").alias("total_observations"),
        
        # Distância total aproximada
        F.sum("distance_meters").alias("distance_traveled_km"),
        
        # Velocidade
        F.avg("speed").alias("avg_speed"),
        F.max("speed").alias("max_speed"),
        
        # Horas de operação
        F.countDistinct("hour").alias("operating_hours"),
        
        # Acessibilidade
        F.avg(F.when(F.col("accessibility") == True, 100).otherwise(0)).alias("accessibility_pct"),
        
        # Qualidade
        F.avg("data_quality_score").alias("avg_data_quality")
    )
    
    # Converter distância para km
    metrics = metrics.withColumn(
        "distance_traveled_km",
        F.col("distance_traveled_km") / 1000
    )
    
    # Calcular horas de serviço por veículo
    metrics = metrics.withColumn(
        "service_hours_per_vehicle",
        F.col("operating_hours") / F.col("total_vehicles")
    )
    
    # Adicionar timestamp
    metrics = metrics.withColumn("processing_timestamp", F.current_timestamp())
    
    logger.info(f"Métricas de rota calculadas: {metrics.count()} rotas")
    
    return metrics


def calculate_headway(
    df: DataFrame,
    date_col: str = "date",
    hour_col: str = "hour"
) -> DataFrame:
    """
    Calcula headway (intervalo entre ônibus) por rota.
    
    Args:
        df: DataFrame Silver
        date_col: Coluna de data
        hour_col: Coluna de hora
    
    Returns:
        DataFrame com análise de headway
    """
    logger.info("Calculando análise de headway...")
    
    # Window para calcular diferença de tempo entre veículos
    window_spec = Window.partitionBy(date_col, hour_col, "route_code") \
                        .orderBy("timestamp")
    
    # Calcular tempo desde último veículo
    df_with_prev = df.withColumn(
        "prev_timestamp",
        F.lag("timestamp").over(window_spec)
    )
    
    # Calcular headway em minutos
    df_with_headway = df_with_prev.withColumn(
        "headway_minutes",
        F.when(
            F.col("prev_timestamp").isNotNull(),
            (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")) / 60
        )
    )
    
    # Agregar por hora e rota
    headway_stats = df_with_headway.groupBy(date_col, hour_col, "route_code").agg(
        F.count("*").alias("total_observations"),
        F.avg("headway_minutes").alias("avg_headway_minutes"),
        F.min("headway_minutes").alias("min_headway_minutes"),
        F.max("headway_minutes").alias("max_headway_minutes"),
        F.stddev("headway_minutes").alias("stddev_headway_minutes"),
        F.expr("percentile_approx(headway_minutes, 0.5)").alias("median_headway_minutes")
    )
    
    # Calcular score de regularidade (0-100)
    # Quanto menor o desvio padrão, melhor a regularidade
    headway_stats = headway_stats.withColumn(
        "headway_regularity_score",
        F.when(
            F.col("avg_headway_minutes").isNotNull(),
            100 - F.least(
                (F.col("stddev_headway_minutes") / F.col("avg_headway_minutes")) * 100,
                F.lit(100)
            )
        ).otherwise(0)
    )
    
    # Classificar qualidade do serviço
    headway_stats = headway_stats.withColumn(
        "service_quality",
        F.when(F.col("avg_headway_minutes") <= 10, "excellent")
         .when(F.col("avg_headway_minutes") <= 15, "good")
         .when(F.col("avg_headway_minutes") <= 20, "fair")
         .otherwise("poor")
    )
    
    # Adicionar timestamp
    headway_stats = headway_stats.withColumn("processing_timestamp", F.current_timestamp())
    
    logger.info(f"Análise de headway calculada: {headway_stats.count()} registros")
    
    return headway_stats


def calculate_system_summary(
    df: DataFrame,
    date_col: str = "date"
) -> DataFrame:
    """
    Calcula resumo geral do sistema por dia.
    
    Args:
        df: DataFrame Silver
        date_col: Coluna de data
    
    Returns:
        DataFrame com resumo do sistema
    """
    logger.info("Calculando resumo do sistema...")
    
    summary = df.groupBy(date_col).agg(
        # Veículos e rotas
        F.countDistinct("vehicle_id").alias("total_active_vehicles"),
        F.countDistinct("route_code").alias("total_active_routes"),
        F.count("*").alias("total_observations"),
        
        # Velocidade do sistema
        F.avg("speed").alias("system_avg_speed"),
        F.max("speed").alias("system_max_speed"),
        
        # Distância total
        F.sum("distance_meters").alias("total_distance_km"),
        
        # Acessibilidade
        F.avg(F.when(F.col("accessibility") == True, 100).otherwise(0)).alias("system_accessibility_pct"),
        
        # Movimento
        F.avg(F.when(F.col("is_moving") == True, 100).otherwise(0)).alias("system_movement_pct"),
        
        # Qualidade de dados
        F.avg("data_quality_score").alias("system_data_quality")
    )
    
def calculate_gtfs_enriched_kpis(
    df_positions: DataFrame,
    df_routes: DataFrame,
    df_trips: DataFrame,
    df_stop_times: DataFrame
) -> DataFrame:
    """
    Calcula KPIs enriquecidos com dados do GTFS (rotas, viagens, horários).
    
    Integra dados de posição em tempo quase real com dados estáticos GTFS,
    permitindo análises mais completas de pontualidade, tempo médio de viagem
    e cobertura de frota.
    """
    logger.info("Calculando KPIs enriquecidos com GTFS...")

    # Join entre posições (API) e rotas (GTFS)
    df_joined = (
        df_positions
        .join(df_routes, "route_code", "left")
        .join(df_trips, "trip_id", "left")
        .join(df_stop_times, "stop_id", "left")
    )

    # Cálculo de pontualidade (diferença entre horário real e previsto)
    df_kpis = (
        df_joined
        .withColumn(
            "delay_minutes",
            (F.unix_timestamp("timestamp") - F.unix_timestamp("arrival_time")) / 60
        )
        .groupBy("route_code")
        .agg(
            F.avg("delay_minutes").alias("avg_delay_minutes"),
            F.countDistinct("vehicle_id").alias("active_vehicles"),
            F.avg("speed").alias("avg_speed_kmh"),
            F.countDistinct("trip_id").alias("total_trips"),
            F.first("route_long_name").alias("route_name")
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )

    logger.info(f"KPIs enriquecidos com GTFS calculados: {df_kpis.count()} rotas")

    return df_kpis
    
    # Converter distância
    summary = summary.withColumn(
        "total_distance_km",
        F.col("total_distance_km") / 1000
    )
    
    # Calcular produtividade (km por veículo)
    summary = summary.withColumn(
        "avg_km_per_vehicle",
        F.col("total_distance_km") / F.col("total_active_vehicles")
    )
    
    # Adicionar timestamp
    summary = summary.withColumn("processing_timestamp", F.current_timestamp())
    
    logger.info(f"Resumo do sistema calculado: {summary.count()} dias")
    
    return summary


def aggregate_by_route(
    df: DataFrame,
    agg_columns: list,
    group_by: list = None
) -> DataFrame:
    """
    Agrega dados por rota com colunas customizadas.
    
    Args:
        df: DataFrame
        agg_columns: Lista de agregações (ex: [F.avg("speed")])
        group_by: Colunas para group by (default: route_code)
    
    Returns:
        DataFrame agregado
    """
    if group_by is None:
        group_by = ["route_code"]
    
    return df.groupBy(group_by).agg(*agg_columns)


def aggregate_by_hour(
    df: DataFrame,
    agg_columns: list,
    include_date: bool = True
) -> DataFrame:
    """
    Agrega dados por hora.
    
    Args:
        df: DataFrame
        agg_columns: Lista de agregações
        include_date: Se True, agrupa por date + hour
    
    Returns:
        DataFrame agregado
    """
    group_cols = ["date", "hour"] if include_date else ["hour"]
    
    return df.groupBy(group_cols).agg(*agg_columns)


def calculate_time_series_metrics(
    df: DataFrame,
    metric_col: str,
    time_col: str = "timestamp",
    window_minutes: int = 30
) -> DataFrame:
    """
    Calcula métricas de série temporal (média móvel, etc).
    
    Args:
        df: DataFrame
        metric_col: Coluna da métrica
        time_col: Coluna de tempo
        window_minutes: Janela em minutos
    
    Returns:
        DataFrame com métricas de série temporal
    """
    logger.info(f"Calculando série temporal para {metric_col}...")
    
    # Window de tempo
    window_seconds = window_minutes * 60
    window_spec = Window.partitionBy("route_code") \
                        .orderBy(F.col(time_col).cast("long")) \
                        .rangeBetween(-window_seconds, 0)
    
    # Média móvel
    df = df.withColumn(
        f"{metric_col}_moving_avg",
        F.avg(metric_col).over(window_spec)
    )
    
    # Desvio padrão móvel
    df = df.withColumn(
        f"{metric_col}_moving_std",
        F.stddev(metric_col).over(window_spec)
    )
    
    return df


def calculate_percentiles(
    df: DataFrame,
    metric_col: str,
    percentiles: list = [0.25, 0.5, 0.75, 0.95]
) -> DataFrame:
    """
    Calcula percentis de uma métrica.
    
    Args:
        df: DataFrame
        metric_col: Coluna da métrica
        percentiles: Lista de percentis (0-1)
    
    Returns:
        DataFrame com percentis
    """
    logger.info(f"Calculando percentis de {metric_col}...")
    
    agg_exprs = [
        F.expr(f"percentile_approx({metric_col}, {p})").alias(f"p{int(p*100)}")
        for p in percentiles
    ]
    
    result = df.agg(*agg_exprs)
    
    return result

# =============================================================================
# END
# =============================================================================
